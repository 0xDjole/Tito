use super::*;
use std::collections::BTreeSet;

pub const MAX_REPLACE_MANY_RECORDS: usize = 100;
pub const MAX_REPLACE_MANY_KEYS: usize = 10_000;
pub const MAX_REPLACE_MANY_BYTES: usize = 8_388_608;

pub struct ReplaceManyBuilder<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> {
    model: &'a TitoModel<E, T>,
    replacements: Vec<(T, T)>,
    timestamps: bool,
}

impl<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> ReplaceManyBuilder<'a, E, T> {
    pub fn timestamps(mut self, timestamps: bool) -> Self {
        self.timestamps = timestamps;
        self
    }

    pub async fn execute(self, tx: &E::Transaction) -> Result<Vec<T>, TitoError> {
        self.model
            .replace_many_internal(self.replacements, self.timestamps, tx)
            .await
    }
}

struct PreparedReplacement<T> {
    id: String,
    old: ReverseIndex,
    reference_keys: HashSet<String>,
    references: BTreeMap<String, TitoReference>,
    marker: Option<String>,
    indexes: Vec<(String, Value)>,
    stored: T,
    writes: BTreeMap<String, Vec<u8>>,
}

impl<E: TitoEngine, T: crate::types::TitoModelConstraints> TitoModel<E, T> {
    pub fn replace_many(&self, replacements: Vec<(T, T)>) -> ReplaceManyBuilder<'_, E, T> {
        ReplaceManyBuilder {
            model: self,
            replacements,
            timestamps: true,
        }
    }

    async fn replace_many_internal(
        &self,
        replacements: Vec<(T, T)>,
        timestamps: bool,
        tx: &E::Transaction,
    ) -> Result<Vec<T>, TitoError> {
        if replacements.is_empty() || replacements.len() > MAX_REPLACE_MANY_RECORDS {
            return Err(TitoError::InvalidInput(format!(
                "Batch replacement requires 1..={} records",
                MAX_REPLACE_MANY_RECORDS
            )));
        }
        let mut ids = BTreeMap::new();
        for (position, (expected, next)) in replacements.iter().enumerate() {
            let id = expected.id();
            if next.id() != id || ids.insert(id, position).is_some() {
                return Err(TitoError::InvalidInput(
                    "Batch replacement requires unchanged, distinct record identities".into(),
                ));
            }
        }

        let version = TitoRecordVersion::from_transaction(tx.start_version())?;
        let mut prepared = Vec::with_capacity(replacements.len());
        let mut unique_owners = BTreeMap::new();
        let mut prepared_keys = BTreeSet::new();
        let mut prepared_bytes = 0usize;
        for (expected, next) in replacements {
            let id = expected.id();
            let primary = format!("{}:{id}", self.get_table());
            let reverse = format!("reverse-index:{primary}");
            let old = self.load_index_state(&primary, tx).await?.ok_or_else(|| {
                TitoError::NotFound(format!("Record '{primary}' not found in database"))
            })?;
            self.manifest_bytes(&primary, &old)?;
            prepared_keys.extend(old.value.iter().cloned());
            if prepared_keys.len() > MAX_REPLACE_MANY_KEYS {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement needs more than {MAX_REPLACE_MANY_KEYS} storage keys"
                )));
            }
            if old.value.iter().collect::<HashSet<_>>().len() != old.value.len() {
                return Err(TitoError::IndexError(
                    "Batch replacement found duplicate source manifest keys".into(),
                ));
            }
            let (_, current) = self.get_raw(&primary, tx).await?;
            let expected = serde_json::to_value(expected)
                .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
            if current != expected {
                return Err(TitoError::UpdateFailed(format!(
                    "Record '{primary}' changed before batch replacement"
                )));
            }
            let value = self.value_with_options(&next, timestamps, false)?;
            let primary_bytes = serde_json::to_vec(&value)
                .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
            if primary_bytes.len() > MAX_REPLACE_MANY_BYTES {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement exceeds {MAX_REPLACE_MANY_BYTES} prepared bytes"
                )));
            }
            let stored = serde_json::from_value(value.clone()).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize replacement for '{primary}': {error}"
                ))
            })?;
            let indexes = self.get_index_keys(primary.clone(), &next, &value)?;
            let references = self.declared_references(&id, &next)?;
            let marker = self.declared_marker(&id, &next);
            let reference_keys = old
                .value
                .iter()
                .filter(|key| Self::is_reference_state_key(key))
                .cloned()
                .collect::<HashSet<_>>();
            let mut keys = indexes
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            keys.extend(references.keys().cloned());
            keys.extend(marker.iter().cloned());
            if keys.iter().collect::<HashSet<_>>().len() != keys.len() {
                return Err(TitoError::InvalidInput(
                    "Batch replacement contains duplicate final manifest keys".into(),
                ));
            }
            let manifest = ReverseIndex {
                value: keys,
                version,
            };
            let mut writes = BTreeMap::new();
            writes.insert(primary.clone(), primary_bytes);
            writes.insert(reverse, self.manifest_bytes(&primary, &manifest)?);
            for (key, value) in &indexes {
                if let Some(index) = Self::unique_index_name(key) {
                    if unique_owners.insert(key.clone(), id.clone()).is_some() {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.to_owned(),
                        });
                    }
                }
                writes.insert(
                    key.clone(),
                    serde_json::to_vec(value)
                        .map_err(|error| TitoError::SerializationFailed(error.to_string()))?,
                );
            }
            for (key, reference) in &references {
                if !reference_keys.contains(key) {
                    writes.insert(
                        key.clone(),
                        serde_json::to_vec(&self.incoming_value(&id, reference)?)
                            .map_err(|error| TitoError::SerializationFailed(error.to_string()))?,
                    );
                }
            }
            if let Some(marker) = &marker {
                if !reference_keys.contains(marker) {
                    writes.insert(marker.clone(), b"true".to_vec());
                }
            }
            prepared_keys.extend(writes.keys().cloned());
            if prepared_keys.len() > MAX_REPLACE_MANY_KEYS {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement needs more than {MAX_REPLACE_MANY_KEYS} storage keys"
                )));
            }
            prepared_bytes = writes
                .iter()
                .try_fold(prepared_bytes, |total, (key, bytes)| {
                    total.checked_add(key.len())?.checked_add(bytes.len())
                })
                .ok_or_else(|| {
                    TitoError::InvalidInput("Batch replacement byte size overflowed".into())
                })?;
            if prepared_bytes > MAX_REPLACE_MANY_BYTES {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement exceeds {MAX_REPLACE_MANY_BYTES} prepared bytes"
                )));
            }
            prepared.push(PreparedReplacement {
                id,
                old,
                reference_keys,
                references,
                marker,
                indexes,
                stored,
                writes,
            });
        }

        let mut deletes = BTreeSet::new();
        for replacement in &prepared {
            for key in &replacement.old.value {
                if Self::unique_index_name(key).is_some()
                    && !self.validate_unique_owner(key, &replacement.id, tx).await?
                {
                    return Err(TitoError::IndexError(format!(
                        "Unique index on model '{}' is missing or owned by another record",
                        T::table()
                    )));
                }
                if !Self::is_reference_state_key(key)
                    || (!replacement.references.contains_key(key)
                        && replacement.marker.as_ref() != Some(key))
                {
                    deletes.insert(key.clone());
                }
            }
            for (key, _) in &replacement.indexes {
                let Some(index) = Self::unique_index_name(key) else {
                    continue;
                };
                if let Some(bytes) = tx.get(key).await? {
                    let owner: T = serde_json::from_slice(&bytes)
                        .map_err(|error| TitoError::DeserializationFailed(error.to_string()))?;
                    let owner_id = owner.id();
                    let Some(position) = ids.get(&owner_id) else {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.to_owned(),
                        });
                    };
                    if !prepared[*position].old.value.contains(key) {
                        return Err(TitoError::IndexError(format!(
                            "Unique index '{index}' on model '{}' is not declared by its owner",
                            T::table()
                        )));
                    }
                }
            }
            let kept = replacement
                .reference_keys
                .iter()
                .filter_map(|key| parse_reference_key(key))
                .map(|key| (key.target_table, key.target_id))
                .collect::<HashSet<_>>();
            let mut external = BTreeMap::new();
            for (key, reference) in &replacement.references {
                let target = (reference.table == T::table())
                    .then(|| ids.get(&reference.id))
                    .flatten();
                match target {
                    Some(position) => {
                        if prepared[*position].marker.is_some()
                            && !kept.contains(&(reference.table.clone(), reference.id.clone()))
                        {
                            return Err(TitoError::ReferenceDeleting {
                                table: reference.table.clone(),
                                id: reference.id.clone(),
                                path: reference.path.clone(),
                            });
                        }
                    }
                    None => {
                        external.insert(key.clone(), reference.clone());
                    }
                }
            }
            deletes.extend(
                self.check_reference_targets(&external, &replacement.reference_keys, tx)
                    .await?,
            );
        }

        let mut writes = BTreeMap::new();
        for replacement in &mut prepared {
            for (key, value) in std::mem::take(&mut replacement.writes) {
                if writes.insert(key, value).is_some() {
                    return Err(TitoError::InvalidInput(
                        "Batch replacement contains duplicate final storage keys".into(),
                    ));
                }
            }
        }
        let keys = deletes.iter().chain(writes.keys()).collect::<HashSet<_>>();
        if keys.len() > MAX_REPLACE_MANY_KEYS {
            return Err(TitoError::InvalidInput(format!(
                "Batch replacement needs more than {MAX_REPLACE_MANY_KEYS} storage keys"
            )));
        }
        let bytes = writes
            .iter()
            .try_fold(0usize, |total, (key, value)| {
                total.checked_add(key.len())?.checked_add(value.len())
            })
            .and_then(|total| {
                deletes
                    .iter()
                    .try_fold(total, |total, key| total.checked_add(key.len()))
            });
        if bytes.is_none_or(|bytes| bytes > MAX_REPLACE_MANY_BYTES) {
            return Err(TitoError::InvalidInput(format!(
                "Batch replacement exceeds {MAX_REPLACE_MANY_BYTES} prepared bytes"
            )));
        }
        for key in deletes {
            tx.delete(key).await?;
        }
        for (key, value) in writes {
            tx.put(key, value).await?;
        }
        Ok(prepared
            .into_iter()
            .map(|replacement| replacement.stored)
            .collect())
    }
}
