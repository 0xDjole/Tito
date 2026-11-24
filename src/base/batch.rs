use super::*;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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
    indexes: Vec<(String, KeySpace)>,
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
        let mut sources = BTreeSet::new();
        for (expected, _) in &replacements {
            let primary = format!("{}:{}", self.get_table(), expected.id());
            sources.insert(format!("reverse-index:{primary}"));
            sources.insert(primary);
        }
        let mut found: HashMap<_, _> = tx
            .batch_get(sources.iter().collect::<Vec<_>>())
            .await?
            .into_iter()
            .collect();
        for (expected, next) in replacements {
            let id = expected.id();
            let primary = format!("{}:{id}", self.get_table());
            let reverse = format!("reverse-index:{primary}");
            let primary_value = found.remove(primary.as_bytes());
            let reverse_value = found.remove(reverse.as_bytes());
            let old = self
                .index_state(
                    &primary,
                    &reverse,
                    primary_value.is_some(),
                    reverse_value.as_deref(),
                )?
                .ok_or_else(|| {
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
            let current = Self::stored_json(&primary, primary_value.as_deref())?;
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
            let indexes = self.index_keys(&primary, &next, &value)?;
            let keys = indexes
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            if keys.iter().collect::<HashSet<_>>().len() != keys.len() {
                return Err(TitoError::InvalidInput(
                    "Batch replacement contains duplicate final manifest keys".into(),
                ));
            }
            let manifest = ReverseIndex {
                value: keys,
                version,
            };
            let manifest_bytes = self.manifest_bytes(&primary, &manifest)?;
            for (key, _) in &indexes {
                if let Some(index) = Self::unique_index_name(key) {
                    if unique_owners.insert(key.clone(), id.clone()).is_some() {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.to_owned(),
                        });
                    }
                }
            }
            prepared_keys.extend(indexes.iter().map(|(key, _)| key.clone()));
            prepared_keys.extend([primary.clone(), reverse.clone()]);
            if prepared_keys.len() > MAX_REPLACE_MANY_KEYS {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement needs more than {MAX_REPLACE_MANY_KEYS} storage keys"
                )));
            }
            prepared_bytes = prepared_bytes
                .checked_add(Self::written_bytes(
                    &primary,
                    &primary_bytes,
                    &indexes,
                    &reverse,
                    &manifest_bytes,
                ))
                .ok_or_else(|| {
                    TitoError::InvalidInput("Batch replacement byte size overflowed".into())
                })?;
            if prepared_bytes > MAX_REPLACE_MANY_BYTES {
                return Err(TitoError::InvalidInput(format!(
                    "Batch replacement exceeds {MAX_REPLACE_MANY_BYTES} prepared bytes"
                )));
            }
            let mut writes = BTreeMap::new();
            for (key, space) in &indexes {
                writes.insert(key.clone(), Self::entry_bytes(*space, &primary_bytes));
            }
            writes.insert(primary.clone(), primary_bytes);
            writes.insert(reverse, manifest_bytes);
            prepared.push(PreparedReplacement {
                id,
                old,
                indexes,
                stored,
                writes,
            });
        }

        let mut unique = BTreeSet::new();
        for replacement in &prepared {
            unique.extend(Self::unique_keys(&replacement.old.value));
            for (key, _) in &replacement.indexes {
                if Self::unique_index_name(key).is_some() {
                    unique.insert(key.clone());
                }
            }
        }
        if !unique.is_empty() {
            found.extend(tx.batch_get(unique.iter().collect::<Vec<_>>()).await?);
        }
        let mut deletes = BTreeSet::new();
        for replacement in &prepared {
            for key in &replacement.old.value {
                if Self::unique_index_name(key).is_some()
                    && !Self::is_unique_owner(key, found.get(key.as_bytes()), &replacement.id)?
                {
                    return Err(TitoError::IndexError(format!(
                        "Unique index on model '{}' is missing or owned by another record",
                        T::table()
                    )));
                }
                deletes.insert(key.clone());
            }
            for (key, _) in &replacement.indexes {
                let Some(index) = Self::unique_index_name(key) else {
                    continue;
                };
                if let Some(bytes) = found.get(key.as_bytes()) {
                    let owner_id = Self::unique_owner_id(key, bytes)?;
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
            deletes.extend(record_fence_keys(&T::table(), &replacement.id));
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
