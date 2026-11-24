use super::*;
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct CreateManyBuilder<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> {
    model: &'a TitoModel<E, T>,
    records: Vec<T>,
    timestamps: bool,
}

#[derive(Default)]
struct CreationWrites {
    entries: BTreeMap<String, Option<Vec<u8>>>,
    bytes: usize,
}

impl CreationWrites {
    fn add(&mut self, key: String, value: Option<Vec<u8>>) -> Result<(), TitoError> {
        let size = key.len().checked_add(value.as_ref().map_or(0, Vec::len));
        self.bytes = size
            .and_then(|size| self.bytes.checked_add(size))
            .filter(|total| *total <= MAX_BATCH_BYTES)
            .ok_or_else(|| {
                TitoError::InvalidInput(format!("Batch creation exceeds {MAX_BATCH_BYTES} bytes"))
            })?;
        if self.entries.len() >= MAX_BATCH_KEYS {
            return Err(TitoError::InvalidInput(format!(
                "Batch creation exceeds {MAX_BATCH_KEYS} storage keys"
            )));
        }
        if self.entries.insert(key, value).is_some() {
            return Err(TitoError::InvalidInput(
                "Batch creation contains duplicate storage keys".into(),
            ));
        }
        Ok(())
    }
}

impl<E: TitoEngine, T: crate::types::TitoModelConstraints> TitoModel<E, T> {
    pub fn create_many(&self, records: Vec<T>) -> CreateManyBuilder<'_, E, T> {
        CreateManyBuilder {
            model: self,
            records,
            timestamps: true,
        }
    }
}

impl<E: TitoEngine, T: crate::types::TitoModelConstraints> CreateManyBuilder<'_, E, T> {
    pub fn timestamps(mut self, timestamps: bool) -> Self {
        self.timestamps = timestamps;
        self
    }

    pub async fn execute(self, tx: &E::Transaction) -> Result<Vec<T>, TitoError> {
        if self.records.is_empty() || self.records.len() > MAX_BATCH_RECORDS {
            return Err(TitoError::InvalidInput(format!(
                "Batch creation requires 1..={MAX_BATCH_RECORDS} records"
            )));
        }
        let version = TitoRecordVersion::from_transaction(tx.start_version())?;
        let mut writes = CreationWrites::default();
        let mut primary_keys = Vec::with_capacity(self.records.len());
        let mut ids = HashSet::with_capacity(self.records.len());
        let mut unique_owners = BTreeMap::new();
        let mut read_keys = Vec::new();
        let mut stored = Vec::with_capacity(self.records.len());

        for record in self.records {
            let id = record.id();
            if !ids.insert(id.clone()) {
                return Err(TitoError::InvalidInput(
                    "Batch creation requires distinct record identities".into(),
                ));
            }
            let primary = format!("{}:{id}", self.model.get_table());
            let reverse = format!("reverse-index:{primary}");
            let value = self
                .model
                .value_with_options(&record, self.timestamps, true)?;
            let bytes = serde_json::to_vec(&value)
                .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
            writes.add(primary.clone(), Some(bytes.clone()))?;
            let indexes = self.model.index_keys(&primary, &record, &value)?;
            let manifest = ReverseIndex {
                value: indexes.iter().map(|(key, _)| key.clone()).collect(),
                version,
            };
            writes.add(
                reverse.clone(),
                Some(self.model.manifest_bytes(&primary, &manifest)?),
            )?;
            read_keys.extend([primary.clone(), reverse.clone()]);
            primary_keys.push((primary, reverse));
            for (key, space) in indexes {
                if let Some(index) = TitoModel::<E, T>::unique_index_name(&key) {
                    if unique_owners
                        .insert(key.clone(), (id.clone(), index.to_owned()))
                        .is_some()
                    {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.into(),
                        });
                    }
                    read_keys.push(key.clone());
                }
                writes.add(key, Some(TitoModel::<E, T>::entry_bytes(space, &bytes)))?;
            }
            for key in record_fence_keys(&T::table(), &id) {
                writes.add(key, None)?;
            }
            stored.push(
                serde_json::from_value(value)
                    .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?,
            );
        }

        let mut found: HashMap<_, _> = tx.batch_get(read_keys).await?.into_iter().collect();
        for (primary, reverse) in primary_keys {
            let has_primary = found.remove(primary.as_bytes()).is_some();
            let metadata = found.remove(reverse.as_bytes());
            if self
                .model
                .index_state(&primary, &reverse, has_primary, metadata.as_deref())?
                .is_some()
            {
                return Err(TitoError::CreateFailed(format!(
                    "Record '{primary}' already exists"
                )));
            }
        }
        for (key, (id, index)) in unique_owners {
            if let Some(bytes) = found.remove(key.as_bytes()) {
                if TitoModel::<E, T>::unique_owner_id(&key, &bytes)? == id {
                    return Err(TitoError::IndexError(format!(
                        "Unique index '{index}' has no primary record for its owner"
                    )));
                }
                return Err(TitoError::UniqueViolation {
                    model: T::table(),
                    index,
                });
            }
        }
        for (key, value) in writes.entries {
            match value {
                Some(bytes) => tx.put(key, bytes).await?,
                None => tx.delete(key).await?,
            }
        }
        Ok(stored)
    }
}
