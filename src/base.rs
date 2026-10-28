use std::collections::HashSet;
use std::future::Future;

use std::marker::PhantomData;

use crate::{
    error::TitoError,
    fence::record_fence_keys,
    query::IndexQueryBuilder,
    types::{
        FieldValue, ReverseIndex, TitoCursor, TitoEngine, TitoFindPayload, TitoGuard, TitoKvPair,
        TitoModelOptions, TitoPaginated, TitoRecordVersion, TitoScanPayload, TitoTransaction,
        TitoVersioned,
    },
    utils::{key_after_bytes, prefix_end_bytes},
};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

const MAX_MANIFEST_KEYS: usize = 10_000;
const MAX_MANIFEST_BYTES: usize = 1_048_576;

mod batch;
pub use batch::{
    ReplaceManyBuilder, MAX_REPLACE_MANY_BYTES, MAX_REPLACE_MANY_KEYS, MAX_REPLACE_MANY_RECORDS,
};

#[derive(Clone)]
pub struct TitoModel<E: TitoEngine, T> {
    pub engine: E,
    pub partition_count: u32,
    _phantom: PhantomData<T>,
}

pub struct SetBuilder<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> {
    model: &'a TitoModel<E, T>,
    payload: T,
    timestamps: bool,
}

impl<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> SetBuilder<'a, E, T> {
    pub fn timestamps(mut self, timestamps: bool) -> Self {
        self.timestamps = timestamps;
        self
    }

    pub async fn execute(self, tx: &E::Transaction) -> Result<T, TitoError> {
        self.model
            .set_internal(self.payload, self.timestamps, tx)
            .await
    }
}

pub struct GetBuilder<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> {
    model: &'a TitoModel<E, T>,
    id: String,
}

impl<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> GetBuilder<'a, E, T> {
    pub async fn execute(self, tx: Option<&E::Transaction>) -> Result<T, TitoError> {
        self.model.get_internal(&self.id, tx).await
    }
}

pub struct GetManyBuilder<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> {
    model: &'a TitoModel<E, T>,
    ids: Vec<String>,
}

impl<'a, E: TitoEngine, T: crate::types::TitoModelConstraints> GetManyBuilder<'a, E, T> {
    pub async fn execute(self, tx: Option<&E::Transaction>) -> Result<Vec<T>, TitoError> {
        self.model.get_many_internal(self.ids, tx).await
    }
}

impl<E: TitoEngine, T: crate::types::TitoModelConstraints> TitoModel<E, T> {
    pub fn new(engine: E, options: TitoModelOptions) -> Self {
        Self {
            engine,
            partition_count: options.partition_count,
            _phantom: PhantomData,
        }
    }

    pub fn get_table(&self) -> String {
        T::key_prefix()
    }

    pub fn get_id_from_table(&self, key: String) -> String {
        let parts: Vec<&str> = key.split(':').collect();
        parts
            .last()
            .map(|last| last.to_string())
            .unwrap_or_else(|| key)
    }

    pub fn query_by_index(&self, index: impl Into<String>) -> IndexQueryBuilder<E, T> {
        IndexQueryBuilder::new(self.clone(), index.into())
    }

    fn decode_cursor(&self, cursor: String) -> Result<TitoCursor, TitoError> {
        let cursor = general_purpose::STANDARD.decode(cursor).map_err(|_err| {
            TitoError::DeserializationFailed("Failed to decode cursor".to_string())
        })?;
        if let Ok(value) = serde_json::from_slice::<TitoCursor>(&cursor) {
            return Ok(value);
        }
        Err(TitoError::DeserializationFailed(
            "Failed to deserialize cursor".to_string(),
        ))
    }

    fn encode_cursors(&self, ids: Vec<Option<String>>) -> Result<String, TitoError> {
        let tikv_cursor = TitoCursor { ids };
        let json_bytes = serde_json::to_vec(&tikv_cursor).map_err(|_| {
            TitoError::SerializationFailed("Failed to serialize cursor".to_string())
        })?;
        Ok(general_purpose::STANDARD.encode(&json_bytes))
    }

    fn validate_scan_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
        if start.is_empty() || end.is_empty() {
            return Err(TitoError::InvalidInput(
                "Scan range bounds must not be empty".to_string(),
            ));
        }
        if start >= end {
            return Err(TitoError::InvalidInput(
                "Scan range start must be less than end".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_cursor_in_range(
        &self,
        cursor: &[u8],
        start: &[u8],
        end: &[u8],
    ) -> Result<(), TitoError> {
        if cursor < start || cursor >= end {
            return Err(TitoError::InvalidInput(
                "Cursor is outside the requested scan range".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_scan_limit(&self, limit: u32) -> Result<(), TitoError> {
        if limit == 0 {
            return Err(TitoError::InvalidInput(
                "Scan limit must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    pub async fn tx<F, Fut, R, Err>(&self, f: F) -> Result<R, Err>
    where
        F: FnOnce(E::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<R, Err>> + Send,
        Err: From<TitoError> + Send + Sync + std::fmt::Debug,
        R: Send,
    {
        self.engine.transaction(f).await
    }

    pub(crate) fn scanned_key(key: Vec<u8>) -> Result<String, TitoError> {
        String::from_utf8(key).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Storage scan returned a non-UTF-8 key (valid through byte {})",
                error.utf8_error().valid_up_to()
            ))
        })
    }

    pub(crate) fn scanned_json(key: &str, value: &[u8]) -> Result<Value, TitoError> {
        serde_json::from_slice::<Value>(value).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize value for scanned key '{}': {}",
                key, error
            ))
        })
    }

    fn to_results(
        &self,
        items: impl IntoIterator<Item = TitoKvPair>,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut results = vec![];
        for (key_bytes, value_bytes) in items {
            let key = Self::scanned_key(key_bytes)?;
            let value = Self::scanned_json(&key, &value_bytes)?;
            results.push((key, value));
        }

        Ok(results)
    }
    pub(crate) async fn get_key(&self, key: &str, tx: &E::Transaction) -> Result<Value, TitoError> {
        let result = tx.get(key.to_string()).await?;

        let result = result
            .ok_or_else(|| TitoError::NotFound(format!("Key '{}' not found in database", key)))?;

        serde_json::from_slice::<Value>(&result).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize value for key '{}': {}",
                key, error
            ))
        })
    }

    fn value_with_options<P>(
        &self,
        payload: P,
        timestamps: bool,
        is_new: bool,
    ) -> Result<Value, TitoError>
    where
        P: Serialize,
    {
        let mut value = serde_json::to_value(&payload)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        if timestamps {
            if let serde_json::Value::Object(ref mut map) = value {
                let now = Utc::now().timestamp_millis();

                if is_new && map.contains_key("created_at") {
                    map.insert("created_at".to_string(), serde_json::json!(now));
                }
                if map.contains_key("updated_at") {
                    map.insert("updated_at".to_string(), serde_json::json!(now));
                }
            }
        }

        Ok(value)
    }

    async fn put_value(
        &self,
        key: String,
        value: &Value,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let bytes =
            serde_json::to_vec(value).map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
        tx.put(key, bytes).await
    }

    pub async fn delete(&self, key: String, tx: &E::Transaction) -> Result<bool, TitoError> {
        tx.delete(key).await?;

        Ok(true)
    }

    pub fn to_paginated_items_with_cursor(
        &self,
        items: Vec<(String, Value)>,
        cursor: String,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let mut results = vec![];

        for (key, value) in items {
            let item = serde_json::from_value::<T>(value).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize record for scanned key '{}': {}",
                    key, error
                ))
            })?;
            results.push(item);
        }

        let results = TitoPaginated::new(results, Some(cursor));

        Ok(results)
    }

    pub(crate) fn next_cursor(
        &self,
        has_more: bool,
        last_key: Option<String>,
    ) -> Result<Option<String>, TitoError> {
        match (has_more, last_key) {
            (true, Some(key)) => Ok(Some(self.encode_cursors(vec![Some(key)]).map_err(|e| {
                TitoError::SerializationFailed(format!("Failed to encode cursor: {}", e))
            })?)),
            _ => Ok(None),
        }
    }

    pub fn to_paginated_items(
        &self,
        items: Vec<(String, Value)>,
        has_more: bool,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let mut results = vec![];
        let mut last_item: Option<String> = None;

        for (key, value) in items {
            let item = serde_json::from_value::<T>(value).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize record for scanned key '{}': {}",
                    key, error
                ))
            })?;
            last_item = Some(key);
            results.push(item);
        }

        let cursor = self.next_cursor(has_more, last_item)?;

        let results = TitoPaginated::new(results, cursor);
        Ok(results)
    }

    fn deserialize_reverse_index(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> Result<ReverseIndex, TitoError> {
        serde_json::from_slice::<ReverseIndex>(bytes).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize reverse index for key '{}': {}",
                key, error
            ))
        })
    }

    fn validate_reverse_index_keys(
        &self,
        primary_key: &str,
        reverse_index: &ReverseIndex,
    ) -> Result<(), TitoError> {
        let table = T::table();
        if !primary_key.starts_with(&format!("{}:", self.get_table())) {
            return Err(TitoError::IndexError(format!(
                "Primary key '{}' does not belong to model '{}'",
                primary_key, table
            )));
        }
        let expected_suffix = format!(":{}", primary_key);
        let unique_prefix = format!("unique-index:{}:", table);
        for key in &reverse_index.value {
            let ordinary = key.starts_with("index:") && key.ends_with(&expected_suffix);
            let unique = key.starts_with(&unique_prefix);
            if !ordinary && !unique {
                return Err(TitoError::IndexError(format!(
                    "Reverse index for '{}' contains an invalid index key '{}'",
                    primary_key, key
                )));
            }
        }
        Ok(())
    }

    fn manifest_bytes(
        &self,
        primary_key: &str,
        manifest: &ReverseIndex,
    ) -> Result<Vec<u8>, TitoError> {
        if manifest.value.len() > MAX_MANIFEST_KEYS {
            return Err(TitoError::InvalidInput(format!(
                "Record '{}' needs more than {} index keys",
                primary_key, MAX_MANIFEST_KEYS
            )));
        }
        let bytes = serde_json::to_vec(manifest)
            .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(TitoError::InvalidInput(format!(
                "Record '{}' has an index manifest over one MiB",
                primary_key
            )));
        }
        Ok(bytes)
    }

    fn unique_index_name(key: &str) -> Option<&str> {
        let prefix = format!("unique-index:{}:", T::table());
        key.strip_prefix(&prefix)?.split(':').next()
    }

    async fn validate_unique_owner(
        &self,
        key: &str,
        expected_id: &str,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let Some(bytes) = tx.get(key).await? else {
            return Ok(false);
        };
        Ok(Self::unique_owner_id(key, bytes)? == expected_id)
    }

    pub async fn rebuild_indexes_for_restore(
        &self,
        id: &str,
        tx: &E::Transaction,
    ) -> Result<T, TitoError> {
        if id.is_empty() || id.len() > 512 {
            return Err(TitoError::InvalidInput("Invalid restore record ID".into()));
        }
        let primary_key = format!("{}:{id}", self.get_table());
        let reverse_key = format!("reverse-index:{primary_key}");
        let primary = tx.get(&primary_key).await?.ok_or_else(|| {
            TitoError::NotFound(format!("Record '{primary_key}' not found in database"))
        })?;
        let reverse = tx.get(&reverse_key).await?.ok_or_else(|| {
            TitoError::IndexError(format!("Record '{primary_key}' has no restore metadata"))
        })?;
        if reverse.len() > MAX_MANIFEST_BYTES {
            return Err(TitoError::IndexError(
                "Restore metadata exceeds one MiB".into(),
            ));
        }
        let metadata = self.deserialize_reverse_index(&reverse_key, &reverse)?;
        if metadata.value.len() > MAX_MANIFEST_KEYS {
            return Err(TitoError::IndexError(
                "Restore metadata exceeds 10000 keys".into(),
            ));
        }
        self.validate_reverse_index_keys(&primary_key, &metadata)?;
        let value: Value = serde_json::from_slice(&primary)
            .map_err(|error| TitoError::DeserializationFailed(error.to_string()))?;
        let stored: T = serde_json::from_value(value.clone())
            .map_err(|error| TitoError::DeserializationFailed(error.to_string()))?;
        if stored.id() != id {
            return Err(TitoError::IndexError(
                "Restore primary identity differs from its key".into(),
            ));
        }
        let owned = self.build_index_entries(&primary_key, &stored, &value, Some(&primary))?;
        let declared: HashSet<_> = metadata.value.iter().collect();
        let computed: HashSet<_> = owned.iter().map(|(key, _)| key).collect();
        if declared.len() != metadata.value.len()
            || computed.len() != owned.len()
            || declared != computed
        {
            return Err(TitoError::IndexError(
                "Restore metadata differs from current model indexes".into(),
            ));
        }
        for (key, _) in &owned {
            if let Some(index) = Self::unique_index_name(key) {
                if let Some(bytes) = tx.get(key).await? {
                    if Self::unique_owner_id(key, bytes)? != id {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.to_string(),
                        });
                    }
                }
            }
        }
        tx.put(primary_key, primary).await?;
        tx.put(reverse_key, reverse).await?;
        for (key, value) in owned {
            tx.put(key, value).await?;
        }
        for fence in record_fence_keys(&T::table(), id) {
            self.delete(fence, tx).await?;
        }
        tx.guard(&T::table(), id, TitoGuard::Written).await;
        Ok(stored)
    }

    async fn load_index_state(
        &self,
        primary_key: &str,
        tx: &E::Transaction,
    ) -> Result<Option<ReverseIndex>, TitoError> {
        let reverse_key = format!("reverse-index:{}", primary_key);
        let primary = tx.get(primary_key).await?;
        let reverse = tx.get(&reverse_key).await?;

        match (primary, reverse) {
            (None, None) => Ok(None),
            (Some(_), Some(bytes)) => {
                let reverse_index = self.deserialize_reverse_index(&reverse_key, &bytes)?;
                self.validate_reverse_index_keys(primary_key, &reverse_index)?;
                Ok(Some(reverse_index))
            }
            (Some(_), None) => Err(TitoError::IndexError(format!(
                "Primary record '{}' exists without reverse index '{}'",
                primary_key, reverse_key
            ))),
            (None, Some(_)) => Err(TitoError::IndexError(format!(
                "Reverse index '{}' exists without primary record '{}'",
                reverse_key, primary_key
            ))),
        }
    }
    pub fn get_nested_values(&self, json: &Value, field_path: &str) -> Option<Vec<FieldValue>> {
        let mut results = Vec::new();
        let mut to_process = vec![(json.clone(), 0)];
        let parts: Vec<&str> = field_path.split('.').collect();

        while let Some((current_value, depth)) = to_process.pop() {
            if depth == parts.len() {
                if let Some(obj) = current_value.as_object() {
                    for (key, value) in obj.iter() {
                        results.push(FieldValue::HashMapEntry {
                            key: key.clone(),
                            value: value.clone(),
                        });
                    }
                } else {
                    results.push(FieldValue::Simple(current_value));
                }
                continue;
            }

            match current_value.get(parts[depth]) {
                Some(nested) => {
                    if nested.is_array() {
                        if let Some(array) = nested.as_array() {
                            if array.is_empty() {
                                return None;
                            }
                            for item in array {
                                to_process.push((item.clone(), depth + 1));
                            }
                        }
                    } else {
                        to_process.push((nested.clone(), depth + 1));
                    }
                }
                None => return None,
            }
        }

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    pub fn set(&self, payload: T) -> SetBuilder<'_, E, T> {
        SetBuilder {
            model: self,
            payload,
            timestamps: true,
        }
    }

    async fn set_internal(
        &self,
        payload: T,
        timestamps: bool,
        tx: &E::Transaction,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let raw_id = payload.id();
        let id = format!("{}:{}", self.get_table(), raw_id);
        let reverse_key = format!("reverse-index:{}", id);
        let old_index_keys = self.load_index_state(&id, tx).await?;
        let stored_value =
            self.value_with_options(&payload, timestamps, old_index_keys.is_none())?;

        let all_index_data = self.get_index_keys(id.clone(), &payload, &stored_value)?;
        let manifest = ReverseIndex {
            value: all_index_data.iter().map(|(key, _)| key.clone()).collect(),
            version: TitoRecordVersion::from_transaction(tx.start_version())?,
        };
        let reverse_bytes = self.manifest_bytes(&id, &manifest)?;

        for (key, _) in &all_index_data {
            if let Some(index) = Self::unique_index_name(key) {
                if tx.get(key).await?.is_some() {
                    if !self.validate_unique_owner(key, &raw_id, tx).await? {
                        return Err(TitoError::UniqueViolation {
                            model: T::table(),
                            index: index.to_string(),
                        });
                    }
                    if old_index_keys
                        .as_ref()
                        .is_none_or(|metadata| !metadata.value.contains(key))
                    {
                        return Err(TitoError::IndexError(format!(
                            "Unique index '{}' on model '{}' is not declared by its owner",
                            index,
                            T::table()
                        )));
                    }
                }
            }
        }

        if let Some(old_index_keys) = old_index_keys {
            for key in old_index_keys.value {
                if Self::unique_index_name(&key).is_some()
                    && !self.validate_unique_owner(&key, &raw_id, tx).await?
                {
                    return Err(TitoError::IndexError(format!(
                        "Unique index on model '{}' is missing or owned by another record",
                        T::table()
                    )));
                }
                self.delete(key, tx).await?;
            }
            self.delete(reverse_key.clone(), tx).await?;
        }

        for fence in record_fence_keys(&T::table(), &raw_id) {
            self.delete(fence, tx).await?;
        }
        tx.guard(&T::table(), &raw_id, TitoGuard::Written).await;
        self.put_value(id, &stored_value, tx).await?;
        for (key, value) in all_index_data {
            tx.put(key, value).await?;
        }
        tx.put(reverse_key, reverse_bytes).await?;

        serde_json::from_value(stored_value).map_err(|e| {
            TitoError::DeserializationFailed(format!("Failed to deserialize stored value: {}", e))
        })
    }

    async fn get_one_with_tx(&self, id: &str, tx: &E::Transaction) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let key = format!("{}:{}", self.get_table(), id);

        let value = self.get_key(&key, tx).await?;
        let record = serde_json::from_value(value).map_err(|err| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize record with id '{}': {}",
                key, err
            ))
        })?;
        tx.guard(&T::table(), id, TitoGuard::Shared).await;
        Ok(record)
    }

    pub fn get(&self, id: &str) -> GetBuilder<'_, E, T> {
        GetBuilder {
            model: self,
            id: id.to_string(),
        }
    }

    pub async fn get_exclusive(&self, id: &str, tx: &E::Transaction) -> Result<T, TitoError> {
        let record = self.get_one_with_tx(id, tx).await?;
        tx.guard(&T::table(), id, TitoGuard::Exclusive).await;
        Ok(record)
    }

    pub async fn get_for_update(&self, id: &str, tx: &E::Transaction) -> Result<T, TitoError> {
        let record = self.get_one_with_tx(id, tx).await?;
        tx.guard(&T::table(), id, TitoGuard::ForUpdate).await;
        Ok(record)
    }

    pub async fn get_versioned(
        &self,
        id: &str,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoVersioned<T>, TitoError> {
        match tx {
            Some(tx) => self.get_versioned_with_tx(id, tx).await,
            None => {
                self.tx(|tx| async move { self.get_versioned_with_tx(id, &tx).await })
                    .await
            }
        }
    }

    async fn get_versioned_with_tx(
        &self,
        id: &str,
        tx: &E::Transaction,
    ) -> Result<TitoVersioned<T>, TitoError> {
        let primary_key = format!("{}:{id}", self.get_table());
        let metadata = self
            .load_index_state(&primary_key, tx)
            .await?
            .ok_or_else(|| {
                TitoError::NotFound(format!("Record '{primary_key}' not found in database"))
            })?;
        let value = self.get_one_with_tx(id, tx).await?;
        if value.id() != id {
            return Err(TitoError::DeserializationFailed(
                "Versioned model identity differs from its primary key".into(),
            ));
        }
        Ok(TitoVersioned {
            value,
            version: metadata.version,
        })
    }

    async fn get_internal(&self, id: &str, tx: Option<&E::Transaction>) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        match tx {
            Some(tx) => self.get_one_with_tx(id, tx).await,
            None => {
                let id = id.to_string();
                self.tx(|tx| {
                    let id = id.clone();
                    async move { self.get_one_with_tx(&id, &tx).await }
                })
                .await
            }
        }
    }

    pub async fn scan(
        &self,
        payload: TitoScanPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: DeserializeOwned,
    {
        let (rows, has_more) = self.scan_rows(payload, false, tx).await?;
        Ok((self.scanned_values(rows, tx).await?, has_more))
    }

    pub(crate) async fn scan_rows(
        &self,
        payload: TitoScanPayload,
        keys_only: bool,
        tx: &E::Transaction,
    ) -> Result<(Vec<TitoKvPair>, bool), TitoError> {
        let range_start = payload.start.into_bytes();
        let range_end = if let Some(end) = payload.end {
            end.into_bytes()
        } else {
            prefix_end_bytes(&range_start).ok_or_else(|| {
                TitoError::InvalidInput("Scan prefix has no finite range endpoint".to_string())
            })?
        };

        self.validate_scan_range(&range_start, &range_end)?;

        let start_bound = if let Some(cursor) = payload.cursor {
            let cursor = self.decode_cursor(cursor)?.first_id()?.into_bytes();
            self.validate_cursor_in_range(&cursor, &range_start, &range_end)?;
            key_after_bytes(&cursor)
        } else {
            range_start
        };

        let limit = payload.limit.unwrap_or(u32::MAX);
        self.validate_scan_limit(limit)?;

        if start_bound >= range_end {
            return Ok((Vec::new(), false));
        }

        let limit_plus_one = if limit == u32::MAX {
            u32::MAX
        } else {
            limit + 1
        };

        let mut items = if keys_only {
            tx.scan_keys(start_bound..range_end, limit_plus_one)
                .await?
                .into_iter()
                .map(|key| (key, Vec::new()))
                .collect()
        } else {
            tx.scan(start_bound..range_end, limit_plus_one).await?
        };

        let has_more = if limit == u32::MAX {
            false
        } else {
            items.len() > limit as usize
        };

        if has_more {
            items.truncate(limit as usize);
        }

        Ok((items, has_more))
    }

    pub(crate) async fn get_many_raw(
        &self,
        ids: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError>
    where
        T: DeserializeOwned,
    {
        let ids = ids
            .into_iter()
            .map(|id| format!("{}:{}", self.get_table(), id))
            .collect();

        self.batch_get(ids, tx).await
    }

    async fn get_many_with_tx(
        &self,
        ids: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        let prefix = format!("{}:", self.get_table());
        let items = self.get_many_raw(ids, tx).await?;

        let mut result = vec![];

        for (key, value) in items {
            let item = serde_json::from_value::<T>(value).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize record for key '{}': {}",
                    key, error
                ))
            })?;
            let id = key.strip_prefix(&prefix).ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Batch read returned key '{}' outside model '{}'",
                    key,
                    T::table()
                ))
            })?;
            tx.guard(&T::table(), id, TitoGuard::Shared).await;
            result.push(item);
        }

        Ok(result)
    }

    pub fn get_many(&self, ids: Vec<String>) -> GetManyBuilder<'_, E, T> {
        GetManyBuilder { model: self, ids }
    }

    async fn get_many_internal(
        &self,
        ids: Vec<String>,
        tx: Option<&E::Transaction>,
    ) -> Result<Vec<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        match tx {
            Some(tx) => self.get_many_with_tx(ids, tx).await,
            None => {
                self.tx(|tx| {
                    let ids = ids.clone();
                    async move { self.get_many_with_tx(ids, &tx).await }
                })
                .await
            }
        }
    }

    pub async fn scan_reverse(
        &self,
        payload: TitoScanPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: DeserializeOwned,
    {
        let (rows, has_more) = self.scan_rows_reverse(payload, tx).await?;
        Ok((self.scanned_values(rows, tx).await?, has_more))
    }

    pub(crate) async fn scan_rows_reverse(
        &self,
        payload: TitoScanPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<TitoKvPair>, bool), TitoError> {
        let start_bound = payload.start.into_bytes();
        let range_end = if let Some(end) = payload.end {
            end.into_bytes()
        } else {
            prefix_end_bytes(&start_bound).ok_or_else(|| {
                TitoError::InvalidInput("Scan prefix has no finite range endpoint".to_string())
            })?
        };

        self.validate_scan_range(&start_bound, &range_end)?;

        let end_bound = if let Some(cursor) = payload.cursor {
            let cursor = self.decode_cursor(cursor)?.first_id()?.into_bytes();
            self.validate_cursor_in_range(&cursor, &start_bound, &range_end)?;
            cursor
        } else {
            range_end
        };

        let limit = payload.limit.unwrap_or(u32::MAX);
        self.validate_scan_limit(limit)?;

        if end_bound <= start_bound {
            return Ok((Vec::new(), false));
        }

        let limit_plus_one = if limit == u32::MAX {
            u32::MAX
        } else {
            limit + 1
        };

        let mut items = tx
            .scan_reverse(start_bound..end_bound, limit_plus_one)
            .await?;

        let has_more = if limit == u32::MAX {
            false
        } else {
            items.len() > limit as usize
        };

        if has_more {
            items.truncate(limit as usize);
        }

        Ok((items, has_more))
    }

    pub fn get_last_id(&self, key: String) -> Option<String> {
        let parts: Vec<&str> = key.split(':').collect();
        parts.last().map(|last| last.to_string())
    }

    pub(crate) async fn batch_get(
        &self,
        keys: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        match tx.batch_get(keys).await {
            Ok(res) => self.to_results(res),
            Err(e) => Err(e),
        }
    }

    pub async fn remove_by_index(
        &self,
        index: &str,
        value: &str,
        batch_size: u32,
        tx: &E::Transaction,
    ) -> Result<Vec<String>, TitoError>
    where
        T: DeserializeOwned,
    {
        let mut query = self.query_by_index(index);
        query.value(value.to_string());
        query.limit(Some(batch_size));
        let ids = query.execute_ids(Some(tx)).await?.items;

        for id in &ids {
            self.remove(id, tx).await?;
        }

        Ok(ids)
    }

    pub async fn remove(&self, raw_id: &str, tx: &E::Transaction) -> Result<bool, TitoError> {
        let id = format!("{}:{}", self.get_table(), raw_id);

        let mut keys = match self.load_index_state(&id, tx).await? {
            Some(metadata) => metadata.value,
            None => return Err(TitoError::NotFound(format!("Entity not found: {}", id))),
        };

        keys.extend(record_fence_keys(&T::table(), raw_id));
        let removed = self.remove_keys(&id, raw_id, keys, tx).await?;
        tx.guard(&T::table(), raw_id, TitoGuard::Written).await;
        Ok(removed)
    }

    async fn remove_keys(
        &self,
        id: &str,
        raw_id: &str,
        mut keys: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        keys.push(id.to_string());
        keys.push(format!("reverse-index:{}", id));

        for key in &keys {
            if Self::unique_index_name(key).is_some()
                && !self.validate_unique_owner(key, raw_id, tx).await?
            {
                return Err(TitoError::IndexError(format!(
                    "Unique index on model '{}' is missing or owned by another record",
                    T::table()
                )));
            }
        }

        for key in keys {
            self.delete(key, tx).await?;
        }

        Ok(true)
    }

    pub async fn find(&self, payload: TitoFindPayload) -> Result<TitoPaginated<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        let table_prefix = format!("{}:", self.get_table());
        let start_bound = format!("{}{}", table_prefix, payload.start);
        let end_bound = payload
            .end
            .as_ref()
            .map(|end| format!("{}{}", table_prefix, end));

        self.tx(|tx| {
            let start_bound = start_bound.clone();
            let end_bound = end_bound.clone();
            let payload = payload.clone();
            async move {
                let (scan_stream, has_more) = self
                    .scan(
                        TitoScanPayload {
                            start: start_bound,
                            end: end_bound,
                            limit: payload.limit,
                            cursor: payload.cursor.clone(),
                        },
                        &tx,
                    )
                    .await?;

                self.to_paginated_items(scan_stream, has_more)
            }
        })
        .await
    }
}
