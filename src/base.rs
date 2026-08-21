use std::future::Future;

use std::marker::PhantomData;

use crate::{
    error::TitoError,
    query::IndexQueryBuilder,
    types::{
        FieldValue, ReverseIndex, TitoCursor, TitoEngine, TitoFindPayload, TitoKvPair,
        TitoModelOptions, TitoPaginated, TitoScanPayload, TitoTransaction,
    },
    utils::{key_after_bytes, prefix_end_bytes},
};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

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

    fn to_results(
        &self,
        items: impl IntoIterator<Item = TitoKvPair>,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut results = vec![];
        for (key_bytes, value_bytes) in items {
            let key = String::from_utf8(key_bytes).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Storage scan returned a non-UTF-8 key (valid through byte {})",
                    error.utf8_error().valid_up_to()
                ))
            })?;
            let value = serde_json::from_slice::<Value>(&value_bytes).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize value for scanned key '{}': {}",
                    key, error
                ))
            })?;
            results.push((key, value));
        }

        Ok(results)
    }
    async fn get_raw(&self, key: &str, tx: &E::Transaction) -> Result<(String, Value), TitoError> {
        let key = key.to_string();
        let value = tx
            .get(key.clone())
            .await?
            .ok_or_else(|| TitoError::NotFound(format!("Key '{}' not found in database", key)))?;
        let value = serde_json::from_slice::<Value>(&value).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize value for key '{}': {}",
                key, error
            ))
        })?;

        Ok((key, value))
    }
    pub async fn get_key(&self, key: &str, tx: &E::Transaction) -> Result<Value, TitoError> {
        let result = tx.get(key.to_string()).await?;

        let result =
            result.ok_or_else(|| TitoError::NotFound(format!("Key '{}' not found", key)))?;

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
                let now = Utc::now().timestamp();

                if is_new {
                    map.insert("created_at".to_string(), serde_json::json!(now));
                }
                map.insert("updated_at".to_string(), serde_json::json!(now));
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

        let cursor = match (has_more, last_item) {
            (true, Some(item)) => Some(self.encode_cursors(vec![Some(item)]).map_err(|e| {
                TitoError::SerializationFailed(format!("Failed to encode cursor: {}", e))
            })?),
            _ => None,
        };

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
        let expected_suffix = format!(":{}", primary_key);
        for key in &reverse_index.value {
            if !key.starts_with("index:") || !key.ends_with(&expected_suffix) {
                return Err(TitoError::IndexError(format!(
                    "Reverse index for '{}' contains an invalid index key '{}'",
                    primary_key, key
                )));
            }
        }
        Ok(())
    }

    async fn load_index_state(
        &self,
        primary_key: &str,
        tx: &E::Transaction,
    ) -> Result<Option<Vec<String>>, TitoError> {
        let reverse_key = format!("reverse-index:{}", primary_key);
        let primary = tx.get(primary_key).await?;
        let reverse = tx.get(&reverse_key).await?;

        match (primary, reverse) {
            (None, None) => Ok(None),
            (Some(_), Some(bytes)) => {
                let reverse_index = self.deserialize_reverse_index(&reverse_key, &bytes)?;
                self.validate_reverse_index_keys(primary_key, &reverse_index)?;
                Ok(Some(reverse_index.value))
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
        let index_json_key = ReverseIndex {
            value: all_index_data.iter().map(|(key, _)| key.clone()).collect(),
        };
        let reverse_value = self.value_with_options(index_json_key, false, true)?;

        if let Some(old_index_keys) = old_index_keys {
            for key in old_index_keys {
                self.delete(key, tx).await?;
            }
            self.delete(reverse_key.clone(), tx).await?;
        }

        self.put_value(id, &stored_value, tx).await?;
        for (key, value) in all_index_data {
            self.put_value(key, &value, tx).await?;
        }
        self.put_value(reverse_key, &reverse_value, tx).await?;

        serde_json::from_value(stored_value).map_err(|e| {
            TitoError::DeserializationFailed(format!("Failed to deserialize stored value: {}", e))
        })
    }

    async fn get_one_with_tx(&self, id: &str, tx: &E::Transaction) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let id = format!("{}:{}", self.get_table(), id);

        let (_, value) = self.get_raw(&id, tx).await?;
        serde_json::from_value(value).map_err(|err| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize record with id '{}': {}",
                id, err
            ))
        })
    }

    pub fn get(&self, id: &str) -> GetBuilder<'_, E, T> {
        GetBuilder {
            model: self,
            id: id.to_string(),
        }
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

        let scan_stream = tx.scan(start_bound..range_end, limit_plus_one).await?;

        let mut items = self.to_results(scan_stream)?;

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

    pub async fn get_many_raw(
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
        let items = self.get_many_raw(ids, tx).await?;

        let mut result = vec![];

        for (key, value) in items {
            let item = serde_json::from_value::<T>(value).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize record for key '{}': {}",
                    key, error
                ))
            })?;
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

        let scan_stream = tx
            .scan_reverse(start_bound..end_bound, limit_plus_one)
            .await?;

        let mut items = self.to_results(scan_stream)?;

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

    pub async fn batch_get(
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
        let items = query.execute(Some(tx)).await?;

        if items.items.is_empty() {
            return Ok(vec![]);
        }

        let mut ids = vec![];
        for item in items.items {
            let id = item.id();
            self.remove(&id, tx).await?;
            ids.push(id);
        }

        Ok(ids)
    }

    pub async fn remove(&self, raw_id: &str, tx: &E::Transaction) -> Result<bool, TitoError> {
        let id = format!("{}:{}", self.get_table(), raw_id);
        let reverse_index_key = format!("reverse-index:{}", id);

        let mut keys = match self.load_index_state(&id, tx).await? {
            Some(keys) => keys,
            None => return Err(TitoError::NotFound(format!("Entity not found: {}", id))),
        };

        keys.push(id.clone());
        keys.push(reverse_index_key);

        for key in keys.into_iter() {
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
