use std::{future::Future, sync::atomic::Ordering};

use crate::{
    error::TitoError,
    query::IndexQueryBuilder,
    types::{
        DBUuid, FieldValue, ReverseIndex, TitoCursor, TitoEmbeddedRelationshipConfig, TitoEngine,
        TitoEvent, TitoFindPayload, TitoGenerateEventPayload, TitoKvPair,
        TitoModelTrait, TitoOperation, TitoOptions, TitoPaginated, TitoRelationshipConfig,
        TitoScanPayload, TitoTransaction,
    },
    utils::{next_string_lexicographically, previous_string_lexicographically},
};
use base64::{decode, encode};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct TitoModel<E: TitoEngine, T> {
    pub model: T,
    pub engine: E,
}

impl<E: TitoEngine, T: crate::types::TitoModelConstraints> TitoModel<E, T> {
    pub(crate) fn new(engine: E) -> Self {
        Self {
            model: T::default(),
            engine,
        }
    }

    pub fn relationships(&self) -> Vec<TitoRelationshipConfig> {
        self.model.relationships()
    }

    pub fn get_table(&self) -> String {
        format!("table:{}", self.model.table())
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
        let cursor = decode(cursor).map_err(|err| {
            TitoError::DeserializationFailed("Failed to decode cursor".to_string())
        })?;
        if let Ok(value) = serde_json::from_slice::<TitoCursor>(&cursor) {
            return Ok(value);
        }
        return Err(TitoError::DeserializationFailed(
            "Failed to deserialize cursor".to_string(),
        ));
    }

    fn encode_cursors(&self, ids: Vec<Option<String>>) -> Result<String, TitoError> {
        let tikv_cursor = TitoCursor { ids };
        let json_bytes = serde_json::to_vec(&tikv_cursor).map_err(|_| {
            TitoError::SerializationFailed("Failed to serialize cursor".to_string())
        })?;
        Ok(encode(&json_bytes))
    }

    pub async fn tx<F, Fut, R, Err>(&self, f: F) -> Result<R, Err>
    where
        F: FnOnce(E::Transaction) -> Fut + Send,
        Fut: Future<Output = Result<R, Err>> + Send,
        Err: From<TitoError> + Send + Sync + std::fmt::Debug, // Added Sync trait bound
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
            let key = match String::from_utf8(key_bytes) {
                Ok(k) => k,
                Err(_) => {
                    continue;
                }
            };

            match serde_json::from_slice::<Value>(&value_bytes) {
                Ok(value) => results.push((key, value)),
                Err(_err) => continue,
            }
        }

        Ok(results)
    }
    async fn get(
        &self,
        key: &str,
        max_retries: usize,
        initial_delay_ms: u64,
        tx: &E::Transaction,
    ) -> Result<(String, Value), TitoError> {
        let mut retries = 0;
        let mut delay = initial_delay_ms;
        let key = key.to_string();

        loop {
            match tx.get_for_update(key.clone()).await {
                Ok(Some(value)) => match serde_json::from_slice::<Value>(&value) {
                    Ok(value) => return Ok((key, value)),
                    Err(e) => {
                        return Err(TitoError::NotFound(format!(
                            "Failed to deserialize value for key '{}': {}",
                            key, e
                        )))
                    }
                },
                Ok(None) => {
                    return Err(TitoError::NotFound(format!(
                        "Key '{}' not found in database",
                        key
                    )))
                }
                Err(e) => {
                    if retries >= max_retries {
                        return Err(TitoError::NotFound(format!(
                            "Failed to get key '{}' after {} retries: {}",
                            key, max_retries, e
                        )));
                    }
                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                }
            }
        }
    }
    pub async fn get_key(&self, key: &str, tx: &E::Transaction) -> Result<Value, TitoError> {
        let result = tx
            .get(key.to_string())
            .await
            .map_err(|e| TitoError::NotFound(e.to_string()))?;

        let result = result.ok_or(TitoError::NotFound("Not found".to_string()))?;

        serde_json::from_slice::<Value>(&result)
            .map_err(|_| TitoError::NotFound("Not found".to_string()))
    }

    async fn put<P>(&self, key: String, payload: P, tx: &E::Transaction) -> Result<bool, TitoError>
    where
        P: Serialize + Unpin + std::marker::Send + Sync,
    {
        let mut retries = 0;
        let mut delay = 10;
        let max_retries = 10;

        let mut value = serde_json::to_value(&payload)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        if let serde_json::Value::Object(ref mut map) = value {
            let now = Utc::now().timestamp();

            let existing = tx.get(&key).await;
            if existing.is_err() || existing.unwrap().is_none() {
                map.insert("created_at".to_string(), serde_json::json!(now));
            }
            map.insert("updated_at".to_string(), serde_json::json!(now));
        }

        loop {
            if self.engine.configs().is_read_only.load(Ordering::SeqCst) {
                if retries >= max_retries {
                    return Err(TitoError::ReadOnlyMode);
                }

                sleep(Duration::from_millis(delay)).await;
                retries += 1;
                delay *= 2;
            }

            let bytes = serde_json::to_vec(&value)
                .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

            match tx.put(key.clone(), bytes).await {
                Ok(()) => return Ok(true),
                Err(e) => {
                    if retries >= max_retries {
                        return Err(TitoError::CreateFailed(e.to_string()));
                    }

                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                }
            }
        }
    }

    pub async fn delete(&self, key: String, tx: &E::Transaction) -> Result<bool, TitoError> {
        let mut retries = 0;
        let mut delay = 10;
        let max_retries = 10;

        loop {
            if self.engine.configs().is_read_only.load(Ordering::SeqCst) {
                if retries >= max_retries {
                    return Err(TitoError::ReadOnlyMode);
                }

                sleep(Duration::from_millis(delay)).await;
                retries += 1;
                delay *= 2;
            }

            match tx.delete(key.clone()).await {
                Ok(()) => {
                    return Ok(true);
                }
                Err(e) => {
                    if retries >= max_retries {
                        return Err(TitoError::DeleteFailed(e.to_string()));
                    }

                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                }
            }
        }
    }

    pub fn to_paginated_items_with_cursor(
        &self,
        items: Vec<(String, Value)>,
        cursor: String,
    ) -> Result<TitoPaginated<T>, TitoError> {
        let mut results = vec![];

        for item in items.into_iter() {
            if let Ok(item) = serde_json::from_value::<T>(item.1) {
                results.push(item);
            }
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

        for item in items.into_iter() {
            last_item = Some(item.0.clone());
            if let Ok(item) = serde_json::from_value::<T>(item.1) {
                results.push(item);
            }
        }

        let cursor = if has_more && last_item.is_some() {
            Some(
                self.encode_cursors(vec![Some(last_item.unwrap())])
                    .expect("Failed to encode cursor"),
            )
        } else {
            None
        };

        let results = TitoPaginated::new(results, cursor);
        Ok(results)
    }

    async fn get_reverse_index(
        &self,
        key: &str,
        tx: &E::Transaction,
    ) -> Result<ReverseIndex, TitoError> {
        let result = tx.get(key.to_string()).await.map_err(|e| {
            TitoError::NotFound(format!(
                "Failed to get reverse index for key '{}': {}",
                key, e
            ))
        })?;

        let result = result.ok_or(TitoError::NotFound(format!(
            "Reverse index not found for key '{}'",
            key
        )))?;

        serde_json::from_slice::<ReverseIndex>(&result).map_err(|e| {
            TitoError::NotFound(format!(
                "Failed to deserialize reverse index for key '{}': {}",
                key, e
            ))
        })
    }
    pub fn get_nested_values(&self, json: &Value, field_path: &str) -> Option<Vec<FieldValue>> {
        let mut results = Vec::new();
        let mut to_process = vec![(json.clone(), 0)];
        let parts: Vec<&str> = field_path.split('.').collect();

        while let Some((current_value, depth)) = to_process.pop() {
            if depth == parts.len() {
                // Check if the final value is a HashMap (JSON object)
                // If so, expand it into key-value pairs
                if let Some(obj) = current_value.as_object() {
                    // This is a HashMap - expand each entry
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
                            // If array is empty, return None
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
                None => return None, // Return None if any part of the path is missing
            }
        }

        if results.is_empty() {
            None
        } else {
            Some(results)
        }
    }

    pub async fn build(&self, payload: T, tx: &E::Transaction) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let metadata = serde_json::to_value(&payload).unwrap_or_default();
        let options = TitoOptions::with_metadata(TitoOperation::Insert, metadata);
        self.build_with_options(payload, options, tx).await
    }

    pub async fn build_with_options(
        &self,
        payload: T,
        options: TitoOptions,
        tx: &E::Transaction,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = serde_json::to_value(&payload).unwrap();

        let raw_id = payload.id();

        let id = format!("{}:{}", self.get_table(), raw_id);

        self.put(id.clone(), &value, tx).await?;

        let all_index_data = self.get_index_keys(id.clone(), &payload.clone(), &value)?;

        let mut all_index_keys = vec![];

        for data in all_index_data.clone() {
            all_index_keys.push(data.0.clone());
            self.put(data.0.clone(), &data.1, tx).await?;
        }

        let index_json_key = ReverseIndex {
            value: all_index_keys.clone(),
        };

        let reverse_key = format!("reverse-index:{}", id);

        self.put(reverse_key.clone(), index_json_key, tx).await?;

        self.generate_event(
            TitoGenerateEventPayload {
                key: id.clone(),
                operation: options.operation,
                event: options.event.clone(),
            },
            &payload,
            tx,
        )
        .await?;

        // Sync references graph (outbound from this entity)
        let source_typed = format!("{}:{}", self.model.table(), payload.id());
        let targets = payload.references();
        self.sync_references(&source_typed, targets, tx).await?;

        Ok(payload)
    }

    async fn generate_event(
        &self,
        payload: TitoGenerateEventPayload,
        model: &T,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        use crate::types::EventConfig;

        if matches!(payload.event, EventConfig::None) {
            return Ok(true);
        }

        let metadata = match payload.event {
            EventConfig::None => unreachable!(),
            EventConfig::Generate => serde_json::Value::Null,
            EventConfig::GenerateWithMetadata(custom_meta) => custom_meta,
        };

        for event_config in model.events().iter() {
            self.lock_keys(vec![payload.key.clone()], tx).await?;
            let created_at = Utc::now().timestamp();

            let message = event_config.name.to_string();

            let uuid_str = DBUuid::new_v4().to_string();

            use crate::types::{PARTITION_DIGITS, SEQUENCE_DIGITS, TOTAL_PARTITIONS};
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let status = String::from("PENDING");

            let partition_key = model.partition_key();
            let mut hasher = DefaultHasher::new();
            partition_key.hash(&mut hasher);
            let partition = (hasher.finish() as u32) % TOTAL_PARTITIONS;

            let sequence = Utc::now().timestamp_micros() as u64;

            let key = format!(
                "event:{}:{:0pwidth$}:{:0swidth$}",
                status,
                partition,
                sequence,
                pwidth = PARTITION_DIGITS,
                swidth = SEQUENCE_DIGITS
            );

            let event = TitoEvent {
                id: uuid_str,
                key: key.clone(),
                entity: payload.key.clone(),
                action: payload.operation.to_string(),
                status,
                message,
                retries: 0,
                max_retries: 5,
                created_at: created_at,
                updated_at: created_at,
                metadata: metadata.clone(),
            };

            self.put(key.clone(), &event, tx).await?;
        }

        Ok(true)
    }

    pub async fn find_by_id_tx(
        &self,
        id: &str,
        rels: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let id = format!("{}:{}", self.get_table(), id);

        let value = match self.get(&id, 10, 10, tx).await {
            Ok(value) => value,
            Err(e) => {
                return Err(TitoError::NotFound(format!(
                    "Failed to get record with id '{}': {}",
                    id, e
                )));
            }
        };

        let items = match self
            .fetch_and_stitch_relationships(vec![value], rels.clone(), tx)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                return Err(TitoError::NotFound(format!(
                    "Failed to fetch relationships for id '{}' with rels {:?}: {}",
                    id, rels, e
                )));
            }
        };

        if let Some(value) = items.get(0) {
            serde_json::from_value(value.1.clone()).map_err(|err| {
                TitoError::NotFound(format!(
                    "Failed to deserialize record with id '{}': {}",
                    id, err
                ))
            })
        } else {
            Err(TitoError::NotFound(format!(
                "No record found with id '{}'",
                id
            )))
        }
    }
    pub async fn find_by_id(&self, id: &str, rels: Vec<String>) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.tx(|tx| async move { self.find_by_id_tx(id, rels, &tx).await })
            .await
    }

    pub async fn scan(
        &self,
        payload: TitoScanPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: DeserializeOwned,
    {
        let mut start_bound = format!("{}", payload.start);
        if let Some(cursor) = payload.cursor.clone() {
            let cursor = self.decode_cursor(cursor)?.first_id()?;
            let after_cursor = next_string_lexicographically(cursor);
            start_bound = after_cursor;
        }

        let end_bound = if let Some(end) = payload.end.clone() {
            end
        } else {
            next_string_lexicographically(payload.start.clone())
        };

        let limit = payload.limit.unwrap_or(u32::MAX);

        let limit_plus_one = if limit == u32::MAX {
            u32::MAX
        } else {
            limit + 1
        };

        let scan_stream = tx
            .scan(start_bound..end_bound, limit_plus_one)
            .await
            .map_err(|e| TitoError::NotFound(e.to_string()))?;

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

    pub async fn find_by_ids_tx(
        &self,
        ids: Vec<String>,
        rels: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        let items = self.find_by_ids_raw(ids, rels, tx).await?;

        let mut result = vec![];

        for value in items.into_iter() {
            if let Ok(item) = serde_json::from_value::<T>(value.1) {
                result.push(item);
            }
        }

        Ok(result)
    }

    pub async fn find_by_ids_raw(
        &self,
        ids: Vec<String>,
        rels: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError>
    where
        T: DeserializeOwned,
    {
        let ids = ids
            .into_iter()
            .map(|id| format!("{}:{}", self.get_table(), id))
            .collect();

        let items = self.batch_get(ids, 10, 10, tx).await?;
        let items = self.fetch_and_stitch_relationships(items, rels, tx).await?;

        Ok(items)
    }

    pub async fn find_by_ids(
        &self,
        ids: Vec<String>,
        rels: Vec<String>,
    ) -> Result<Vec<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        self.tx(|tx| async move {
            let items = self.find_by_ids_raw(ids, rels, &tx).await?;
            let mut result = vec![];
            for value in items.into_iter() {
                if let Ok(item) = serde_json::from_value::<T>(value.1) {
                    result.push(item);
                }
            }
            Ok(result)
        })
        .await
    }

    pub async fn scan_reverse(
        &self,
        payload: TitoScanPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: DeserializeOwned,
    {
        let start_bound = format!("{}", payload.start.clone());

        let mut end_bound = if let Some(end) = payload.end {
            end
        } else {
            next_string_lexicographically(payload.start.clone())
        };

        if let Some(cursor) = payload.cursor {
            let cursor = self.decode_cursor(cursor)?.first_id()?;
            let after_cursor = previous_string_lexicographically(cursor.clone());
            end_bound = after_cursor;
        }

        let limit = payload.limit.unwrap_or(u32::MAX);

        let limit_plus_one = if limit == u32::MAX {
            u32::MAX
        } else {
            limit + 1
        };

        let scan_stream = tx
            .scan_reverse(start_bound..end_bound, limit_plus_one)
            .await
            .map_err(|e| TitoError::NotFound(e.to_string()))?;

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

    pub async fn update(&self, payload: T, tx: &E::Transaction) -> Result<bool, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let metadata = serde_json::to_value(&payload).unwrap_or_default();
        let options = TitoOptions::with_metadata(TitoOperation::Update, metadata);
        self.update_with_options(payload, options, tx).await
    }

    pub fn get_last_id(&self, key: String) -> Option<String> {
        let parts: Vec<&str> = key.split(':').collect();
        parts.last().map(|last| last.to_string())
    }

    pub async fn update_with_options(
        &self,
        payload: T,
        options: TitoOptions,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let raw_id = payload.id();

        let deleted = self
            .delete_by_id_with_options(
                &raw_id,
                TitoOptions::skip_events(TitoOperation::Delete),
                tx,
            )
            .await;

        self.build_with_options(payload, options, tx).await?;

        Ok(true)
    }

    pub async fn lock_keys(
        &self,
        keys: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let keys: Vec<_> = keys
            .into_iter()
            .map(|key| format!("lock:{}", key).into_bytes())
            .collect();

        let mut retries = 0;
        let mut delay = 10;
        let max_retries = 10;

        loop {
            match tx.batch_get_for_update(keys.clone()).await {
                Ok(_) => return Ok(true),

                Err(_) => {
                    if retries >= max_retries {
                        return Err(TitoError::NotFound("Not found relationship".to_string()));
                    }

                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                    continue;
                }
            }
        }
    }

    pub async fn batch_get(
        &self,
        keys: Vec<String>,
        max_retries: usize,
        initial_delay_ms: u64,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut retries = 0;
        let mut delay = initial_delay_ms;

        loop {
            match tx.batch_get_for_update(keys.clone()).await {
                Ok(res) => {
                    return self.to_results(res.into_iter());
                }
                Err(e) => {
                    if retries >= max_retries {
                        return Err(TitoError::NotFound(format!(
                            "Failed to batch get keys {:?} after {} retries: {}",
                            keys, max_retries, e
                        )));
                    }
                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                }
            }
        }
    }

    pub async fn delete_by_id_with_options(
        &self,
        raw_id: &str,
        options: TitoOptions,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let id = format!("{}:{}", self.get_table(), raw_id);
        let reverse_index_key = format!("reverse-index:{}", id);

        // Clear references graph for this source (typed)
        let source_typed = format!("{}:{}", self.model.table(), raw_id);
        self.clear_references(&source_typed, tx).await?;

        let (mut metadata, model) = match tx.get(&id).await {
            Ok(Some(entity_data)) => {
                let metadata = match serde_json::from_slice::<serde_json::Value>(&entity_data) {
                    Ok(entity_json) => entity_json,
                    Err(_) => serde_json::json!({}),
                };
                let model = serde_json::from_slice::<T>(&entity_data).map_err(|e| {
                    TitoError::DeserializationFailed(format!(
                        "Failed to deserialize model for delete event: {}",
                        e
                    ))
                })?;
                (metadata, model)
            }
            Ok(None) => return Err(TitoError::NotFound(format!("Entity not found: {}", id))),
            Err(e) => {
                return Err(TitoError::QueryFailed(format!(
                    "Failed to fetch entity: {}",
                    e
                )))
            }
        };

        let reverse_index = self.get_reverse_index(&reverse_index_key, tx).await?;
        let mut keys = reverse_index.value;

        keys.push(id.clone());

        keys.push(reverse_index_key);

        for key in keys.into_iter() {
            self.delete(key, tx).await?;
        }

        self.generate_event(
            TitoGenerateEventPayload {
                key: id.to_string(),
                operation: options.operation,
                event: options.event.clone(),
            },
            &model,
            tx,
        )
        .await?;

        Ok(true)
    }

    pub async fn delete_by_id(&self, raw_id: &str, tx: &E::Transaction) -> Result<bool, TitoError> {
        self.delete_by_id_with_options(
            raw_id,
            TitoOptions::with_events(TitoOperation::Delete),
            tx,
        )
        .await
    }

    // References graph API
    pub async fn has_inbound_references(
        engine: &E,
        target_id: &str,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let prefix = format!("reference:i:target:{}:", target_id);
        let end = next_string_lexicographically(prefix.clone());
        let results = tx
            .scan(prefix.clone()..end, 1)
            .await
            .map_err(|e| TitoError::QueryFailed(e.to_string()))?;
        Ok(!results.is_empty())
    }

    pub async fn inbound_references(
        engine: &E,
        target_id: &str,
        limit: u32,
        tx: &E::Transaction,
    ) -> Result<Vec<String>, TitoError> {
        let prefix = format!("reference:i:target:{}:", target_id);
        let end = next_string_lexicographically(prefix.clone());
        let items = tx
            .scan(prefix.clone()..end, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(e.to_string()))?;
        let mut out = Vec::new();
        for (k, _) in items {
            if let Ok(s) = String::from_utf8(k) {
                if let Some(src) = s.split("source:").nth(1) {
                    out.push(src.to_string());
                }
            }
        }
        Ok(out)
    }

    pub async fn outbound_references(
        engine: &E,
        source_id: &str,
        limit: u32,
        tx: &E::Transaction,
    ) -> Result<Vec<String>, TitoError> {
        let prefix = format!("reference:o:source:{}:", source_id);
        let end = next_string_lexicographically(prefix.clone());
        let items = tx
            .scan(prefix.clone()..end, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(e.to_string()))?;
        let mut out = Vec::new();
        for (k, _) in items {
            if let Ok(s) = String::from_utf8(k) {
                if let Some(dst) = s.split("target:").nth(1) {
                    out.push(dst.to_string());
                }
            }
        }
        Ok(out)
    }

    async fn clear_references(
        &self,
        source_id: &str,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let prefix = format!("reference:o:source:{}:", source_id);
        let end = next_string_lexicographically(prefix.clone());
        let items = tx
            .scan(prefix.clone()..end, 10_000)
            .await
            .map_err(|e| TitoError::QueryFailed(e.to_string()))?;
        for (k, _) in items {
            if let Ok(s) = String::from_utf8(k.clone()) {
                if let Some(dst) = s.split("target:").nth(1) {
                    // delete forward
                    tx.delete(k.clone())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(e.to_string()))?;
                    // delete reverse
                    let rev = format!("reference:i:target:{}:source:{}", dst, source_id);
                    let _ = tx.delete(rev).await; // ignore missing
                }
            }
        }
        Ok(())
    }

    async fn sync_references(
        &self,
        source_id: &str,
        targets: Vec<String>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        // Clear old
        self.clear_references(source_id, tx).await?;
        // Insert new
        use std::collections::HashSet;
        let unique: HashSet<String> = targets.into_iter().filter(|t| !t.is_empty()).collect();
        for dst in unique.into_iter() {
            let fwd = format!("reference:o:source:{}:target:{}", source_id, dst);
            let marker = serde_json::to_vec(&1)
                .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
            tx.put(&fwd, marker.clone())
                .await
                .map_err(|e| TitoError::CreateFailed(e.to_string()))?;
            let rev = format!("reference:i:target:{}:source:{}", dst, source_id);
            tx.put(&rev, marker.clone())
                .await
                .map_err(|e| TitoError::CreateFailed(e.to_string()))?;
        }
        Ok(())
    }

    pub async fn find(&self, payload: TitoFindPayload) -> Result<TitoPaginated<T>, TitoError>
    where
        T: DeserializeOwned,
    {
        let start_bound = format!("{}:{}", self.get_table(), payload.start);

        self.tx(|tx| async move {
            let (scan_stream, has_more) = self
                .scan(
                    TitoScanPayload {
                        start: start_bound,
                        end: None,
                        limit: payload.limit,
                        cursor: payload.cursor.clone(),
                    },
                    &tx,
                )
                .await?;

            let items = self
                .fetch_and_stitch_relationships(scan_stream, payload.rels, &tx)
                .await?;

            self.to_paginated_items(items, has_more)
        })
        .await
    }

    pub async fn add_field(&self, field_name: &str, field_value: Value) -> Result<(), TitoError> {
        let table = self.get_table();

        let start_key = format!("{}:", table);
        let end_key = next_string_lexicographically(start_key.clone());

        let mut cursor = start_key.clone();

        self.tx(|tx| async move {
            loop {
                let scan_range = cursor.clone()..end_key.clone();
                let kvs = tx.scan(scan_range, 100).await.map_err(|_| {
                    TitoError::TransactionFailed(String::from("Failed migration, scan"))
                })?;

                let mut has_kvs = false;
                for kv in kvs {
                    has_kvs = true;
                    let key = String::from_utf8(kv.0.into()).unwrap();
                    let mut value: Value = serde_json::from_slice(&kv.1).unwrap();

                    value[field_name] = field_value.clone();

                    let model_instance =
                        serde_json::from_value::<T>(value.clone()).map_err(|_| {
                            TitoError::TransactionFailed(String::from("Failed migration, model"))
                        })?;

                    self.update_with_options(
                        model_instance,
                        TitoOptions::with_events(TitoOperation::Update),
                        &tx,
                    )
                    .await?;

                    cursor = next_string_lexicographically(key);
                }

                if !has_kvs {
                    break;
                }
            }

            Ok::<_, TitoError>(true)
        })
        .await;

        Ok(())
    }

    pub async fn remove_field(&self, field_name: &str) -> Result<(), TitoError> {
        let table = self.get_table();
        let start_key = format!("{}:", table);
        let end_key = next_string_lexicographically(start_key.clone());

        let mut cursor = start_key.clone();

        self.tx(|tx| async move {
            loop {
                let scan_range = cursor.clone()..end_key.clone();
                let kvs = tx.scan(scan_range, 100).await.map_err(|_| {
                    TitoError::TransactionFailed(String::from("Failed migration, scan"))
                })?;

                let mut has_kvs = false;
                for kv in kvs {
                    has_kvs = true;
                    let key = String::from_utf8(kv.0.into()).unwrap();

                    let mut value: Value = serde_json::from_slice(&kv.1).unwrap();

                    if value.as_object_mut().unwrap().remove(field_name).is_some() {
                        let model_instance =
                            serde_json::from_value::<T>(value.clone()).map_err(|_| {
                                TitoError::TransactionFailed(String::from(
                                    "Failed migration, model",
                                ))
                            })?;

                        self.update_with_options(
                            model_instance,
                            TitoOptions::with_events(TitoOperation::Update),
                            &tx,
                        )
                        .await?;
                    }

                    cursor = next_string_lexicographically(key);
                }

                if !has_kvs {
                    break;
                }
            }

            Ok::<_, TitoError>(true)
        })
        .await?;

        Ok(())
    }

    pub async fn find_all(&self) -> Result<TitoPaginated<T>, TitoError> {
        let table_name = self.get_table();
        let start_key = format!("{}:", table_name);
        let end_key = next_string_lexicographically(start_key.clone());

        self.tx(|tx| async move {
            let (items, has_more) = self
                .scan(
                    TitoScanPayload {
                        start: start_key,
                        end: Some(end_key),
                        limit: None,
                        cursor: None,
                    },
                    &tx,
                )
                .await?;

            let results: Vec<T> = items
                .iter()
                .map(|(_, value)| {
                    serde_json::from_value::<T>(value.clone()).map_err(|_| {
                        TitoError::DeserializationFailed("Failed to deserialize".to_string())
                    })
                })
                .collect::<Result<_, _>>()?;

            Ok(TitoPaginated::new(results, None))
        })
        .await
    }
}
