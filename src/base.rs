use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::atomic::Ordering,
};

use crate::{
    query::IndexQueryBuilder,
    transaction::{TitoTransaction, TransactionManager},
    types::{
        DBUuid, ReverseIndex, TitoChangeLog, TitoConfigs, TitoCursor, TitoDatabase,
        TitoEmbeddedRelationshipConfig, TitoError, TitoFindByIndexPayload,
        TitoFindChangeLogSincePaylaod, TitoFindOneByIndexPayload, TitoFindPayload,
        TitoGenerateJobPayload, TitoIndexBlockType, TitoIndexConfig, TitoJob, TitoModelTrait,
        TitoPaginated, TitoScanPayload,
    },
    utils::{next_string_lexicographically, previous_string_lexicographically, to_snake_case},
};
use async_trait::async_trait;
use base64::{decode, encode};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use tikv_client::{Key, KvPair};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct TitoModel<T> {
    pub model: T,
    pub configs: TitoConfigs,
    pub transaction_manager: TransactionManager,
}

impl<
        T: Default
            + Clone
            + Serialize
            + DeserializeOwned
            + Unpin
            + std::marker::Send
            + Sync
            + TitoModelTrait,
    > TitoModel<T>
{
    pub fn new(configs: TitoConfigs, transaction_manager: TransactionManager) -> Self {
        Self {
            model: T::default(),
            configs,
            transaction_manager,
        }
    }

    fn get_id(&self) -> String {
        self.model.get_id()
    }

    fn get_model(&self) -> &T {
        &self.model
    }

    fn get_embedded_relationships(&self) -> Vec<TitoEmbeddedRelationshipConfig> {
        self.model.get_embedded_relationships()
    }

    fn get_indexes(&self) -> Vec<TitoIndexConfig> {
        self.model.get_indexes()
    }

    fn get_table(&self) -> String {
        self.model.get_table_name()
    }

    fn transaction_manager(&self) -> TransactionManager {
        self.transaction_manager.clone()
    }

    fn get_event_table(&self) -> Option<String> {
        self.model.get_event_table_name()
    }

    fn get_configs(&self) -> &TitoConfigs {
        &self.configs
    }

    pub fn query_by_index(&self, index: impl Into<String>) -> IndexQueryBuilder<T> {
        IndexQueryBuilder::new(self.clone(), index.into())
    }

    fn decode_cursor(&self, cursor: String) -> Result<TitoCursor, TitoError> {
        let cursor = decode(cursor).map_err(|err| TitoError::Failed)?;
        if let Ok(value) = serde_json::from_slice::<TitoCursor>(&cursor) {
            return Ok(value);
        }
        return Err(TitoError::Failed);
    }

    fn encode_cursors(&self, ids: Vec<Option<String>>) -> Result<String, TitoError> {
        let tikv_cursor = TitoCursor { ids };
        let json_bytes = serde_json::to_vec(&tikv_cursor).map_err(|_| TitoError::Failed)?;

        Ok(encode(&json_bytes))
    }

    async fn tx<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(TitoTransaction) -> Fut + Send,
        Fut: Future<Output = Result<R, E>> + Send,
        E: From<TitoError> + Send + Sync + std::fmt::Debug, // Added Sync trait bound
        R: Send,
    {
        self.transaction_manager().transaction(f).await
    }

    fn to_results(
        &self,
        items: impl Iterator<Item = KvPair>,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut results = vec![];
        for kv in items {
            let key_bytes: Vec<u8> = kv.0.into();
            let key = match String::from_utf8(key_bytes) {
                Ok(k) => k,
                Err(_) => {
                    continue;
                }
            };

            match serde_json::from_slice::<Value>(&kv.1) {
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
        tx: &TitoTransaction,
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
    pub async fn get_key(&self, key: &str, tx: &TitoTransaction) -> Result<Value, TitoError> {
        let result = tx
            .get(key.to_string())
            .await
            .map_err(|e| TitoError::NotFound(e.to_string()))?;

        let result = result.ok_or(TitoError::NotFound("Not found".to_string()))?;

        serde_json::from_slice::<Value>(&result)
            .map_err(|_| TitoError::NotFound("Not found".to_string()))
    }

    pub async fn put_change_log(
        &self,
        change_log: TitoChangeLog,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        let change_log_id = format!("change-log:{}:{}", change_log.created_at, change_log.id);

        let change_log_value = serde_json::to_value(&change_log)
            .map_err(|e| TitoError::FailedCreate(e.to_string()))?;

        self.put(change_log_id, change_log_value, tx).await?;

        Ok(true)
    }

    pub async fn find_changelog_since(
        &self,
        payload: TitoFindChangeLogSincePaylaod,
        tx: &TitoTransaction,
    ) -> Result<TitoPaginated<TitoChangeLog>, TitoError> {
        let start = format!("change-log:{}:", payload.timestamp);
        let end_key = format!("change-log:{}:", Utc::now().timestamp());

        let scan_payload = TitoScanPayload {
            start,
            end: Some(end_key),
            limit: payload.limit,
            cursor: payload.cursor,
        };

        let (changelog_entries, has_more) = self.scan(scan_payload, tx).await?;

        let mut results = vec![];

        let mut last_item: Option<String> = None;

        for item in changelog_entries.into_iter() {
            last_item = Some(item.0.clone());
            if let Ok(item) = serde_json::from_value::<TitoChangeLog>(item.1) {
                results.push(item);
            }
        }

        let cursor: Option<String> = last_item
            .and_then(|item| {
                Some(
                    self.encode_cursors(vec![Some(item)])
                        .expect("Failed to encode cursor"),
                )
            })
            .or(None);

        let results = TitoPaginated::new(results, cursor);

        Ok(results)
    }

    async fn put<P>(&self, key: String, payload: P, tx: &TitoTransaction) -> Result<bool, TitoError>
    where
        P: Serialize + Unpin + std::marker::Send + Sync,
    {
        let mut retries = 0;
        let mut delay = 10;
        let max_retries = 10;

        // Serialize the payload to serde_json::Value
        let mut value =
            serde_json::to_value(&payload).map_err(|e| TitoError::FailedCreate(e.to_string()))?;

        // Add the last_modified timestamp only if the value is an object
        if let serde_json::Value::Object(ref mut map) = value {
            let now = Utc::now().timestamp();
            map.insert("last_modified".to_string(), serde_json::json!(now));
        }

        loop {
            if self.get_configs().is_read_only.load(Ordering::SeqCst) {
                if retries >= max_retries {
                    return Err(TitoError::ReadOnlyMode);
                }

                sleep(Duration::from_millis(delay)).await;
                retries += 1;
                delay *= 2;
            }

            let bytes =
                serde_json::to_vec(&value).map_err(|e| TitoError::FailedCreate(e.to_string()))?;

            match tx.put(key.clone(), bytes).await {
                Ok(()) => return Ok(true),
                Err(e) => {
                    if retries >= max_retries {
                        return Err(TitoError::FailedCreate(e.to_string()));
                    }

                    sleep(Duration::from_millis(delay)).await;
                    retries += 1;
                    delay *= 2;
                }
            }
        }
    }

    pub async fn delete(&self, key: String, tx: &TitoTransaction) -> Result<bool, TitoError> {
        let mut retries = 0;
        let mut delay = 10;
        let max_retries = 10;

        loop {
            if self.get_configs().is_read_only.load(Ordering::SeqCst) {
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
                        return Err(TitoError::FailedDelete(e.to_string()));
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

    fn to_paginated_items(
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
        tx: &TitoTransaction,
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
    fn get_nested_values(&self, json: &Value, field_path: &str) -> Option<Vec<Value>> {
        let mut results = Vec::new();
        let mut to_process = vec![(json.clone(), 0)];
        let parts: Vec<&str> = field_path.split('.').collect();

        while let Some((current_value, depth)) = to_process.pop() {
            if depth == parts.len() {
                results.push(current_value);
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

    fn get_index_keys(
        &self,
        id: String,
        value: &T,
        json: &Value,
    ) -> Result<Vec<(String, Value)>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let mut all_index_keys = vec![];

        for index_config in value.get_indexes().iter() {
            if !index_config.condition {
                continue;
            }

            let base_key = format!("index:{}:", index_config.name);

            let mut combinations: Vec<String> = vec![String::new()];

            if let Some(generator) = &index_config.custom_generator {
                let custom_keys = generator()?;
                for key in custom_keys {
                    let key = format!("{}{}:{}", base_key, key, id);
                    if !key.is_empty() {
                        all_index_keys.push((key, json.clone()));
                    }
                }
            }

            for field in index_config.fields.iter() {
                let field_values = match self.get_nested_values(json, &field.name) {
                    Some(values) if !values.is_empty() => values,
                    _ => {
                        // Handle null/empty case
                        vec![Value::String("__null__".to_string())]
                    }
                };

                let mut new_combinations = vec![];

                for value in field_values {
                    let field_str = match field.r#type {
                        TitoIndexBlockType::String => match value.as_str() {
                            Some("") => Some(format!("{}:__null__", field.name)),
                            Some(s) => Some(format!("{}:{}", field.name, to_snake_case(s))),
                            None => Some(format!("{}:__null__", field.name)),
                        },
                        TitoIndexBlockType::Number => match value.as_i64() {
                            Some(n) => Some(format!("{}:{:0>10}", field.name, n)),
                            None => Some(format!("{}:__null__", field.name)),
                        },
                    };

                    if let Some(field_str) = field_str {
                        for existing_combo in &combinations {
                            new_combinations.push(format!(
                                "{}{}{}",
                                existing_combo,
                                if existing_combo.is_empty() { "" } else { ":" },
                                field_str
                            ));
                        }
                    }
                }

                if !new_combinations.is_empty() {
                    combinations = new_combinations;
                }
            }

            for combo in combinations {
                if !combo.is_empty() {
                    all_index_keys.push((format!("{}{}:{}", base_key, combo, id), json.clone()));
                }
            }
        }

        Ok(all_index_keys)
    }

    pub async fn build(&self, payload: T, tx: &TitoTransaction) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.build_with_options(payload, Some(String::from("CREATE")), tx)
            .await
    }

    pub async fn build_with_options(
        &self,
        payload: T,
        event_action: Option<String>,
        tx: &TitoTransaction,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let value = serde_json::to_value(&payload).unwrap();

        let raw_id = payload.get_id();

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

        let change_log = TitoChangeLog {
            id: DBUuid::new_v4().to_string(),
            record_id: id.clone(),
            operation: String::from("put"),
            created_at: Utc::now().timestamp(),
            data: Some(value),
            indexes: all_index_keys,
        };

        self.put_change_log(change_log, tx).await?;

        self.generate_job(
            TitoGenerateJobPayload {
                id: raw_id.clone(),
                action: event_action,
                clear_future: false,
                scheduled_for: None,
            },
            tx,
        )
        .await?;

        Ok(payload)
    }

    async fn generate_job(
        &self,
        payload: TitoGenerateJobPayload,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        if let Some(raw_action) = payload.action.clone() {
            if let Some(value) = self.get_event_table() {
                self.lock_keys(vec![payload.id.clone()], tx).await?;
                let action = format!("{}_{}", raw_action, self.get_table().to_string());
                let created_at = Utc::now().timestamp();

                let scheduled_for = if let Some(scheduled_for) = payload.scheduled_for {
                    scheduled_for
                } else {
                    created_at
                };

                let message = value.to_string();
                let status = String::from("PENDING");
                let id = payload.id.clone();
                let uuid_str = DBUuid::new_v4().to_string();

                if payload.clear_future {
                    let future_events_start =
                        format!("{}_by_owner:{}:{}:{}", value, id, action, created_at);
                    let future_events_end =
                        format!("{}_by_owner:{}:{}:{}", value, id, action, u32::MAX);

                    self.tx(|inner_tx| async move {
                        let scan_stream = inner_tx
                            .scan(future_events_start..future_events_end, 1000)
                            .await
                            .map_err(|e| TitoError::NotFound(e.to_string()))?;

                        let mut future_events = Vec::new();

                        for item in scan_stream {
                            if let Ok(key_by_entity) = serde_json::from_slice::<String>(&item.1) {
                                let key_bytes: Vec<u8> = item.0.into();
                                let key = match String::from_utf8(key_bytes) {
                                    Ok(k) => k,
                                    Err(_) => continue,
                                };

                                future_events.push(key_by_entity.clone());
                                future_events.push(key.clone());

                                let change_log = TitoChangeLog {
                                    id: DBUuid::new_v4().to_string(),
                                    record_id: key.clone(),
                                    operation: String::from("delete"),
                                    created_at: Utc::now().timestamp(),
                                    data: None,
                                    indexes: vec![key_by_entity],
                                };

                                self.put_change_log(change_log, &inner_tx).await?;
                            }
                        }

                        for key in future_events.iter() {
                            self.delete(key.clone(), &inner_tx).await?;
                        }

                        Ok::<_, TitoError>(())
                    })
                    .await?;
                }

                let key = format!("{}:{}:{}:{}:{}", value, status, scheduled_for, id, uuid_str);
                let key_by_entity = format!(
                    "{}_by_owner:{}:{}:{}:{}",
                    value, id, action, scheduled_for, uuid_str
                );

                let job = TitoJob {
                    id: uuid_str,
                    key: key.clone(),
                    entity_id: id.clone(),
                    group_id: id.clone(),
                    action: action.clone(),
                    status,
                    message,
                    retries: 0,
                    max_retries: 5,
                    scheduled_for,
                    created_at: created_at,
                    updated_at: created_at,
                };

                self.put(key.clone(), &job, tx).await?;
                self.put(key_by_entity.clone(), &key, tx).await?;

                let value = serde_json::to_value(&job).ok();

                let id = DBUuid::new_v4().to_string();

                let change_log = TitoChangeLog {
                    id,
                    record_id: key.clone(),
                    operation: String::from("put"),
                    created_at: created_at,
                    data: value,
                    indexes: vec![key_by_entity],
                };

                self.put_change_log(change_log, tx).await?;
            }
        }

        Ok(true)
    }

    pub fn stitch_relationship(
        &self,
        item: &mut Value,
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        let source_parts: Vec<&str> = config.source_field_name.split('.').collect();
        let dest_parts: Vec<&str> = config.destination_field_name.split('.').collect();

        Self::_stitch_recursive_helper(item, &source_parts, &dest_parts, rel_map, config);
    }

    fn _stitch_recursive_helper(
        current_json_node: &mut Value,
        source_path_remaining: &[&str],
        dest_path_remaining: &[&str],
        rel_map: &HashMap<String, Value>,
        config: &TitoEmbeddedRelationshipConfig,
    ) {
        if source_path_remaining.len() == 1 && dest_path_remaining.len() == 1 {
            let source_key = source_path_remaining[0];
            let dest_key = dest_path_remaining[0];

            if let Some(obj_to_modify) = current_json_node.as_object_mut() {
                if let Some(id_val_at_source_key) = obj_to_modify.get(source_key) {
                    if let Some(id_str) = id_val_at_source_key.as_str() {
                        let rel_lookup_key = format!("{}:{}", config.model, id_str);
                        if let Some(related_data) = rel_map.get(&rel_lookup_key) {
                            obj_to_modify.insert(dest_key.to_string(), related_data.clone());
                        }
                    } else if let Some(ids_array) = id_val_at_source_key.as_array() {
                        // Source is an array of ID strings
                        let mut stitched_related_items_array = Vec::new();
                        for id_elem_in_array in ids_array {
                            if let Some(id_str_elem) = id_elem_in_array.as_str() {
                                let rel_lookup_key = format!("{}:{}", config.model, id_str_elem);
                                if let Some(related_data) = rel_map.get(&rel_lookup_key) {
                                    stitched_related_items_array.push(related_data.clone());
                                }
                            }
                        }

                        obj_to_modify.insert(
                            dest_key.to_string(),
                            Value::Array(stitched_related_items_array),
                        );
                    }
                }
            }
            return;
        }

        if !source_path_remaining.is_empty()
            && !dest_path_remaining.is_empty()
            && source_path_remaining[0] == dest_path_remaining[0]
        {
            let common_key = source_path_remaining[0];
            if let Some(next_json_node_candidate) = current_json_node.get_mut(common_key) {
                match next_json_node_candidate {
                    Value::Object(obj) => {
                        Self::_stitch_recursive_helper(
                            next_json_node_candidate, // obj is implicitly &mut Value here
                            &source_path_remaining[1..],
                            &dest_path_remaining[1..],
                            rel_map,
                            config,
                        );
                    }
                    Value::Array(arr) => {
                        for element_in_array in arr.iter_mut() {
                            Self::_stitch_recursive_helper(
                                element_in_array,
                                &source_path_remaining[1..],
                                &dest_path_remaining[1..],
                                rel_map,
                                config,
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn get_relationship_data(
        &self,
        items: &Vec<(String, Value)>,
        rels_config: &[TitoEmbeddedRelationshipConfig],
        rels: &Vec<String>, // List of destination_field_names to populate
    ) -> Vec<(TitoEmbeddedRelationshipConfig, String)> {
        // (Config for this rel, "model_name:id_found")
        let mut relationship_keys_to_fetch = Vec::new();

        for (_, item_value) in items {
            for config in rels_config {
                // Check if this relationship is requested
                if rels.contains(&config.destination_field_name) {
                    // Use get_nested_values to find all ID values at the source path
                    if let Some(found_values_at_source_path) =
                        self.get_nested_values(item_value, &config.source_field_name)
                    {
                        for value_or_array_of_values in found_values_at_source_path {
                            // The value_or_array_of_values could be a single ID (Value::String)
                            // or an array of IDs (Value::Array of Value::String)
                            if let Some(id_str) = value_or_array_of_values.as_str() {
                                if id_str != "__null__" {
                                    // Assuming __null__ is a skip marker
                                    relationship_keys_to_fetch.push((
                                        config.clone(),
                                        format!("{}:{}", config.model, id_str),
                                    ));
                                }
                            } else if let Some(id_array) = value_or_array_of_values.as_array() {
                                for id_element in id_array {
                                    if let Some(id_str) = id_element.as_str() {
                                        if id_str != "__null__" {
                                            relationship_keys_to_fetch.push((
                                                config.clone(),
                                                format!("{}:{}", config.model, id_str),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        relationship_keys_to_fetch
    }
    async fn fetch_and_stitch_relationships(
        &self,
        items: Vec<(String, Value)>,
        rels: Vec<String>,
        tx: &TitoTransaction,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        if rels.is_empty() {
            return Ok(items);
        }

        let rels_config = self.get_embedded_relationships();
        let relationship_data = self.get_relationship_data(&items, &rels_config, &rels);

        let rel_keys: Vec<String> = relationship_data.into_iter().map(|item| item.1).collect();

        let rel_items = self.batch_get(rel_keys.clone(), 10, 2, tx).await?;

        let mut rel_map: HashMap<String, Value> = HashMap::new();
        for kv in rel_items {
            rel_map.insert(kv.0, kv.1);
        }

        let final_items = items
            .into_iter()
            .map(|mut item| {
                for config in &rels_config {
                    if rels.contains(&config.destination_field_name) {
                        self.stitch_relationship(&mut item.1, &rel_map, config);
                    }
                }
                item
            })
            .collect();

        Ok(final_items)
    }

    pub async fn find_by_id_tx(
        &self,
        id: &str,
        rels: Vec<String>,
        tx: &TitoTransaction,
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

    pub async fn find_by_index_reverse(
        &self,
        payload: TitoFindByIndexPayload,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.tx(|tx| async move { self.find_by_index_reverse_tx(payload, &tx).await })
            .await
    }

    pub async fn find_by_index(
        &self,
        payload: TitoFindByIndexPayload,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.tx(|tx| async move { self.find_by_index_tx(payload, &tx).await })
            .await
    }

    pub async fn find_by_index_reverse_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &TitoTransaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let indexes = self.get_indexes();
        let index = indexes
            .iter()
            .find(|index_config| index_config.name == payload.index)
            .unwrap();

        let index_fields = index.fields.clone();

        let mut key_from_values = vec![];
        for (i, value) in payload.values.iter().enumerate() {
            let index_field = index_fields[i].clone();
            let index_field_type = index_field.r#type;

            let value = match index_field_type {
                TitoIndexBlockType::String => to_snake_case(value),
                TitoIndexBlockType::Number => format!("{:0>10}", value),
            };

            let field_name = index_field.name.clone();
            let key_part = format!("{}:{}", field_name, value);
            key_from_values.push(key_part);
        }

        let key_from_value = key_from_values.join(":");

        let id = format!("index:{}:{}", payload.index, key_from_value);

        let (items, has_more) = self
            .scan_reverse(
                TitoScanPayload {
                    start: id,
                    end: payload.end.clone(),
                    limit: payload.limit,
                    cursor: payload.cursor.clone(),
                },
                tx,
            )
            .await?;

        let items = self
            .fetch_and_stitch_relationships(items, payload.rels, tx)
            .await?;

        Ok((items, has_more))
    }

    pub async fn find_by_index_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &TitoTransaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let indexes = self.get_indexes();

        let index = indexes
            .iter()
            .find(|index_config| index_config.name == payload.index)
            .unwrap();

        let index_fields = index.fields.clone();

        let mut key_from_values = vec![];
        for (i, value) in payload.values.iter().enumerate() {
            let index_field = index_fields[i].clone();
            let index_field_type = index_field.r#type;

            let value = match index_field_type {
                TitoIndexBlockType::String => to_snake_case(value),
                TitoIndexBlockType::Number => format!("{:0>10}", value),
            };

            let field_name = index_field.name.clone();
            let key_part = format!("{}:{}", field_name, value);
            key_from_values.push(key_part);
        }

        let key_from_value = key_from_values.join(":");

        let mut id = format!("index:{}:{}", payload.index, key_from_value);

        if payload.exact_match {
            id.push_str(":");
        }

        let end = if let Some(end) = payload.end {
            if let Some(last_colon_index) = id.rfind(':') {
                Some(format!("{}:{}", &id[..last_colon_index], end))
            } else {
                None
            }
        } else {
            None
        };

        let (items, has_more) = self
            .scan(
                TitoScanPayload {
                    start: id,
                    end,
                    limit: payload.limit,
                    cursor: payload.cursor.clone(),
                },
                tx,
            )
            .await?;

        let items = self
            .fetch_and_stitch_relationships(items, payload.rels, tx)
            .await?;

        Ok((items, has_more))
    }

    pub async fn scan(
        &self,
        payload: TitoScanPayload,
        tx: &TitoTransaction,
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

    pub async fn find_by_index_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &TitoTransaction,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (items, has_more) = self
            .find_by_index_raw(
                TitoFindByIndexPayload {
                    index: payload.index,
                    values: payload.values,
                    rels: payload.rels,
                    end: payload.end,
                    exact_match: true,
                    limit: payload.limit,
                    cursor: payload.cursor,
                },
                tx,
            )
            .await?;

        let results = self.to_paginated_items(items, has_more)?;

        Ok(results)
    }

    pub async fn find_by_index_reverse_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &TitoTransaction,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let cursor = payload.cursor.clone();
        let (items, has_more) = self.find_by_index_reverse_raw(payload, tx).await?;

        let results = self.to_paginated_items(items, has_more)?;

        Ok(results)
    }

    pub fn deduplicate_and_track_last_valid(
        &self,
        items: &[(String, Value)],
        origins: &[usize],
        limit: Option<u32>,
    ) -> (Vec<(String, Value)>, HashMap<usize, String>) {
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        let mut query_cursors = HashMap::new();
        let limit = limit.map(|l| l as usize); // Convert Option<u32> to Option<usize> for easier comparison

        for (i, item) in items.iter().enumerate() {
            let origin = origins[i];
            let id = item.1.get("id");

            if let Some(id) = id {
                if let Some(id) = id.as_str() {
                    if seen.insert(id) {
                        if limit.map_or(true, |lim| result.len() < lim) {
                            result.push(item.clone());
                            query_cursors.insert(origin, item.0.clone());
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        (result, query_cursors)
    }

    pub async fn find_one_by_index_tx(
        &self,
        payload: TitoFindOneByIndexPayload,
        tx: &TitoTransaction,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (items, _) = self
            .find_by_index_raw(
                TitoFindByIndexPayload {
                    index: payload.index.clone(),
                    values: payload.values.clone(),
                    rels: payload.rels.clone(),
                    end: None,
                    exact_match: true,
                    limit: Some(1),
                    cursor: None,
                },
                tx,
            )
            .await?;

        if let Some(value) = items.get(0) {
            match serde_json::from_value::<T>(value.1.clone()) {
                Ok(item) => Ok(item),
                Err(e) => Err(TitoError::NotFound(format!(
                    "Failed to deserialize record for index '{}' with values {:?}: {}",
                    payload.index, payload.values, e
                ))),
            }
        } else {
            Err(TitoError::NotFound(format!(
                "No record found for index '{}' with values {:?}",
                payload.index, payload.values
            )))
        }
    }

    pub async fn find_one_by_index(
        &self,
        payload: TitoFindOneByIndexPayload,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.tx(|tx| async move { self.find_one_by_index_tx(payload, &tx).await })
            .await
    }
    pub async fn find_by_ids_tx(
        &self,
        ids: Vec<String>,
        rels: Vec<String>,
        tx: &TitoTransaction,
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
        tx: &TitoTransaction,
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
        tx: &TitoTransaction,
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

    pub async fn update(&self, payload: T, tx: &TitoTransaction) -> Result<bool, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.update_with_options(payload, true, tx).await
    }

    pub fn get_last_id(&self, key: String) -> Option<String> {
        let parts: Vec<&str> = key.split(':').collect();
        parts.last().map(|last| last.to_string())
    }

    pub fn extract_relationship(&self, input: &str) -> Option<String> {
        if let (Some(start), Some(end)) = (input.find("-").map(|idx| idx + 1), input.rfind("-")) {
            if start < end {
                return Some(input[start..end].trim().to_string());
            }
        }

        None
    }

    pub async fn update_with_options(
        &self,
        payload: T,
        trigger_event: bool,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let raw_id = payload.get_id();

        let trigger_event = trigger_event.then_some(String::from("UPDATE"));

        let deleted = self.delete_by_id_with_options(&raw_id, false, tx).await;

        self.build_with_options(payload, trigger_event, tx).await?;

        Ok(true)
    }

    pub async fn lock_keys(
        &self,
        keys: Vec<String>,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        let keys: Vec<Key> = keys
            .into_iter()
            .map(|key| Key::from(format!("lock:{}", key).into_bytes()))
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
        tx: &TitoTransaction,
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
        trigger_event: bool,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        let id = format!("{}:{}", self.get_table(), raw_id);
        let reverse_index_key = format!("reverse-index:{}", id);

        let reverse_index = self.get_reverse_index(&reverse_index_key, tx).await?;
        let mut keys = reverse_index.value;

        let change_log = TitoChangeLog {
            id: DBUuid::new_v4().to_string(),
            record_id: id.clone(),
            operation: String::from("delete"),
            created_at: Utc::now().timestamp(),
            data: None,
            indexes: keys.clone(),
        };

        self.put_change_log(change_log, tx).await?;

        keys.push(id.clone());

        keys.push(reverse_index_key);

        for key in keys.into_iter() {
            self.delete(key, tx).await?;
        }

        self.generate_job(
            TitoGenerateJobPayload {
                id: raw_id.to_string(),
                action: trigger_event.then(|| String::from("DELETE")),
                clear_future: false,
                scheduled_for: None,
            },
            tx,
        )
        .await?;

        Ok(true)
    }

    pub async fn delete_by_id(
        &self,
        raw_id: &str,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        self.delete_by_id_with_options(raw_id, true, tx).await
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

                    self.update_with_options(model_instance, false, &tx).await?;

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

                        self.update_with_options(model_instance, false, &tx).await?;
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

    pub async fn reindex(&self) -> Result<(), TitoError> {
        let table = self.get_table();
        let start_key = format!("{}:", table);
        let end_key = next_string_lexicographically(start_key.clone());

        let mut cursor = start_key.clone();

        self.tx(|tx| async move {
            loop {
                let scan_range = cursor.clone()..end_key.clone();
                let kvs = tx.scan(scan_range, 100).await.map_err(|e| {
                    TitoError::TransactionFailed(String::from("Failed migration, scan"))
                })?;

                let mut has_kvs = false;
                for kv in kvs {
                    has_kvs = true;
                    let key = String::from_utf8(kv.0.into()).unwrap();
                    let value: Value = serde_json::from_slice(&kv.1).unwrap();

                    let model_instance =
                        serde_json::from_value::<T>(value.clone()).map_err(|_| {
                            TitoError::TransactionFailed(String::from("Failed migration, model"))
                        })?;

                    self.update_with_options(model_instance, false, &tx).await?;

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

    pub async fn apply_change_logs(
        &self,
        change_logs: Vec<TitoChangeLog>,
        tx: &TitoTransaction,
    ) -> Result<bool, TitoError> {
        for log in change_logs {
            self.put_change_log(log.clone(), tx).await?;

            match log.operation.as_str() {
                "put" => {
                    if let Some(data) = &log.data {
                        let key = log.record_id.clone();

                        self.put(key.clone(), data.clone(), tx).await?;

                        for index_key in &log.indexes {
                            self.put(index_key.clone(), data.clone(), tx).await?;
                        }

                        let reverse_index = ReverseIndex {
                            value: log.indexes.clone(),
                        };
                        let reverse_key = format!("reverse-index:{}", key);
                        self.put(reverse_key, reverse_index, tx).await?;
                    } else {
                        return Err(TitoError::FailedCreate(
                            "Missing data for put operation".to_string(),
                        ));
                    }
                }
                "delete" => {
                    let key = log.record_id.clone();

                    self.delete(key.clone(), tx).await?;

                    for index_key in &log.indexes {
                        self.delete(index_key.clone(), tx).await?;
                    }

                    let reverse_key = format!("reverse-index:{}", key);
                    self.delete(reverse_key, tx).await?;
                }

                _ => {
                    return Err(TitoError::FailedDelete(format!(
                        "Unsupported operation '{}'",
                        log.operation
                    )));
                }
            }
        }
        Ok(true)
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
                        TitoError::DeserializationError("Failed to deserialize".to_string())
                    })
                })
                .collect::<Result<_, _>>()?;

            Ok(TitoPaginated::new(results, None))
        })
        .await
    }
}
