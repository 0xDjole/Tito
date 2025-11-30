use crate::{
    error::TitoError,
    key_encoder::safe_encode,
    types::{
        FieldValue, TitoEngine, TitoFindByIndexPayload, TitoFindOneByIndexPayload,
        TitoIndexBlockType, TitoModelTrait, TitoOperation, TitoOptions, TitoPaginated,
        TitoScanPayload, TitoTransaction,
    },
    utils::next_string_lexicographically,
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

impl<
        E: TitoEngine,
        T: Default
            + Clone
            + Serialize
            + DeserializeOwned
            + Unpin
            + std::marker::Send
            + Sync
            + TitoModelTrait,
    > TitoModel<E, T>
{
    pub fn get_index_keys(
        &self,
        id: String,
        value: &T,
        json: &Value,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut all_index_keys = vec![];

        for index_config in value.indexes().iter() {
            if !index_config.condition {
                continue;
            }

            let base_key = format!("index:{}:", index_config.name);

            let mut combinations: Vec<String> = vec![String::new()];

            for field in index_config.fields.iter() {
                let field_values = match self.get_nested_values(json, &field.name) {
                    Some(values) if !values.is_empty() => values,
                    _ => vec![FieldValue::Simple(Value::String("__null__".to_string()))],
                };

                let mut new_combinations = vec![];

                for field_value in field_values {
                    let field_str = match field_value {
                        // Handle HashMap entries with key-value pairs
                        FieldValue::HashMapEntry { key, value } => match field.r#type {
                            TitoIndexBlockType::String => match value.as_str() {
                                Some("") => Some(format!("{}:{}.__null__", field.name, key)),
                                Some(s) => {
                                    Some(format!("{}:{}.{}", field.name, key, safe_encode(&s)))
                                }
                                None => Some(format!("{}:{}.__null__", field.name, key)),
                            },
                            TitoIndexBlockType::Number => match value.as_i64() {
                                Some(n) => Some(format!("{}:{}:{:0>10}", field.name, key, n)),
                                None => Some(format!("{}:{}:__null__", field.name, key)),
                            },
                        },
                        // Handle simple values
                        FieldValue::Simple(value) => match field.r#type {
                            TitoIndexBlockType::String => match value.as_str() {
                                Some("") => Some(format!("{}:__null__", field.name)),
                                Some(s) => Some(format!("{}:{}", field.name, safe_encode(&s))),
                                None => Some(format!("{}:__null__", field.name)),
                            },
                            TitoIndexBlockType::Number => match value.as_i64() {
                                Some(n) => Some(format!("{}:{:0>10}", field.name, n)),
                                None => Some(format!("{}:__null__", field.name)),
                            },
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

    pub async fn find_by_index_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let indexes = self.model.indexes();

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
                TitoIndexBlockType::String => safe_encode(value),
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

    pub async fn find_by_index_reverse_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let indexes = self.model.indexes();
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
                TitoIndexBlockType::String => safe_encode(value),
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

    async fn find_by_index_with_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
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
                    exact_match: payload.exact_match,
                    limit: payload.limit,
                    cursor: payload.cursor,
                },
                tx,
            )
            .await?;

        let results = self.to_paginated_items(items, has_more)?;

        Ok(results)
    }

    pub async fn find_by_index(
        &self,
        payload: TitoFindByIndexPayload,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        match tx {
            Some(tx) => self.find_by_index_with_tx(payload, tx).await,
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move { self.find_by_index_with_tx(payload, &tx).await }
                })
                .await
            }
        }
    }

    async fn find_by_index_reverse_with_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (items, has_more) = self.find_by_index_reverse_raw(payload, tx).await?;

        let results = self.to_paginated_items(items, has_more)?;

        Ok(results)
    }

    pub async fn find_by_index_reverse(
        &self,
        payload: TitoFindByIndexPayload,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        match tx {
            Some(tx) => self.find_by_index_reverse_with_tx(payload, tx).await,
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move { self.find_by_index_reverse_with_tx(payload, &tx).await }
                })
                .await
            }
        }
    }

    async fn find_one_by_index_with_tx(
        &self,
        payload: TitoFindOneByIndexPayload,
        tx: &E::Transaction,
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
        tx: Option<&E::Transaction>,
    ) -> Result<T, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        match tx {
            Some(tx) => self.find_one_by_index_with_tx(payload, tx).await,
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move { self.find_one_by_index_with_tx(payload, &tx).await }
                })
                .await
            }
        }
    }

    pub async fn reindex(&self) -> Result<(), TitoError> {
        let table = self.get_table();
        let start_key = format!("{}:", table);
        let end_key = next_string_lexicographically(start_key.clone());

        self.tx(|tx| {
            let end_key = end_key.clone();
            let mut cursor = start_key.clone();
            async move {
                loop {
                    let scan_range = cursor.clone()..end_key.clone();
                    let kvs = tx.scan(scan_range, 100).await.map_err(|_| {
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
            }
        })
        .await?;

        Ok(())
    }
}
