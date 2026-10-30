use crate::{
    error::TitoError,
    key_encoder::{encode_index_integer_query, encode_index_number, encode_index_string},
    types::{
        FieldValue, TitoEngine, TitoFindByIndexPayload, TitoFindOneByIndexPayload, TitoGuard,
        TitoIndexConfig, TitoIndexFieldType, TitoIndexValue, TitoKvPair, TitoModelTrait,
        TitoPaginated, TitoScanPayload, TitoTransaction,
    },
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

enum ScannedRow {
    Value(Value),
    Record(String),
}

fn after_segment(value: &str) -> Option<&str> {
    let mut escaped = false;
    for (position, character) in value.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ':' {
            return Some(&value[position + 1..]);
        }
    }
    None
}

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
    fn build_index_keys(
        &self,
        id: &str,
        indexes: &[crate::types::TitoIndexConfig],
        json: &Value,
        unique: bool,
    ) -> Result<Vec<String>, TitoError> {
        let mut all_index_keys = vec![];

        for index_config in indexes {
            if !index_config.condition {
                continue;
            }

            let base_key = if unique {
                format!("unique-index:{}:{}:", T::table(), index_config.name)
            } else {
                format!("index:{}:", index_config.name)
            };

            let mut combinations: Vec<String> = vec![String::new()];

            for field in &index_config.fields {
                let field_values = match &field.r#type {
                    TitoIndexFieldType::CustomString(value) => {
                        vec![FieldValue::Simple(Value::String(value.clone()))]
                    }
                    _ => self
                        .get_nested_values(json, &field.name)
                        .unwrap_or_default(),
                };

                let mut new_combinations = vec![];

                for field_value in field_values {
                    let field_str = match field_value {
                        FieldValue::HashMapEntry { key, value } => match &field.r#type {
                            TitoIndexFieldType::String | TitoIndexFieldType::CustomString(_) => {
                                value
                                    .as_str()
                                    .filter(|value| !value.is_empty())
                                    .map(|value| {
                                        format!(
                                            "{}:{}",
                                            field.name,
                                            encode_index_string(&format!("{key}.{value}"))
                                        )
                                    })
                            }
                            TitoIndexFieldType::Number => {
                                encode_index_number(&value)?.map(|value| {
                                    format!("{}:{}:{value}", field.name, encode_index_string(&key))
                                })
                            }
                        },
                        FieldValue::Simple(value) => match &field.r#type {
                            TitoIndexFieldType::String | TitoIndexFieldType::CustomString(_) => {
                                value
                                    .as_str()
                                    .filter(|value| !value.is_empty())
                                    .map(|value| {
                                        format!("{}:{}", field.name, encode_index_string(value))
                                    })
                            }
                            TitoIndexFieldType::Number => encode_index_number(&value)?
                                .map(|value| format!("{}:{value}", field.name)),
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

                if new_combinations.is_empty() {
                    combinations.clear();
                    break;
                }

                combinations = new_combinations;
            }

            for combo in combinations {
                if !combo.is_empty() {
                    let key = if unique {
                        format!("{base_key}{combo}")
                    } else {
                        format!("{base_key}{combo}:{id}")
                    };
                    all_index_keys.push(key);
                }
            }
        }

        Ok(all_index_keys)
    }

    pub fn get_index_keys(
        &self,
        id: String,
        value: &T,
        json: &Value,
    ) -> Result<Vec<(String, Vec<u8>)>, TitoError> {
        let owner = value.id().into_bytes();
        let indexes = value.indexes();
        let unique_indexes = value.unique_indexes();
        let record_copy = if indexes
            .iter()
            .chain(&unique_indexes)
            .any(|index| index.condition && index.value == TitoIndexValue::RecordCopy)
        {
            serde_json::to_vec(json)
                .map_err(|error| TitoError::SerializationFailed(error.to_string()))?
        } else {
            Vec::new()
        };
        let mut all_index_keys = Vec::new();
        let mut seen = HashMap::new();
        for (unique, indexes) in [(false, indexes), (true, unique_indexes)] {
            for index in indexes {
                for key in self.build_index_keys(&id, std::slice::from_ref(&index), json, unique)? {
                    if let Some(previous) = seen.insert(key.clone(), index.value) {
                        if previous != index.value {
                            return Err(TitoError::IndexError(format!(
                                "Index key '{key}' declares conflicting value modes"
                            )));
                        }
                        continue;
                    }
                    let bytes = match index.value {
                        TitoIndexValue::RecordCopy => record_copy.clone(),
                        TitoIndexValue::RecordReference if unique => owner.clone(),
                        TitoIndexValue::RecordReference => Vec::new(),
                    };
                    all_index_keys.push((key, bytes));
                }
            }
        }
        Ok(all_index_keys)
    }

    fn unique_index_value(key: &str) -> Result<TitoIndexValue, TitoError> {
        let indexes = T::default().unique_indexes();
        let mut matches = indexes.iter().filter(|index| {
            key.starts_with(&format!("unique-index:{}:{}:", T::table(), index.name))
        });
        let index = matches.next().ok_or_else(|| {
            TitoError::IndexError(format!(
                "Index key '{key}' is not a unique entry of model '{}'",
                T::table()
            ))
        })?;
        if matches.any(|other| other.name != index.name || other.value != index.value) {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' has conflicting declarations on model '{}'",
                T::table()
            )));
        }
        Ok(index.value)
    }

    pub(crate) fn unique_owner_id(key: &str, bytes: Vec<u8>) -> Result<String, TitoError> {
        match Self::unique_index_value(key)? {
            TitoIndexValue::RecordReference => String::from_utf8(bytes).map_err(|_| {
                TitoError::DeserializationFailed(format!(
                    "Unique index key '{key}' on model '{}' holds an owner id that is not UTF-8",
                    T::table()
                ))
            }),
            TitoIndexValue::RecordCopy => serde_json::from_slice::<T>(&bytes)
                .map(|record| record.id())
                .map_err(|error| {
                    TitoError::DeserializationFailed(format!(
                        "Failed to deserialize copied owner for unique index key '{key}': {error}"
                    ))
                }),
        }
    }

    fn index_entry_id(index: &TitoIndexConfig, key: &str) -> Option<String> {
        let mut rest = key
            .strip_prefix("index:")?
            .strip_prefix(index.name.as_str())?
            .strip_prefix(':')?;
        for (position, field) in index.fields.iter().enumerate() {
            rest = after_segment(rest.strip_prefix(field.name.as_str())?.strip_prefix(':')?)?;
            if matches!(field.r#type, TitoIndexFieldType::Number) {
                let next = index
                    .fields
                    .get(position + 1)
                    .map_or_else(T::key_prefix, |next| next.name.clone());
                if !rest
                    .strip_prefix(next.as_str())
                    .is_some_and(|after| after.starts_with(':'))
                {
                    rest = after_segment(rest)?;
                }
            }
        }
        rest.strip_prefix(T::key_prefix().as_str())?
            .strip_prefix(':')
            .map(str::to_string)
    }

    fn index_entry_owner(
        indexes: &mut Option<Vec<TitoIndexConfig>>,
        key: &str,
    ) -> Result<(String, TitoIndexValue), TitoError> {
        let indexes = indexes.get_or_insert_with(|| T::default().indexes());
        let mut owners = indexes
            .iter()
            .filter_map(|index| Self::index_entry_id(index, key).map(|id| (id, index.value)));
        let owner = owners.next().ok_or_else(|| {
            TitoError::IndexError(format!(
                "Index key '{key}' is not an entry of model '{}'",
                T::table()
            ))
        })?;
        if owners.any(|other| other != owner) {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' matches more than one index of model '{}'",
                T::table()
            )));
        }
        Ok(owner)
    }

    fn copied_index_record(
        &self,
        key: &str,
        bytes: &[u8],
        unique: bool,
    ) -> Result<(T, Value), TitoError> {
        let json = Self::scanned_json(key, bytes)?;
        let record: T = serde_json::from_value(json.clone()).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize copied record for index key '{key}': {error}"
            ))
        })?;
        let primary = format!("{}:{}", self.get_table(), record.id());
        let indexes = if unique {
            record.unique_indexes()
        } else {
            record.indexes()
        };
        if !self
            .build_index_keys(&primary, &indexes, &json, unique)?
            .iter()
            .any(|owned| owned == key)
        {
            return Err(TitoError::IndexError(format!(
                "Copied record for index key '{key}' disagrees with its key"
            )));
        }
        Ok((record, json))
    }

    pub(crate) async fn scanned_values(
        &self,
        rows: Vec<TitoKvPair>,
        tx: &E::Transaction,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut indexes = None;
        let mut scanned = Vec::with_capacity(rows.len());
        let mut owners = Vec::new();
        let mut requested = HashSet::new();
        for (key, value) in rows {
            let key = Self::scanned_key(key)?;
            let row = if key.starts_with("unique-index:") {
                match Self::unique_index_value(&key)? {
                    TitoIndexValue::RecordCopy => {
                        ScannedRow::Value(self.copied_index_record(&key, &value, true)?.1)
                    }
                    TitoIndexValue::RecordReference => ScannedRow::Record(format!(
                        "{}:{}",
                        self.get_table(),
                        Self::unique_owner_id(&key, value)?
                    )),
                }
            } else if key.starts_with("index:") {
                let (owner, mode) = Self::index_entry_owner(&mut indexes, &key)?;
                match mode {
                    TitoIndexValue::RecordCopy => {
                        ScannedRow::Value(self.copied_index_record(&key, &value, false)?.1)
                    }
                    TitoIndexValue::RecordReference => {
                        if !value.is_empty() {
                            return Err(TitoError::IndexError(format!(
                                "Reference index key '{key}' has a non-empty value"
                            )));
                        }
                        ScannedRow::Record(format!("{}:{owner}", self.get_table()))
                    }
                }
            } else {
                ScannedRow::Value(Self::scanned_json(&key, &value)?)
            };
            if let ScannedRow::Record(primary) = &row {
                if requested.insert(primary.clone()) {
                    owners.push(primary.clone());
                }
            }
            scanned.push((key, row));
        }
        let mut records = HashMap::new();
        if !owners.is_empty() {
            records.extend(self.batch_get(owners, tx).await?);
        }
        scanned
            .into_iter()
            .map(|(key, row)| match row {
                ScannedRow::Value(value) => Ok((key, value)),
                ScannedRow::Record(primary) => match records.get(&primary) {
                    Some(record) => Ok((key, record.clone())),
                    None => Err(TitoError::IndexError(format!(
                        "Index key '{key}' names the missing record '{primary}'"
                    ))),
                },
            })
            .collect()
    }

    fn unique_index_key(&self, payload: &TitoFindOneByIndexPayload) -> Result<String, TitoError> {
        let schema = T::default();
        let indexes = schema.unique_indexes();
        let index = indexes
            .iter()
            .find(|index| index.name == payload.index)
            .ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Unique index '{}' not found on model '{}'",
                    payload.index,
                    T::table()
                ))
            })?;

        let fields = Self::exact_index_fields(index, &payload.values)?;
        Ok(format!(
            "unique-index:{}:{}:{}",
            T::table(),
            payload.index,
            fields
        ))
    }

    fn exact_index_fields(index: &TitoIndexConfig, values: &[String]) -> Result<String, TitoError> {
        if values.len() != index.fields.len() || values.is_empty() {
            return Err(TitoError::IndexError(format!(
                "Index '{}' has {} fields but {} values were provided",
                index.name,
                index.fields.len(),
                values.len()
            )));
        }

        let mut parts = Vec::with_capacity(index.fields.len());
        for (field, value) in index.fields.iter().zip(values) {
            let encoded = match field.r#type {
                TitoIndexFieldType::String | TitoIndexFieldType::CustomString(_) => {
                    encode_index_string(value)
                }
                TitoIndexFieldType::Number => encode_index_integer_query(value)?,
            };
            parts.push(format!("{}:{encoded}", field.name));
        }

        Ok(parts.join(":"))
    }

    pub async fn find_one_by_unique_index(
        &self,
        payload: TitoFindOneByIndexPayload,
        tx: Option<&E::Transaction>,
    ) -> Result<T, TitoError> {
        match tx {
            Some(tx) => self.find_one_by_unique_index_with_tx(&payload, tx).await,
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move { self.find_one_by_unique_index_with_tx(&payload, &tx).await }
                })
                .await
            }
        }
    }

    async fn find_one_by_unique_index_with_tx(
        &self,
        payload: &TitoFindOneByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<T, TitoError> {
        let key = self.unique_index_key(payload)?;
        let owner_bytes = tx.get(&key).await?.ok_or_else(|| {
            TitoError::NotFound(format!(
                "No record found for unique index '{}'",
                payload.index
            ))
        })?;
        if Self::unique_index_value(&key)? == TitoIndexValue::RecordCopy {
            let (record, _) = self.copied_index_record(&key, &owner_bytes, true)?;
            tx.guard(&T::table(), &record.id(), TitoGuard::Shared).await;
            return Ok(record);
        }
        let owner_id = Self::unique_owner_id(&key, owner_bytes)?;
        let primary_key = format!("{}:{owner_id}", self.get_table());
        let primary_bytes = tx.get(&primary_key).await?.ok_or_else(|| {
            TitoError::IndexError(format!(
                "Unique index '{}' on model '{}' points to a missing owner",
                payload.index,
                T::table()
            ))
        })?;
        let json = serde_json::from_slice::<Value>(&primary_bytes).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize owner of unique index '{}': {}",
                payload.index, error
            ))
        })?;
        let primary: T = serde_json::from_value(json.clone()).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize owner of unique index '{}': {}",
                payload.index, error
            ))
        })?;
        if primary.id() != owner_id
            || !self
                .build_index_keys(&primary_key, &primary.unique_indexes(), &json, true)?
                .contains(&key)
        {
            return Err(TitoError::IndexError(format!(
                "Unique index '{}' on model '{}' disagrees with its owner",
                payload.index,
                T::table()
            )));
        }
        tx.guard(&T::table(), &owner_id, TitoGuard::Shared).await;
        Ok(primary)
    }

    fn declared_index<'a>(
        indexes: &'a [TitoIndexConfig],
        name: &str,
    ) -> Result<&'a TitoIndexConfig, TitoError> {
        indexes
            .iter()
            .find(|index_config| index_config.name == name)
            .ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Index '{}' not found on model '{}'",
                    name,
                    T::table()
                ))
            })
    }

    fn index_scan(
        index: &TitoIndexConfig,
        payload: TitoFindByIndexPayload,
    ) -> Result<TitoScanPayload, TitoError> {
        let mut key_from_values = vec![];
        for (i, value) in payload.values.iter().enumerate() {
            let index_field = index.fields.get(i).ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Index '{}' has {} fields but {} values were provided",
                    payload.index,
                    index.fields.len(),
                    payload.values.len()
                ))
            })?;

            let value = match index_field.r#type {
                TitoIndexFieldType::String | TitoIndexFieldType::CustomString(_) => {
                    encode_index_string(value)
                }
                TitoIndexFieldType::Number => encode_index_integer_query(value)?,
            };

            key_from_values.push(format!("{}:{}", index_field.name, value));
        }

        Ok(TitoScanPayload {
            start: format!("index:{}:{}:", payload.index, key_from_values.join(":")),
            end: None,
            limit: payload.limit,
            cursor: payload.cursor,
        })
    }

    pub async fn find_by_index_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let schema = T::default();
        let indexes = schema.indexes();
        let index = Self::declared_index(&indexes, &payload.index)?;
        self.scan(Self::index_scan(index, payload)?, tx).await
    }

    pub async fn find_by_index_reverse_raw(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<(Vec<(String, Value)>, bool), TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let schema = T::default();
        let indexes = schema.indexes();
        let index = Self::declared_index(&indexes, &payload.index)?;
        self.scan_reverse(Self::index_scan(index, payload)?, tx)
            .await
    }

    fn index_id_page(
        &self,
        index: &TitoIndexConfig,
        rows: Vec<TitoKvPair>,
        has_more: bool,
    ) -> Result<TitoPaginated<String>, TitoError> {
        let mut ids = Vec::with_capacity(rows.len());
        let mut last_key = None;
        for (key, _) in rows {
            let key = Self::scanned_key(key)?;
            let id = Self::index_entry_id(index, &key).ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Index key '{key}' is not an entry of index '{}' on model '{}'",
                    index.name,
                    T::table()
                ))
            })?;
            ids.push(id);
            last_key = Some(key);
        }
        Ok(TitoPaginated::new(
            ids,
            self.next_cursor(has_more, last_key)?,
        ))
    }

    async fn index_ids_with_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<TitoPaginated<String>, TitoError> {
        let schema = T::default();
        let indexes = schema.indexes();
        let index = Self::declared_index(&indexes, &payload.index)?;
        let (rows, has_more) = self
            .scan_rows(Self::index_scan(index, payload)?, true, tx)
            .await?;
        self.index_id_page(index, rows, has_more)
    }

    pub(crate) async fn index_ids(
        &self,
        payload: TitoFindByIndexPayload,
        tx: Option<&E::Transaction>,
    ) -> Result<TitoPaginated<String>, TitoError> {
        match tx {
            Some(tx) => self.index_ids_with_tx(payload, tx).await,
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move { self.index_ids_with_tx(payload, &tx).await }
                })
                .await
            }
        }
    }

    async fn find_by_index_with_tx(
        &self,
        payload: TitoFindByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<TitoPaginated<T>, TitoError>
    where
        T: serde::de::DeserializeOwned,
    {
        let (items, has_more) = self.find_by_index_raw(payload, tx).await?;

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
                    limit: Some(1),
                    cursor: None,
                },
                tx,
            )
            .await?;

        if let Some(value) = items.first() {
            match serde_json::from_value::<T>(value.1.clone()) {
                Ok(item) => Ok(item),
                Err(e) => Err(TitoError::DeserializationFailed(format!(
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
}
