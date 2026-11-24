use crate::{
    error::TitoError,
    key_encoder::{encode_index_integer_query, encode_index_number, encode_index_string},
    types::{
        FieldValue, TitoEngine, TitoFindByIndexPayload, TitoFindOneByIndexPayload, TitoGuard,
        TitoIndexConfig, TitoIndexFieldType, TitoKvPair, TitoModelTrait, TitoPaginated,
        TitoScanPayload, TitoTransaction,
    },
    TitoModel,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeySpace {
    Index,
    UniqueIndex,
    HolderIndex,
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
    pub(crate) fn key_base(space: KeySpace, name: &str) -> String {
        match space {
            KeySpace::Index => format!("index:{name}:"),
            KeySpace::UniqueIndex => format!("unique-index:{}:{name}:", T::table()),
            KeySpace::HolderIndex => format!("holder-index:{name}:"),
        }
    }

    pub(crate) fn build_index_keys(
        &self,
        id: &str,
        indexes: &[TitoIndexConfig],
        json: &Value,
        space: KeySpace,
    ) -> Result<Vec<String>, TitoError> {
        let mut all_index_keys = vec![];

        for index_config in indexes {
            if !index_config.condition {
                continue;
            }

            let base_key = Self::key_base(space, &index_config.name);

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
                    let key = match space {
                        KeySpace::UniqueIndex => format!("{base_key}{combo}"),
                        KeySpace::Index | KeySpace::HolderIndex => {
                            format!("{base_key}{combo}:{id}")
                        }
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
        let copy = serde_json::to_vec(json)
            .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
        Ok(self
            .index_keys(&id, value, json)?
            .into_iter()
            .map(|(key, space)| (key, Self::entry_bytes(space, &copy)))
            .collect())
    }

    pub(crate) fn index_keys(
        &self,
        id: &str,
        value: &T,
        json: &Value,
    ) -> Result<Vec<(String, KeySpace)>, TitoError> {
        let mut keys = Vec::new();
        let mut seen = HashSet::new();
        for (space, indexes) in [
            (KeySpace::Index, value.indexes()),
            (KeySpace::UniqueIndex, value.unique_indexes()),
            (KeySpace::HolderIndex, value.holder_indexes()),
        ] {
            for key in self.build_index_keys(id, &indexes, json, space)? {
                if seen.insert(key.clone()) {
                    keys.push((key, space));
                }
            }
        }
        Ok(keys)
    }

    pub(crate) fn entry_bytes(space: KeySpace, record: &[u8]) -> Vec<u8> {
        match space {
            KeySpace::Index | KeySpace::UniqueIndex => record.to_vec(),
            KeySpace::HolderIndex => Vec::new(),
        }
    }

    pub(crate) fn unique_owner_id(key: &str, bytes: &[u8]) -> Result<String, TitoError> {
        serde_json::from_slice::<T>(bytes)
            .map(|record| record.id())
            .map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize the copied owner of unique index key '{key}': {error}"
                ))
            })
    }

    fn holder_record_read(key: &str) -> TitoError {
        TitoError::IndexError(format!(
            "Holder index key '{key}' holds no record and is read only by key"
        ))
    }

    fn index_entry_id(index: &TitoIndexConfig, key: &str, space: KeySpace) -> Option<String> {
        let mut rest = key.strip_prefix(Self::key_base(space, &index.name).as_str())?;
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

    fn copied_index_record(
        &self,
        key: &str,
        bytes: &[u8],
        space: KeySpace,
    ) -> Result<(T, Value), TitoError> {
        let json = Self::scanned_json(key, bytes)?;
        let record: T = serde_json::from_value(json.clone()).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize copied record for index key '{key}': {error}"
            ))
        })?;
        let indexes = match space {
            KeySpace::Index => record.indexes(),
            KeySpace::UniqueIndex => record.unique_indexes(),
            KeySpace::HolderIndex => return Err(Self::holder_record_read(key)),
        };
        let own = indexes
            .into_iter()
            .filter(|index| key.starts_with(Self::key_base(space, &index.name).as_str()))
            .collect::<Vec<_>>();
        let primary = format!("{}:{}", self.get_table(), record.id());
        if !self
            .build_index_keys(&primary, &own, &json, space)?
            .iter()
            .any(|owned| owned == key)
        {
            return Err(TitoError::IndexError(format!(
                "Copied record for index key '{key}' disagrees with its key"
            )));
        }
        Ok((record, json))
    }

    pub(crate) fn scanned_values(
        &self,
        rows: Vec<TitoKvPair>,
    ) -> Result<Vec<(String, Value)>, TitoError> {
        let mut scanned = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let key = Self::scanned_key(key)?;
            let json = if key.starts_with("unique-index:") {
                self.copied_index_record(&key, &value, KeySpace::UniqueIndex)?
                    .1
            } else if key.starts_with("index:") {
                self.copied_index_record(&key, &value, KeySpace::Index)?.1
            } else if key.starts_with("holder-index:") {
                return Err(Self::holder_record_read(&key));
            } else {
                Self::scanned_json(&key, &value)?
            };
            scanned.push((key, json));
        }
        Ok(scanned)
    }

    pub fn unique_key(&self, payload: &TitoFindOneByIndexPayload) -> Result<String, TitoError> {
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
            "{}{fields}",
            Self::key_base(KeySpace::UniqueIndex, &payload.index)
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

    pub async fn find_one_by_unique_index_for_share(
        &self,
        payload: TitoFindOneByIndexPayload,
        tx: Option<&E::Transaction>,
    ) -> Result<T, TitoError> {
        match tx {
            Some(tx) => {
                self.find_one_by_unique_index_for_share_with_tx(&payload, tx)
                    .await
            }
            None => {
                self.tx(|tx| {
                    let payload = payload.clone();
                    async move {
                        self.find_one_by_unique_index_for_share_with_tx(&payload, &tx)
                            .await
                    }
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
        let key = self.unique_key(payload)?;
        let copy = tx.get(&key).await?.ok_or_else(|| {
            TitoError::NotFound(format!(
                "No record found for unique index '{}'",
                payload.index
            ))
        })?;
        let (record, _) = self.copied_index_record(&key, &copy, KeySpace::UniqueIndex)?;
        Ok(record)
    }

    async fn find_one_by_unique_index_for_share_with_tx(
        &self,
        payload: &TitoFindOneByIndexPayload,
        tx: &E::Transaction,
    ) -> Result<T, TitoError> {
        let record = self.find_one_by_unique_index_with_tx(payload, tx).await?;
        tx.guard(&T::table(), &record.id(), TitoGuard::Shared).await;
        Ok(record)
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

    fn read_index<'a>(
        indexes: &'a [TitoIndexConfig],
        name: &str,
    ) -> Result<&'a TitoIndexConfig, TitoError> {
        if !indexes.iter().any(|index| index.name == name)
            && T::default()
                .holder_indexes()
                .iter()
                .any(|holder| holder.name == name)
        {
            return Err(TitoError::IndexError(format!(
                "Index '{}' of model '{}' is a holder index: it holds no record and is read only by key",
                name,
                T::table()
            )));
        }
        Self::declared_index(indexes, name)
    }

    fn index_scan(
        index: &TitoIndexConfig,
        payload: TitoFindByIndexPayload,
        space: KeySpace,
    ) -> Result<TitoScanPayload, TitoError> {
        if payload.values.is_empty() {
            return Err(TitoError::IndexError(format!(
                "Index '{}' requires at least one value",
                payload.index
            )));
        }
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
            start: format!(
                "{}{}:",
                Self::key_base(space, &payload.index),
                key_from_values.join(":")
            ),
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
        let index = Self::read_index(&indexes, &payload.index)?;
        self.scan(Self::index_scan(index, payload, KeySpace::Index)?, tx)
            .await
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
        let index = Self::read_index(&indexes, &payload.index)?;
        self.scan_reverse(Self::index_scan(index, payload, KeySpace::Index)?, tx)
            .await
    }

    fn index_id_page(
        &self,
        index: &TitoIndexConfig,
        space: KeySpace,
        rows: Vec<TitoKvPair>,
        has_more: bool,
    ) -> Result<TitoPaginated<String>, TitoError> {
        let mut ids = Vec::with_capacity(rows.len());
        let mut last_key = None;
        for (key, _) in rows {
            let key = Self::scanned_key(key)?;
            let id = Self::index_entry_id(index, &key, space).ok_or_else(|| {
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
        let holders = schema.holder_indexes();
        let (index, space) = match indexes.iter().find(|index| index.name == payload.index) {
            Some(index) => (index, KeySpace::Index),
            None => (
                Self::declared_index(&holders, &payload.index)?,
                KeySpace::HolderIndex,
            ),
        };
        let (rows, has_more) = self
            .scan_rows(Self::index_scan(index, payload, space)?, true, tx)
            .await?;
        self.index_id_page(index, space, rows, has_more)
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
