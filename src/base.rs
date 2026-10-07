use std::collections::{BTreeMap, HashSet};
use std::future::Future;

use std::marker::PhantomData;

use crate::{
    error::TitoError,
    query::IndexQueryBuilder,
    reference::{
        deleting_marker_key, parse_reference_key, primary_key, reference_key,
        reference_source_prefix, reference_target_prefix, validate_reference,
        validate_reference_id, validate_reference_table, DELETING_MARKER_KEY_PREFIX,
        REFERENCE_KEY_PREFIX,
    },
    types::{
        FieldValue, ReverseIndex, TitoCursor, TitoEngine, TitoFindPayload, TitoIncomingReference,
        TitoKvPair, TitoModelOptions, TitoPaginated, TitoRecordVersion, TitoReference,
        TitoScanPayload, TitoTransaction, TitoVersioned,
    },
    utils::{key_after_bytes, prefix_end_bytes},
};

use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

const MAX_MANIFEST_KEYS: usize = 10_000;
const MAX_MANIFEST_BYTES: usize = 1_048_576;
const MAX_REFUSAL_REFERENCES: usize = 20;
const REFERENCE_SCAN_BATCH: u32 = 256;

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
        let table = T::table();
        let raw_id = primary_key
            .strip_prefix(&format!("{}:", self.get_table()))
            .ok_or_else(|| {
                TitoError::IndexError(format!(
                    "Primary key '{}' does not belong to model '{}'",
                    primary_key, table
                ))
            })?;
        let expected_suffix = format!(":{}", primary_key);
        let unique_prefix = format!("unique-index:{}:", table);
        let marker = deleting_marker_key(&table, raw_id);
        for key in &reverse_index.value {
            let ordinary = key.starts_with("index:") && key.ends_with(&expected_suffix);
            let unique = key.starts_with(&unique_prefix);
            let reference = parse_reference_key(key).is_some_and(|reference| {
                reference.source_table == table && reference.source_id == raw_id
            });
            let deleting = *key == marker;
            if !ordinary && !unique && !reference && !deleting {
                return Err(TitoError::IndexError(format!(
                    "Reverse index for '{}' contains an invalid index key '{}'",
                    primary_key, key
                )));
            }
        }
        Ok(())
    }

    fn is_reference_state_key(key: &str) -> bool {
        key.starts_with(REFERENCE_KEY_PREFIX) || key.starts_with(DELETING_MARKER_KEY_PREFIX)
    }

    fn declared_references(
        &self,
        raw_id: &str,
        value: &T,
    ) -> Result<BTreeMap<String, TitoReference>, TitoError> {
        let table = T::table();
        let mut references = BTreeMap::new();
        for reference in value.references() {
            validate_reference(&reference)?;
            if reference.table == table && reference.id == raw_id {
                continue;
            }
            let key = reference_key(
                &reference.table,
                &reference.id,
                &table,
                raw_id,
                &reference.path,
            );
            references.insert(key, reference);
        }
        Ok(references)
    }

    fn declared_marker(&self, raw_id: &str, value: &T) -> Option<String> {
        value
            .is_deleting()
            .then(|| deleting_marker_key(&T::table(), raw_id))
    }

    fn incoming_value(&self, raw_id: &str, reference: &TitoReference) -> Result<Value, TitoError> {
        serde_json::to_value(TitoIncomingReference::new(
            T::table(),
            raw_id,
            reference.path.clone(),
        ))
        .map_err(|error| TitoError::SerializationFailed(error.to_string()))
    }

    fn manifest_bytes(
        &self,
        primary_key: &str,
        manifest: &ReverseIndex,
    ) -> Result<Vec<u8>, TitoError> {
        if manifest.value.len() > MAX_MANIFEST_KEYS {
            return Err(TitoError::InvalidInput(format!(
                "Record '{}' needs more than {} index and reference keys",
                primary_key, MAX_MANIFEST_KEYS
            )));
        }
        let bytes = serde_json::to_vec(manifest)
            .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(TitoError::InvalidInput(format!(
                "Record '{}' has an index and reference manifest over one MiB",
                primary_key
            )));
        }
        Ok(bytes)
    }

    async fn check_reference_targets(
        &self,
        references: &BTreeMap<String, TitoReference>,
        old_reference_keys: &HashSet<String>,
        tx: &E::Transaction,
    ) -> Result<Vec<String>, TitoError> {
        let kept: HashSet<(String, String)> = old_reference_keys
            .iter()
            .filter_map(|key| parse_reference_key(key))
            .map(|key| (key.target_table, key.target_id))
            .collect();
        let mut targets: BTreeMap<(String, String), String> = BTreeMap::new();
        for reference in references.values() {
            targets
                .entry((reference.table.clone(), reference.id.clone()))
                .or_insert_with(|| reference.path.clone());
        }
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        let mut reads = Vec::with_capacity(targets.len() * 2);
        for (table, id) in targets.keys() {
            reads.push(primary_key(table, id));
            if !kept.contains(&(table.clone(), id.clone())) {
                reads.push(deleting_marker_key(table, id));
            }
        }
        let found: HashSet<Vec<u8>> = tx
            .batch_get(reads)
            .await?
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        let mut fences = Vec::new();
        for ((table, id), path) in targets {
            if !found.contains(primary_key(&table, &id).as_bytes()) {
                return Err(TitoError::ReferenceMissing { table, id, path });
            }
            if kept.contains(&(table.clone(), id.clone())) {
                continue;
            }
            let marker = deleting_marker_key(&table, &id);
            if found.contains(marker.as_bytes()) {
                return Err(TitoError::ReferenceDeleting { table, id, path });
            }
            fences.push(marker);
        }
        Ok(fences)
    }

    fn reference_range_end(prefix: &[u8]) -> Result<Vec<u8>, TitoError> {
        prefix_end_bytes(prefix).ok_or_else(|| {
            TitoError::InvalidInput("Reference prefix has no finite range endpoint".to_string())
        })
    }

    async fn scan_incoming_references(
        &self,
        raw_id: &str,
        mut start: Vec<u8>,
        end: &[u8],
        limit: Option<usize>,
        incoming: &mut Vec<TitoIncomingReference>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let table = T::table();
        while start.as_slice() < end {
            let batch = match limit {
                Some(limit) => {
                    let remaining = limit.saturating_sub(incoming.len());
                    if remaining == 0 {
                        return Ok(());
                    }
                    u32::try_from(remaining)
                        .unwrap_or(u32::MAX)
                        .min(REFERENCE_SCAN_BATCH)
                }
                None => REFERENCE_SCAN_BATCH,
            };
            let page = tx.scan(start.clone()..end.to_vec(), batch).await?;
            let scanned = page.len();
            for (key_bytes, value_bytes) in page {
                let key = String::from_utf8(key_bytes).map_err(|error| {
                    TitoError::DeserializationFailed(format!(
                        "Reference scan returned a non-UTF-8 key (valid through byte {})",
                        error.utf8_error().valid_up_to()
                    ))
                })?;
                let reference: TitoIncomingReference = serde_json::from_slice(&value_bytes)
                    .map_err(|error| {
                        TitoError::DeserializationFailed(format!(
                            "Failed to deserialize reference '{}': {}",
                            key, error
                        ))
                    })?;
                if reference_key(
                    &table,
                    raw_id,
                    &reference.table,
                    &reference.id,
                    &reference.path,
                ) != key
                {
                    return Err(TitoError::IndexError(format!(
                        "Reference key '{}' disagrees with its value",
                        key
                    )));
                }
                start = key_after_bytes(key.as_bytes());
                incoming.push(reference);
            }
            if scanned < batch as usize {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn incoming_references(
        &self,
        raw_id: &str,
        limit: Option<usize>,
        tx: &E::Transaction,
    ) -> Result<Vec<TitoIncomingReference>, TitoError> {
        let prefix = reference_target_prefix(&T::table(), raw_id);
        let end = Self::reference_range_end(prefix.as_bytes())?;
        let mut incoming = Vec::new();
        self.scan_incoming_references(raw_id, prefix.into_bytes(), &end, limit, &mut incoming, tx)
            .await?;
        Ok(incoming)
    }

    pub async fn referenced_by(
        &self,
        id: &str,
        tx: &E::Transaction,
    ) -> Result<Vec<TitoIncomingReference>, TitoError> {
        validate_reference_id(id)?;
        self.incoming_references(id, None, tx).await
    }

    pub async fn referenced_by_except(
        &self,
        id: &str,
        source_tables: &[&str],
        limit: usize,
        tx: &E::Transaction,
    ) -> Result<Vec<TitoIncomingReference>, TitoError> {
        validate_reference_id(id)?;
        if limit == 0 {
            return Err(TitoError::InvalidInput(
                "A reference limit must be greater than zero".to_string(),
            ));
        }
        let table = T::table();
        let prefix = reference_target_prefix(&table, id);
        let end = Self::reference_range_end(prefix.as_bytes())?;
        let mut skipped = Vec::with_capacity(source_tables.len());
        for source_table in source_tables {
            validate_reference_table(source_table)?;
            let skip_start = reference_source_prefix(&table, id, source_table).into_bytes();
            let skip_end = Self::reference_range_end(&skip_start)?;
            skipped.push((skip_start, skip_end));
        }
        skipped.sort();
        skipped.dedup();
        let mut incoming = Vec::new();
        let mut start = prefix.into_bytes();
        for (skip_start, skip_end) in skipped {
            if start < skip_start {
                self.scan_incoming_references(
                    id,
                    start.clone(),
                    &skip_start,
                    Some(limit),
                    &mut incoming,
                    tx,
                )
                .await?;
                if incoming.len() >= limit {
                    return Ok(incoming);
                }
            }
            if skip_end > start {
                start = skip_end;
            }
        }
        self.scan_incoming_references(id, start, &end, Some(limit), &mut incoming, tx)
            .await?;
        Ok(incoming)
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
        let owner: T = serde_json::from_slice(&bytes).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Failed to deserialize unique index on model '{}': {}",
                T::table(),
                error
            ))
        })?;
        if owner.id() != expected_id {
            return Ok(false);
        }
        Ok(true)
    }

    pub async fn assert_current(&self, id: &str, tx: &E::Transaction) -> Result<(), TitoError> {
        let primary_key = format!("{}:{id}", self.get_table());
        self.load_index_state(&primary_key, tx)
            .await?
            .ok_or_else(|| {
                TitoError::NotFound(format!("Record '{primary_key}' not found in database"))
            })?;
        self.fence_primary(&primary_key, tx).await
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
        let mut owned = self.get_index_keys(primary_key.clone(), &stored, &value)?;
        for (key, reference) in self.declared_references(id, &stored)? {
            let incoming = self.incoming_value(id, &reference)?;
            owned.push((key, incoming));
        }
        if let Some(marker) = self.declared_marker(id, &stored) {
            owned.push((marker, Value::Bool(true)));
        }
        let declared: HashSet<_> = metadata.value.iter().collect();
        let computed: HashSet<_> = owned.iter().map(|(key, _)| key).collect();
        if declared.len() != metadata.value.len()
            || computed.len() != owned.len()
            || declared != computed
        {
            return Err(TitoError::IndexError(
                "Restore metadata differs from current model indexes and references".into(),
            ));
        }
        for (key, _) in &owned {
            if let Some(index) = Self::unique_index_name(key) {
                if let Some(bytes) = tx.get(key).await? {
                    let owner: T = serde_json::from_slice(&bytes)
                        .map_err(|error| TitoError::DeserializationFailed(error.to_string()))?;
                    if owner.id() != id {
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
            self.put_value(key, &value, tx).await?;
        }
        Ok(stored)
    }

    async fn fence_primary(&self, primary_key: &str, tx: &E::Transaction) -> Result<(), TitoError> {
        let bytes = tx.get(&primary_key).await?.ok_or_else(|| {
            TitoError::NotFound(format!("Record '{primary_key}' not found in database"))
        })?;
        tx.put(primary_key, bytes).await
    }

    pub async fn assert_version(
        &self,
        id: &str,
        expected: TitoRecordVersion,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let primary_key = format!("{}:{id}", self.get_table());
        let Some(metadata) = self.load_index_state(&primary_key, tx).await? else {
            return Ok(false);
        };
        if metadata.version != expected {
            return Ok(false);
        }
        self.fence_primary(&primary_key, tx).await?;
        Ok(true)
    }

    pub(crate) async fn assert_reverse_index_key(
        &self,
        primary_key: &str,
        prefix: &str,
        expected: &str,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let reverse_key = format!("reverse-index:{primary_key}");
        let Some(bytes) = tx.get(&reverse_key).await? else {
            return Ok(false);
        };
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(TitoError::IndexError(
                "Index assertion metadata exceeds one MiB".to_string(),
            ));
        }
        let reverse = self.deserialize_reverse_index(&reverse_key, &bytes)?;
        if reverse.value.len() > MAX_MANIFEST_KEYS {
            return Err(TitoError::IndexError(
                "Index assertion metadata exceeds 10000 keys".to_string(),
            ));
        }
        self.validate_reverse_index_keys(primary_key, &reverse)?;
        let mut unique = std::collections::HashSet::with_capacity(reverse.value.len());
        let mut selected = None;
        for key in &reverse.value {
            if !unique.insert(key) {
                return Err(TitoError::IndexError(
                    "Index assertion metadata contains duplicate keys".to_string(),
                ));
            }
            if key.starts_with(prefix) {
                if selected.replace(key.as_str()).is_some() {
                    return Err(TitoError::IndexError(
                        "Index assertion requires a single-valued index".to_string(),
                    ));
                }
            }
        }
        if selected != Some(expected) {
            return Ok(false);
        }
        tx.put(reverse_key, bytes).await?;
        Ok(true)
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
        let references = self.declared_references(&raw_id, &payload)?;
        let marker = self.declared_marker(&raw_id, &payload);
        let mut manifest_keys: Vec<String> =
            all_index_data.iter().map(|(key, _)| key.clone()).collect();
        manifest_keys.extend(references.keys().cloned());
        manifest_keys.extend(marker.iter().cloned());
        let manifest = ReverseIndex {
            value: manifest_keys,
            version: TitoRecordVersion::from_transaction(tx.start_version())?,
        };
        let reverse_bytes = self.manifest_bytes(&id, &manifest)?;
        let old_reference_keys: HashSet<String> = old_index_keys
            .as_ref()
            .map(|metadata| {
                metadata
                    .value
                    .iter()
                    .filter(|key| Self::is_reference_state_key(key))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

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

        let fences = self
            .check_reference_targets(&references, &old_reference_keys, tx)
            .await?;

        if let Some(old_index_keys) = old_index_keys {
            for key in old_index_keys.value {
                if Self::is_reference_state_key(&key) {
                    if !references.contains_key(&key) && marker.as_ref() != Some(&key) {
                        self.delete(key, tx).await?;
                    }
                    continue;
                }
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

        for fence in fences {
            self.delete(fence, tx).await?;
        }
        self.put_value(id, &stored_value, tx).await?;
        for (key, value) in all_index_data {
            self.put_value(key, &value, tx).await?;
        }
        for (key, reference) in &references {
            if !old_reference_keys.contains(key) {
                let incoming = self.incoming_value(&raw_id, reference)?;
                self.put_value(key.clone(), &incoming, tx).await?;
            }
        }
        if let Some(marker) = marker {
            if !old_reference_keys.contains(&marker) {
                self.put_value(marker, &Value::Bool(true), tx).await?;
            }
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

        let mut keys = match self.load_index_state(&id, tx).await? {
            Some(metadata) => metadata.value,
            None => return Err(TitoError::NotFound(format!("Entity not found: {}", id))),
        };

        let by = self
            .incoming_references(raw_id, Some(MAX_REFUSAL_REFERENCES), tx)
            .await?;
        if !by.is_empty() {
            return Err(TitoError::Referenced {
                table: T::table(),
                id: raw_id.to_string(),
                by,
            });
        }

        keys.push(deleting_marker_key(&T::table(), raw_id));
        self.remove_keys(&id, raw_id, keys, tx).await
    }

    pub async fn erase(&self, key: String, tx: &E::Transaction) -> Result<bool, TitoError> {
        let id = format!("{}:{}", self.get_table(), key);

        let keys = match self.load_index_state(&id, tx).await? {
            Some(metadata) => metadata.value,
            None => return Err(TitoError::NotFound(format!("Entity not found: {}", id))),
        };

        self.remove_keys(&id, &key, keys, tx).await
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
