use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::marker::PhantomData;

use crate::{
    backend::tikv::{TiKVBackend, TiKVTransaction},
    error::TitoError,
    fence::record_fence_keys,
    index::{self, Entry, Holds, INDEX_KEY_PREFIX},
    query::IndexQueryBuilder,
    read::{key_rows, single_row, Found, Noted, Read, Request},
    types::{
        Guard, Sealed, TitoCursor, TitoEngine, TitoIndexField, TitoIndexValue,
        TitoModelConstraints, TitoPaginated, TitoTransaction,
    },
    utils::{key_after_bytes, prefix_end, prefix_end_bytes},
};

use base64::{engine::general_purpose, Engine};
use serde_json::Value;

pub const MAX_SAVE_BYTES: usize = 67_108_864;
const REBUILD_PAGE_RECORDS: u32 = 100;
const REBUILD_PAGE_BYTES: usize = 8_388_608;
const REBUILD_REMOVE_KEYS: u32 = 1_000;

#[derive(Clone)]
pub struct TitoModel<E: TitoEngine, T> {
    pub engine: E,
    _phantom: PhantomData<T>,
}

impl<E: TitoEngine, T> TitoModel<E, T> {
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            _phantom: PhantomData,
        }
    }
}

impl<E: TitoEngine, T: TitoModelConstraints> TitoModel<E, T> {
    pub fn get_table(&self) -> String {
        T::key_prefix()
    }

    pub fn key(&self, id: &str) -> String {
        record_key::<T>(id)
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
}

fn record_key<T: TitoModelConstraints>(id: &str) -> String {
    format!("{}:{}", T::key_prefix(), id)
}

fn record_json<T: TitoModelConstraints>(record: &T) -> Result<Value, TitoError> {
    serde_json::to_value(record).map_err(|error| TitoError::SerializationFailed(error.to_string()))
}

fn json_bytes(json: &Value) -> Result<Vec<u8>, TitoError> {
    serde_json::to_vec(json).map_err(|error| TitoError::SerializationFailed(error.to_string()))
}

pub(crate) fn noted<T: TitoModelConstraints>(record: &T) -> Result<Noted, TitoError> {
    let id = record.id();
    Ok(Noted {
        table: T::table(),
        key: record_key::<T>(&id),
        entries: index::remembered_entries(record, &record_json(record)?)?,
        id,
    })
}

fn stored_record<T: TitoModelConstraints>(
    key: &str,
    id: &str,
    bytes: &[u8],
) -> Result<T, TitoError> {
    let record: T = serde_json::from_slice(bytes).map_err(|error| {
        TitoError::DeserializationFailed(format!("Failed to deserialize record '{key}': {error}"))
    })?;
    if record.id() != id {
        return Err(TitoError::DeserializationFailed(format!(
            "Record '{key}' holds the id of another record"
        )));
    }
    Ok(record)
}

fn scanned_id<'a>(prefix: &str, key: &'a str) -> Result<&'a str, TitoError> {
    key.strip_prefix(prefix)
        .ok_or_else(|| TitoError::Internal(format!("Scan of '{prefix}' returned the key '{key}'")))
}

pub(crate) fn utf8_key(key: Vec<u8>) -> Result<String, TitoError> {
    String::from_utf8(key).map_err(|error| {
        TitoError::DeserializationFailed(format!(
            "Storage scan returned a non-UTF-8 key (valid through byte {})",
            error.utf8_error().valid_up_to()
        ))
    })
}

pub(crate) fn encode_cursor(key: &[u8]) -> Result<String, TitoError> {
    let cursor = TitoCursor {
        ids: vec![Some(utf8_key(key.to_vec())?)],
    };
    let bytes = serde_json::to_vec(&cursor).map_err(|error| {
        TitoError::SerializationFailed(format!("Failed to encode cursor: {error}"))
    })?;
    Ok(general_purpose::STANDARD.encode(bytes))
}

pub(crate) fn decode_cursor(cursor: &str) -> Result<Vec<u8>, TitoError> {
    let bytes = general_purpose::STANDARD
        .decode(cursor)
        .map_err(|_| TitoError::DeserializationFailed("Failed to decode cursor".to_string()))?;
    let cursor = serde_json::from_slice::<TitoCursor>(&bytes).map_err(|_| {
        TitoError::DeserializationFailed("Failed to deserialize cursor".to_string())
    })?;
    Ok(cursor.first_id()?.into_bytes())
}

pub(crate) fn check_cursor(cursor: &[u8], start: &[u8], end: &[u8]) -> Result<(), TitoError> {
    if cursor < start || cursor >= end {
        return Err(TitoError::InvalidInput(
            "Cursor is outside the requested scan range".to_string(),
        ));
    }
    Ok(())
}

fn entry_values(
    entries: Vec<Entry>,
    id: &str,
    json: &Value,
    record: &[u8],
) -> Result<Vec<(Entry, Vec<u8>)>, TitoError> {
    let mut values = Vec::with_capacity(entries.len());
    for entry in entries {
        let value = index::entry_bytes(&entry, id, json, record)?;
        values.push((entry, value));
    }
    Ok(values)
}

fn check_save_size(
    key: &str,
    record: &[u8],
    entries: &[(Entry, Vec<u8>)],
) -> Result<(), TitoError> {
    let total = entries.iter().fold(
        key.len().saturating_add(record.len()),
        |total, (entry, value)| {
            total
                .saturating_add(entry.key.len())
                .saturating_add(value.len())
        },
    );
    if total > MAX_SAVE_BYTES {
        return Err(TitoError::InvalidInput(format!(
            "A save exceeds the {MAX_SAVE_BYTES}-byte limit"
        )));
    }
    Ok(())
}

fn unique_violation<T: TitoModelConstraints>(entry: &Entry) -> TitoError {
    TitoError::UniqueViolation {
        model: T::table(),
        index: entry.name.to_string(),
        values: entry.fields.clone(),
    }
}

async fn delete_fences<T: TitoModelConstraints>(
    id: &str,
    tx: &TiKVTransaction,
) -> Result<(), TitoError> {
    for fence in record_fence_keys(&T::table(), id) {
        tx.delete(fence).await?;
    }
    Ok(())
}

impl<T: TitoModelConstraints> TitoModel<TiKVBackend, T> {
    pub fn query_by_index(&self, index: impl Into<String>) -> IndexQueryBuilder<TiKVBackend, T> {
        IndexQueryBuilder::new(self.clone(), index.into())
    }

    pub fn read(&self, id: &str, guard: Guard) -> Read<T> {
        let key = self.key(id);
        self.read_maybe(id, guard).map(move |found| {
            found
                .ok_or_else(|| TitoError::NotFound(format!("Record '{key}' not found in database")))
        })
    }

    pub fn read_maybe(&self, id: &str, guard: Guard) -> Read<Option<T>> {
        let key = self.key(id);
        let id = id.to_string();
        Read::new(
            Request::Keys(vec![key.clone().into_bytes()]),
            guard,
            move |rows| {
                let Some(bytes) = single_row(rows)? else {
                    return Ok(Found {
                        value: None,
                        records: Vec::new(),
                    });
                };
                let record = stored_record::<T>(&key, &id, &bytes)?;
                let records = vec![noted(&record)?];
                Ok(Found {
                    value: Some(record),
                    records,
                })
            },
        )
    }

    pub fn read_many(&self, ids: Vec<String>, guard: Guard) -> Read<Vec<T>> {
        let keys = ids.iter().map(|id| self.key(id)).collect::<Vec<_>>();
        let request = Request::Keys(keys.iter().map(|key| key.clone().into_bytes()).collect());
        Read::new(request, guard, move |rows| {
            let mut found = Found {
                value: Vec::new(),
                records: Vec::new(),
            };
            for ((id, key), bytes) in ids.iter().zip(&keys).zip(key_rows(rows)?) {
                let Some(bytes) = bytes else {
                    continue;
                };
                let record = stored_record::<T>(key, id, &bytes)?;
                found.records.push(noted(&record)?);
                found.value.push(record);
            }
            Ok(found)
        })
    }

    pub fn read_unique(
        &self,
        index: &'static str,
        values: Vec<TitoIndexField>,
        guard: Guard,
    ) -> Read<T> {
        self.read_unique_maybe(index, values, guard)
            .map(move |found| {
                found.ok_or_else(|| {
                    TitoError::NotFound(format!("No record found for unique index '{index}'"))
                })
            })
    }

    pub fn read_unique_maybe(
        &self,
        index: &'static str,
        values: Vec<TitoIndexField>,
        guard: Guard,
    ) -> Read<Option<T>> {
        let key = match index::unique_key(index, &values) {
            Ok(key) => key,
            Err(error) => return Read::refused(error),
        };
        Read::new(
            Request::Keys(vec![key.clone().into_bytes()]),
            guard,
            move |rows| {
                let Some(bytes) = single_row(rows)? else {
                    return Ok(Found {
                        value: None,
                        records: Vec::new(),
                    });
                };
                let record = index::unique_copy::<T>(&key, &bytes)?;
                let records = vec![noted(&record)?];
                Ok(Found {
                    value: Some(record),
                    records,
                })
            },
        )
    }

    pub async fn create(&self, record: &T, tx: &TiKVTransaction) -> Result<(), TitoError> {
        let id = record.id();
        let key = self.key(&id);
        let json = record_json(record)?;
        let bytes = json_bytes(&json)?;
        let written = entry_values(index::entries(record)?, &id, &json, &bytes)?;
        check_save_size(&key, &bytes, &written)?;
        let remembered = written
            .iter()
            .map(|(entry, value)| index::remembered_entry(entry, value))
            .collect::<Vec<_>>();
        let taken = TitoError::RecordExists {
            model: T::table(),
            id: id.clone(),
        };
        tx.insert(key.clone().into_bytes(), bytes, taken).await?;
        for (entry, value) in written {
            if entry.unique {
                let taken = unique_violation::<T>(&entry);
                tx.insert(entry.key.into_bytes(), value, taken).await?;
            } else {
                tx.put(entry.key, value).await?;
            }
        }
        delete_fences::<T>(&id, tx).await?;
        tx.note_written(key, remembered);
        Ok(())
    }

    pub async fn update(&self, old: &T, new: &T, tx: &TiKVTransaction) -> Result<(), TitoError> {
        let id = old.id();
        let key = self.key(&id);
        if new.id() != id {
            return Err(TitoError::UpdateFailed(format!(
                "An update of record '{key}' must keep its id"
            )));
        }
        let Some(previous) = tx.remembered(&key) else {
            return Err(TitoError::UpdateFailed(format!(
                "Record '{key}' was not read in this transaction"
            )));
        };
        let new_json = record_json(new)?;
        let new_bytes = json_bytes(&new_json)?;
        let written = entry_values(index::entries(new)?, &id, &new_json, &new_bytes)?;
        check_save_size(&key, &new_bytes, &written)?;
        let kept = written
            .iter()
            .map(|(entry, _)| entry.key.as_str())
            .collect::<HashSet<_>>();
        let before = previous
            .iter()
            .map(|entry| (entry.key.as_str(), &entry.holds))
            .collect::<HashMap<_, _>>();
        for entry in &previous {
            if !kept.contains(entry.key.as_str()) {
                tx.delete(&entry.key).await?;
            }
        }
        tx.put(&key, &new_bytes).await?;
        for (entry, value) in &written {
            let Some(held) = before.get(entry.key.as_str()) else {
                if entry.unique {
                    let taken = unique_violation::<T>(entry);
                    tx.insert(entry.key.clone().into_bytes(), value.clone(), taken)
                        .await?;
                } else {
                    tx.put(&entry.key, value).await?;
                }
                continue;
            };
            let unchanged = match (held, entry.value) {
                (Holds::KeysOnly, TitoIndexValue::KeysOnly) => true,
                (Holds::Part(bytes), TitoIndexValue::Part(_)) => bytes == value,
                _ => false,
            };
            if !unchanged {
                tx.put(&entry.key, value).await?;
            }
        }
        delete_fences::<T>(&id, tx).await?;
        let remembered = written
            .iter()
            .map(|(entry, value)| index::remembered_entry(entry, value))
            .collect();
        tx.note_written(key, remembered);
        Ok(())
    }

    pub async fn remove(&self, record: &T, tx: &TiKVTransaction) -> Result<(), TitoError> {
        let id = record.id();
        let key = self.key(&id);
        let Some(previous) = tx.remembered(&key) else {
            return Err(TitoError::DeleteFailed(format!(
                "Record '{key}' was not read in this transaction"
            )));
        };
        for entry in &previous {
            tx.delete(&entry.key).await?;
        }
        tx.delete(&key).await?;
        delete_fences::<T>(&id, tx).await?;
        tx.forget(&key);
        Ok(())
    }

    pub async fn scan_records(
        &self,
        limit: u32,
        cursor: Option<String>,
        tx: &TiKVTransaction,
    ) -> Result<TitoPaginated<T>, TitoError> {
        if limit == 0 {
            return Err(TitoError::InvalidInput(
                "Scan limit must be greater than zero".to_string(),
            ));
        }
        let prefix = format!("{}:", self.get_table());
        let first = prefix.clone().into_bytes();
        let end = prefix_end_bytes(&first).ok_or_else(|| {
            TitoError::InvalidInput("Scan prefix has no finite range endpoint".to_string())
        })?;
        let start = match cursor {
            Some(cursor) => {
                let last = decode_cursor(&cursor)?;
                check_cursor(&last, &first, &end)?;
                key_after_bytes(&last)
            }
            None => first,
        };
        let mut rows = tx.scan(start..end, limit.saturating_add(1)).await?;
        let more = rows.len() > limit as usize;
        rows.truncate(limit as usize);
        let cursor = match rows.last() {
            Some((key, _)) if more => Some(encode_cursor(key)?),
            _ => None,
        };
        let mut items = Vec::with_capacity(rows.len());
        let mut records = Vec::with_capacity(rows.len());
        for (key, bytes) in rows {
            let key = utf8_key(key)?;
            let id = scanned_id(&prefix, &key)?;
            let record = stored_record::<T>(&key, id, &bytes)?;
            records.push(noted(&record)?);
            items.push(record);
        }
        tx.remember(records, Guard::None).await;
        Ok(TitoPaginated::new(items, cursor))
    }

    pub async fn rebuild_index(&self, index: &'static str) -> Result<(), TitoError> {
        index::validate_name(index)?;
        let start = format!("{INDEX_KEY_PREFIX}{index}:");
        let end = prefix_end(start.clone());
        loop {
            let removed = self
                .engine
                .transaction(|tx| {
                    let range = start.clone().into_bytes()..end.clone().into_bytes();
                    async move {
                        let keys = tx.scan_keys(range, REBUILD_REMOVE_KEYS).await?;
                        for key in &keys {
                            tx.delete(key).await?;
                        }
                        Ok::<_, TitoError>(keys.len())
                    }
                })
                .await?;
            if removed < REBUILD_REMOVE_KEYS as usize {
                break;
            }
        }
        let mut after: Option<Vec<u8>> = None;
        loop {
            let next = self
                .engine
                .transaction(|tx| {
                    let after = after.clone();
                    async move { self.rebuild_page(index, after, &tx).await }
                })
                .await?;
            match next {
                Some(next) => after = Some(next),
                None => return Ok(()),
            }
        }
    }

    async fn rebuild_page(
        &self,
        index: &str,
        after: Option<Vec<u8>>,
        tx: &TiKVTransaction,
    ) -> Result<Option<Vec<u8>>, TitoError> {
        let prefix = format!("{}:", self.get_table());
        let first = prefix.clone().into_bytes();
        let end = prefix_end_bytes(&first).ok_or_else(|| {
            TitoError::InvalidInput("Scan prefix has no finite range endpoint".to_string())
        })?;
        let start = match after {
            Some(after) => key_after_bytes(&after),
            None => first,
        };
        let rows = tx.scan(start..end, REBUILD_PAGE_RECORDS).await?;
        let count = rows.len();
        let mut done = 0;
        let mut bytes = 0_usize;
        let mut last = None;
        let mut writes = Vec::new();
        for (key, value) in rows {
            let text = utf8_key(key.clone())?;
            let id = scanned_id(&prefix, &text)?;
            let record = stored_record::<T>(&text, id, &value)?;
            let json = record_json(&record)?;
            let record_bytes = json_bytes(&json)?;
            for entry in index::entries(&record)? {
                if entry.name != index {
                    continue;
                }
                let copy = index::entry_bytes(&entry, id, &json, &record_bytes)?;
                bytes = bytes
                    .saturating_add(entry.key.len())
                    .saturating_add(copy.len());
                writes.push((record.id(), entry, copy));
            }
            done += 1;
            last = Some(key);
            if bytes >= REBUILD_PAGE_BYTES {
                break;
            }
        }
        let unique = writes
            .iter()
            .filter(|(_, entry, _)| entry.unique)
            .map(|(_, entry, _)| entry.key.clone())
            .collect::<Vec<_>>();
        let held = tx
            .get_keys(unique, Sealed)
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();
        for (id, entry, copy) in writes {
            if !entry.unique {
                tx.put(&entry.key, &copy).await?;
                continue;
            }
            match held.get(entry.key.as_bytes()) {
                Some(holder) if index::unique_copy::<T>(&entry.key, holder)?.id() == id => {
                    tx.put(&entry.key, &copy).await?;
                }
                Some(_) => return Err(unique_violation::<T>(&entry)),
                None => {
                    let taken = unique_violation::<T>(&entry);
                    tx.insert(entry.key.into_bytes(), copy, taken).await?;
                }
            }
        }
        let more = done < count || count == REBUILD_PAGE_RECORDS as usize;
        Ok(if more { last } else { None })
    }
}
