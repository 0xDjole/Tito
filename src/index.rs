use crate::{
    error::TitoError,
    key_encoder::{encode_index_integer, encode_index_string},
    types::{TitoIndex, TitoIndexBound, TitoIndexField, TitoIndexValue, TitoModelTrait},
    utils::prefix_end,
};
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::{Map, Value};
use std::collections::HashSet;

pub(crate) const INDEX_KEY_PREFIX: &str = "index:";

pub(crate) struct Entry {
    pub(crate) key: String,
    pub(crate) name: &'static str,
    pub(crate) fields: Vec<TitoIndexField>,
    pub(crate) value: TitoIndexValue,
    pub(crate) unique: bool,
}

#[derive(Clone)]
pub(crate) enum Holds {
    Full,
    KeysOnly,
    Part(Vec<u8>),
}

#[derive(Clone)]
pub(crate) struct RememberedEntry {
    pub(crate) key: String,
    pub(crate) holds: Holds,
}

enum Stored<'a> {
    KeysOnly,
    Full(&'a [u8]),
    Part(&'a [u8]),
}

fn stored<'a>(key: &str, value: &'a [u8]) -> Result<Stored<'a>, TitoError> {
    match value.first() {
        None => Ok(Stored::KeysOnly),
        Some(b'{') => Ok(Stored::Full(value)),
        Some(b'[') => Ok(Stored::Part(value)),
        Some(_) => Err(TitoError::IndexError(format!(
            "Index key '{key}' holds a value that is no index entry"
        ))),
    }
}

fn encode_field(field: &TitoIndexField) -> String {
    match field {
        TitoIndexField::Text(value) => encode_index_string(value),
        TitoIndexField::Number(value) => encode_index_integer(*value),
    }
}

fn encode_fields(fields: &[TitoIndexField]) -> String {
    fields
        .iter()
        .map(encode_field)
        .collect::<Vec<_>>()
        .join(":")
}

pub(crate) fn validate_name(name: &str) -> Result<(), TitoError> {
    if name.is_empty() || name.contains(':') || name.contains('\\') {
        return Err(TitoError::IndexError(format!(
            "Index name '{name}' must be non-empty and hold no ':' or '\\'"
        )));
    }
    Ok(())
}

fn validate_values(name: &str, fields: &[TitoIndexField]) -> Result<(), TitoError> {
    for field in fields {
        if matches!(field, TitoIndexField::Text(value) if value.is_empty()) {
            return Err(TitoError::IndexError(format!(
                "Index '{name}' was handed an empty Text value"
            )));
        }
    }
    Ok(())
}

fn validate_index(index: &TitoIndex) -> Result<(), TitoError> {
    validate_name(index.name)?;
    if index.fields.is_empty() {
        return Err(TitoError::IndexError(format!(
            "Index '{}' has no field",
            index.name
        )));
    }
    validate_values(index.name, &index.fields)?;
    if index.unique && index.value != TitoIndexValue::Full {
        return Err(TitoError::IndexError(format!(
            "Unique index '{}' must hold Full copies",
            index.name
        )));
    }
    if matches!(index.value, TitoIndexValue::Part(paths) if paths.is_empty()) {
        return Err(TitoError::IndexError(format!(
            "Part index '{}' names no field",
            index.name
        )));
    }
    Ok(())
}

pub(crate) fn unique_key(name: &str, values: &[TitoIndexField]) -> Result<String, TitoError> {
    validate_name(name)?;
    if values.is_empty() {
        return Err(TitoError::IndexError(format!(
            "A lookup by unique index '{name}' needs its values"
        )));
    }
    validate_values(name, values)?;
    Ok(format!(
        "{INDEX_KEY_PREFIX}{name}:{}:",
        encode_fields(values)
    ))
}

pub(crate) fn entries<T: TitoModelTrait>(record: &T) -> Result<Vec<Entry>, TitoError> {
    let id = record.id();
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for index in record.indexes() {
        validate_index(&index)?;
        let values = encode_fields(&index.fields);
        let key = if index.unique {
            format!("{INDEX_KEY_PREFIX}{}:{values}:", index.name)
        } else {
            format!(
                "{INDEX_KEY_PREFIX}{}:{values}:{}:",
                index.name,
                encode_index_string(&id)
            )
        };
        if seen.insert(key.clone()) {
            entries.push(Entry {
                key,
                name: index.name,
                fields: index.fields,
                value: index.value,
                unique: index.unique,
            });
        }
    }
    Ok(entries)
}

pub(crate) fn entry_bytes(
    entry: &Entry,
    id: &str,
    json: &Value,
    record: &[u8],
) -> Result<Vec<u8>, TitoError> {
    match entry.value {
        TitoIndexValue::Full if json.is_object() => Ok(record.to_vec()),
        TitoIndexValue::Full => Err(TitoError::IndexError(format!(
            "Index '{}' keeps full copies, which needs a record stored as an object",
            entry.name
        ))),
        TitoIndexValue::KeysOnly => Ok(Vec::new()),
        TitoIndexValue::Part(paths) => part_bytes(paths, id, json),
    }
}

pub(crate) fn remembered_entry(entry: &Entry, value: &[u8]) -> RememberedEntry {
    let holds = match entry.value {
        TitoIndexValue::Full => Holds::Full,
        TitoIndexValue::KeysOnly => Holds::KeysOnly,
        TitoIndexValue::Part(_) => Holds::Part(value.to_vec()),
    };
    RememberedEntry {
        key: entry.key.clone(),
        holds,
    }
}

pub(crate) fn remembered_entries<T: TitoModelTrait>(
    record: &T,
    json: &Value,
) -> Result<Vec<RememberedEntry>, TitoError> {
    let id = record.id();
    let mut remembered = Vec::new();
    for entry in entries(record)? {
        let holds = match entry.value {
            TitoIndexValue::Full => Holds::Full,
            TitoIndexValue::KeysOnly => Holds::KeysOnly,
            TitoIndexValue::Part(paths) => Holds::Part(part_bytes(paths, &id, json)?),
        };
        remembered.push(RememberedEntry {
            key: entry.key,
            holds,
        });
    }
    Ok(remembered)
}

pub(crate) fn part_bytes(paths: &[&str], id: &str, json: &Value) -> Result<Vec<u8>, TitoError> {
    let mut part = Value::Object(Map::new());
    for path in paths {
        let steps = path.split('.').collect::<Vec<_>>();
        copy_path(json, &steps, &mut part);
    }
    serde_json::to_vec(&Value::Array(vec![Value::String(id.to_string()), part]))
        .map_err(|error| TitoError::SerializationFailed(error.to_string()))
}

fn copy_path(source: &Value, path: &[&str], target: &mut Value) {
    let Some((field, rest)) = path.split_first() else {
        return;
    };
    let Some(child) = source.get(*field) else {
        return;
    };
    let Value::Object(copied) = target else {
        return;
    };
    if rest.is_empty() {
        copied.insert(field.to_string(), child.clone());
        return;
    }
    match child {
        Value::Array(items) => {
            let slot = copied.entry(field.to_string()).or_insert_with(|| {
                Value::Array(items.iter().map(|_| Value::Object(Map::new())).collect())
            });
            if let Value::Array(copies) = slot {
                for (item, copy) in items.iter().zip(copies.iter_mut()) {
                    copy_path(item, rest, copy);
                }
            }
        }
        Value::Object(_) => {
            let slot = copied
                .entry(field.to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            copy_path(child, rest, slot);
        }
        Value::Null => {
            copied.entry(field.to_string()).or_insert(Value::Null);
        }
        _ => {}
    }
}

fn key_segments(key: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for character in key.chars() {
        if escaped {
            current.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == ':' {
            segments.push(std::mem::take(&mut current));
        } else {
            current.push(character);
        }
    }
    segments.push(current);
    segments
}

fn key_id(key: &str) -> String {
    let mut segments = key_segments(key);
    segments.pop();
    segments.pop().unwrap_or_default()
}

pub(crate) fn full_copy<T: TitoModelTrait + DeserializeOwned>(
    key: &str,
    value: &[u8],
) -> Result<T, TitoError> {
    copied_record(key, value, false)
}

pub(crate) fn unique_copy<T: TitoModelTrait + DeserializeOwned>(
    key: &str,
    value: &[u8],
) -> Result<T, TitoError> {
    copied_record(key, value, true)
}

fn copied_record<T: TitoModelTrait + DeserializeOwned>(
    key: &str,
    value: &[u8],
    unique: bool,
) -> Result<T, TitoError> {
    let bytes = match stored(key, value)? {
        Stored::Full(bytes) => bytes,
        Stored::KeysOnly => {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' is a KeysOnly entry and holds no record"
            )))
        }
        Stored::Part(_) => {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' is a Part entry; read it with execute_part"
            )))
        }
    };
    let record: T = serde_json::from_slice(bytes).map_err(|error| {
        TitoError::DeserializationFailed(format!(
            "Failed to deserialize the copied record of index key '{key}': {error}"
        ))
    })?;
    if !entries(&record)?.iter().any(|entry| {
        entry.key == key && entry.value == TitoIndexValue::Full && (entry.unique || !unique)
    }) {
        return Err(TitoError::IndexError(format!(
            "Copied record for index key '{key}' disagrees with its key"
        )));
    }
    Ok(record)
}

pub(crate) fn part_copy<P: DeserializeOwned>(key: &str, value: &[u8]) -> Result<P, TitoError> {
    let bytes = match stored(key, value)? {
        Stored::Part(bytes) => bytes,
        Stored::KeysOnly => {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' is a KeysOnly entry and holds no part"
            )))
        }
        Stored::Full(_) => {
            return Err(TitoError::IndexError(format!(
                "Index key '{key}' holds a full record; read it with execute"
            )))
        }
    };
    let (id, part): (String, P) = serde_json::from_slice(bytes).map_err(|error| {
        TitoError::DeserializationFailed(format!(
            "Failed to deserialize the part of index key '{key}': {error}"
        ))
    })?;
    if key_id(key) != id {
        return Err(TitoError::IndexError(format!(
            "Part for index key '{key}' disagrees with its key"
        )));
    }
    Ok(part)
}

pub(crate) fn entry_id<T: TitoModelTrait + DeserializeOwned>(
    key: &str,
    value: &[u8],
) -> Result<String, TitoError> {
    match stored(key, value)? {
        Stored::KeysOnly => Ok(key_id(key)),
        Stored::Part(bytes) => {
            let (id, _): (String, IgnoredAny) = serde_json::from_slice(bytes).map_err(|error| {
                TitoError::DeserializationFailed(format!(
                    "Failed to deserialize the part of index key '{key}': {error}"
                ))
            })?;
            if key_id(key) != id {
                return Err(TitoError::IndexError(format!(
                    "Part for index key '{key}' disagrees with its key"
                )));
            }
            Ok(id)
        }
        Stored::Full(_) => Ok(full_copy::<T>(key, value)?.id()),
    }
}

pub(crate) fn query_range(
    name: &str,
    values: &[TitoIndexField],
    lower: Option<&TitoIndexBound>,
    upper: Option<&TitoIndexBound>,
) -> Result<(String, String), TitoError> {
    validate_name(name)?;
    validate_values(name, values)?;
    let prefix = if values.is_empty() {
        format!("{INDEX_KEY_PREFIX}{name}:")
    } else {
        format!("{INDEX_KEY_PREFIX}{name}:{}:", encode_fields(values))
    };
    let start = match lower {
        None => prefix.clone(),
        Some(TitoIndexBound::Included(fields)) => {
            format!("{prefix}{}:", bound_fields(name, fields)?)
        }
        Some(TitoIndexBound::Excluded(fields)) => {
            prefix_end(format!("{prefix}{}:", bound_fields(name, fields)?))
        }
    };
    let end = match upper {
        None => prefix_end(prefix.clone()),
        Some(TitoIndexBound::Included(fields)) => {
            prefix_end(format!("{prefix}{}:", bound_fields(name, fields)?))
        }
        Some(TitoIndexBound::Excluded(fields)) => {
            format!("{prefix}{}:", bound_fields(name, fields)?)
        }
    };
    Ok((start, end))
}

fn bound_fields(name: &str, fields: &[TitoIndexField]) -> Result<String, TitoError> {
    if fields.is_empty() {
        return Err(TitoError::IndexError(format!(
            "A bound on index '{name}' needs at least one value"
        )));
    }
    validate_values(name, fields)?;
    Ok(encode_fields(fields))
}
