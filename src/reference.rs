use crate::types::TitoReference;
use crate::TitoError;

pub(crate) const MAX_REFERENCE_TABLE_BYTES: usize = 512;
pub(crate) const MAX_REFERENCE_ID_BYTES: usize = 512;
pub(crate) const MAX_REFERENCE_PATH_BYTES: usize = 2_048;
pub(crate) const REFERENCE_KEY_PREFIX: &str = "ref:";
pub(crate) const DELETING_MARKER_KEY_PREFIX: &str = "deleting:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReferenceKey {
    pub(crate) target_table: String,
    pub(crate) target_id: String,
    pub(crate) source_table: String,
    pub(crate) source_id: String,
}

pub(crate) fn encode_reference_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for character in value.chars() {
        if character == ':' || character == '\\' {
            encoded.push('\\');
        }
        encoded.push(character);
    }
    encoded
}

fn decode_reference_segments(encoded: &str) -> Option<Vec<String>> {
    let mut segments = vec![String::new()];
    let mut characters = encoded.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => match characters.next() {
                Some(escaped @ (':' | '\\')) => segments.last_mut()?.push(escaped),
                _ => return None,
            },
            ':' => segments.push(String::new()),
            other => segments.last_mut()?.push(other),
        }
    }
    Some(segments)
}

pub(crate) fn reference_key(
    target_table: &str,
    target_id: &str,
    source_table: &str,
    source_id: &str,
    path: &str,
) -> String {
    format!(
        "{REFERENCE_KEY_PREFIX}{}:{}:{}:{}:{}",
        encode_reference_segment(target_table),
        encode_reference_segment(target_id),
        encode_reference_segment(source_table),
        encode_reference_segment(source_id),
        encode_reference_segment(path)
    )
}

pub(crate) fn reference_target_prefix(table: &str, id: &str) -> String {
    format!(
        "{REFERENCE_KEY_PREFIX}{}:{}:",
        encode_reference_segment(table),
        encode_reference_segment(id)
    )
}

pub(crate) fn reference_source_prefix(
    target_table: &str,
    target_id: &str,
    source_table: &str,
) -> String {
    format!(
        "{}{}:",
        reference_target_prefix(target_table, target_id),
        encode_reference_segment(source_table)
    )
}

pub(crate) fn deleting_marker_key(table: &str, id: &str) -> String {
    format!(
        "{DELETING_MARKER_KEY_PREFIX}{}:{}",
        encode_reference_segment(table),
        encode_reference_segment(id)
    )
}

pub(crate) fn primary_key(table: &str, id: &str) -> String {
    format!("table:{table}:{id}")
}

pub(crate) fn parse_reference_key(key: &str) -> Option<ReferenceKey> {
    let segments = decode_reference_segments(key.strip_prefix(REFERENCE_KEY_PREFIX)?)?;
    let [target_table, target_id, source_table, source_id, path]: [String; 5] =
        segments.try_into().ok()?;
    if [&target_table, &target_id, &source_table, &source_id, &path]
        .iter()
        .any(|segment| segment.is_empty())
    {
        return None;
    }
    Some(ReferenceKey {
        target_table,
        target_id,
        source_table,
        source_id,
    })
}

pub(crate) fn validate_reference_id(id: &str) -> Result<(), TitoError> {
    if id.is_empty() || id.len() > MAX_REFERENCE_ID_BYTES {
        return Err(TitoError::InvalidInput(format!(
            "A referenced record id must be 1 to {MAX_REFERENCE_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_reference_table(table: &str) -> Result<(), TitoError> {
    if table.is_empty() || table.len() > MAX_REFERENCE_TABLE_BYTES {
        return Err(TitoError::InvalidInput(format!(
            "A source table must be 1 to {MAX_REFERENCE_TABLE_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_reference(reference: &TitoReference) -> Result<(), TitoError> {
    if reference.table.is_empty()
        || reference.table.len() > MAX_REFERENCE_TABLE_BYTES
        || reference.id.is_empty()
        || reference.id.len() > MAX_REFERENCE_ID_BYTES
        || reference.path.is_empty()
        || reference.path.len() > MAX_REFERENCE_PATH_BYTES
    {
        return Err(TitoError::InvalidInput(format!(
            "A reference needs a table of 1 to {MAX_REFERENCE_TABLE_BYTES} bytes, an id of 1 to {MAX_REFERENCE_ID_BYTES} bytes and a path of 1 to {MAX_REFERENCE_PATH_BYTES} bytes"
        )));
    }
    Ok(())
}
