use crate::TitoError;
use serde_json::Value;

pub fn encode_index_integer(value: i64) -> String {
    let ordered = (value as u64) ^ (1_u64 << 63);
    format!("{ordered:020}")
}

pub(crate) fn encode_index_number(value: &Value) -> Result<Option<String>, TitoError> {
    if !value.is_number() {
        return Ok(None);
    }

    value
        .as_i64()
        .map(encode_index_integer)
        .map(Some)
        .ok_or_else(|| {
            TitoError::InvalidInput("Number indexes require a signed 64-bit integer".to_string())
        })
}

pub(crate) fn encode_index_integer_query(value: &str) -> Result<String, TitoError> {
    let integer = value
        .parse::<i64>()
        .ok()
        .filter(|integer| integer.to_string() == value)
        .ok_or_else(|| {
            TitoError::InvalidInput(
                "Number index queries require a canonical signed 64-bit decimal integer"
                    .to_string(),
            )
        })?;
    Ok(encode_index_integer(integer))
}

pub fn encode_index_string(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for character in value.chars() {
        if character == ':' || character == '\\' {
            encoded.push('\\');
        }
        encoded.push(character);
    }

    encoded
}
