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

pub fn to_snake_case(s: &str) -> String {
    let mut snake_case = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        if ch.is_uppercase() && i != 0 && chars[i - 1].is_lowercase() {
            snake_case.push('_');
        }

        if ch.is_alphabetic() {
            snake_case.push(ch.to_lowercase().next().unwrap());
        } else {
            snake_case.push(ch);
        }
    }

    snake_case = snake_case
        .trim_matches(|c: char| c.is_whitespace() && c != '_')
        .to_string();

    snake_case
}

pub fn safe_encode(key: &str) -> String {
    let key = to_snake_case(key);

    let mut result = String::with_capacity(key.len() * 2);

    for ch in key.chars() {
        if ch == ':' || ch == '\\' {
            result.push('\\');
        }
        result.push(ch);
    }

    result
}
