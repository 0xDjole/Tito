pub fn encode_index_integer(value: i64) -> String {
    let ordered = (value as u64) ^ (1_u64 << 63);
    format!("{ordered:020}")
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
