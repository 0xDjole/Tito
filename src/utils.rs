use uuid::Uuid;

pub fn next_string_lexicographically(payload: String) -> String {
    let mut prefix_end = payload.as_bytes().to_vec();

    if let Some(last_byte) = prefix_end.last_mut() {
        if *last_byte < 0xFF {
            *last_byte += 1; // Increment the last byte if it's less than 0xFF
        } else {
            prefix_end.push(0x00); // Otherwise, append 0x00
        }
    }

    String::from_utf8_lossy(&prefix_end).to_string()
}

pub fn to_snake_case(s: &str) -> String {
    let mut snake_case = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, &ch) in chars.iter().enumerate() {
        // Insert an underscore before uppercase letters that are not at the start
        // and only if the previous character is a lowercase letter
        if ch.is_uppercase() && i != 0 && chars[i - 1].is_lowercase() {
            snake_case.push('_');
        }

        // Convert alphabetic characters to lowercase, preserve all other characters including underscores
        if ch.is_alphabetic() {
            snake_case.push(ch.to_lowercase().next().unwrap());
        } else {
            // Keep original character (including underscores, numbers, etc)
            snake_case.push(ch);
        }
    }

    // Only trim non-underscore whitespace characters from ends
    snake_case = snake_case
        .trim_matches(|c: char| c.is_whitespace() && c != '_')
        .to_string();

    snake_case
}

pub fn next_uuid(current: &str) -> String {
    // Parse the current UUID
    if let Ok(uuid) = Uuid::parse_str(current) {
        // Convert to bytes (big-endian)
        let bytes = uuid.as_bytes();
        let mut next_bytes = bytes.to_vec();

        // Increment the bytes as a big-endian number
        for byte in next_bytes.iter_mut().rev() {
            if *byte == 0xff {
                *byte = 0;
                continue;
            }
            *byte += 1;
            break;
        }

        // Create new UUID from the incremented bytes
        if let Ok(next_uuid) = Uuid::from_slice(&next_bytes) {
            return next_uuid.to_string();
        }
    }

    // Fallback: return a maximum UUID if parsing fails
    "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string()
}

pub fn previous_string_lexicographically(payload: String) -> String {
    let mut prefix_end = payload.as_bytes().to_vec();

    if let Some(last_byte) = prefix_end.last_mut() {
        if *last_byte > 0x00 {
            *last_byte -= 1; // Decrement the last byte if it's greater than 0x00
        } else {
            prefix_end.pop(); // Otherwise, remove the last byte if it is 0x00
        }
    }

    String::from_utf8_lossy(&prefix_end).to_string()
}
