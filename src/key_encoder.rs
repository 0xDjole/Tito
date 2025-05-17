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
