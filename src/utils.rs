pub fn prefix_end(prefix: String) -> String {
    let mut chars = prefix.chars().collect::<Vec<_>>();

    while let Some(character) = chars.pop() {
        let mut next = character as u32 + 1;
        while next <= char::MAX as u32 {
            if let Some(next_character) = char::from_u32(next) {
                chars.push(next_character);
                return chars.into_iter().collect();
            }
            next += 1;
        }
    }

    String::new()
}

pub fn key_after(mut key: String) -> String {
    key.push('\0');
    key
}

pub(crate) fn prefix_end_bytes(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();

    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }

    None
}

pub(crate) fn key_after_bytes(key: &[u8]) -> Vec<u8> {
    let mut after = Vec::with_capacity(key.len() + 1);
    after.extend_from_slice(key);
    after.push(0);
    after
}
