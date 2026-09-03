/// Returns the exclusive byte-range endpoint for every key beginning with `prefix`.
///
/// This is deliberately different from [`key_after`]. A prefix endpoint advances
/// the last possible byte and truncates the suffix, while an after-key bound appends
/// a zero byte so that no keys between two adjacent-looking strings are skipped.
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

/// Returns an inclusive lower bound that sorts immediately after `key` itself.
///
/// Appending NUL is the correct continuation for a half-open scan: it excludes the
/// exact key while retaining keys such as `a20` after a cursor at `a2`.
pub fn key_after(mut key: String) -> String {
    key.push('\0');
    key
}

/// Historical name retained for source compatibility.
///
/// This function has prefix-range semantics. New code should call [`prefix_end`]
/// when closing a prefix range and [`key_after`] when continuing after an exact key.
pub fn next_string_lexicographically(payload: String) -> String {
    prefix_end(payload)
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
