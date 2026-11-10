use crate::key_encoder::encode_index_string;
use crate::types::TitoGuard;
use std::collections::{BTreeMap, HashSet};

pub(crate) const FENCE_KEY_PREFIX: &str = "fence:";

pub(crate) const SHARED_FENCE_KEYS: u16 = 16;

#[derive(Default)]
pub(crate) struct Guards {
    records: BTreeMap<(String, String), TitoGuard>,
    written_keys: HashSet<Vec<u8>>,
}

fn fence_key_base(table: &str, id: &str) -> String {
    format!(
        "{FENCE_KEY_PREFIX}{}:{}",
        encode_index_string(table),
        encode_index_string(id)
    )
}

pub(crate) fn shared_fence_key(table: &str, id: &str, number: u16) -> String {
    format!("{}:{number}", fence_key_base(table, id))
}

pub(crate) fn exclusive_fence_key(table: &str, id: &str) -> String {
    format!("{}:exclusive", fence_key_base(table, id))
}

pub(crate) fn record_fence_keys(table: &str, id: &str) -> Vec<String> {
    let mut keys: Vec<String> = (0..SHARED_FENCE_KEYS)
        .map(|number| shared_fence_key(table, id, number))
        .collect();
    keys.push(exclusive_fence_key(table, id));
    keys
}

pub(crate) fn note_guard(guards: &mut Guards, table: &str, id: &str, guard: TitoGuard) {
    let held = guards
        .records
        .entry((table.to_string(), id.to_string()))
        .or_insert(guard);
    *held = (*held).max(guard);
}

pub(crate) fn note_write(guards: &mut Guards, key: &[u8]) {
    if key.starts_with(FENCE_KEY_PREFIX.as_bytes()) {
        guards.written_keys.insert(key.to_vec());
    }
}

pub(crate) fn guard_keys(guards: &Guards, wrote: bool, pick: u64) -> Vec<String> {
    let exclusive = guards
        .records
        .values()
        .any(|guard| *guard == TitoGuard::Exclusive);
    if !wrote && !exclusive {
        return Vec::new();
    }
    let number = (pick % u64::from(SHARED_FENCE_KEYS)) as u16;
    guards
        .records
        .iter()
        .filter_map(|((table, id), guard)| {
            let key = match guard {
                TitoGuard::Shared => shared_fence_key(table, id, number),
                TitoGuard::Exclusive => exclusive_fence_key(table, id),
            };
            (!guards.written_keys.contains(key.as_bytes())).then_some(key)
        })
        .collect()
}
