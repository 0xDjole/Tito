use crate::key_encoder::encode_index_string;
use crate::types::TitoGuard;
use std::collections::BTreeMap;

pub(crate) const FENCE_KEY_PREFIX: &str = "fence:";

pub(crate) const SHARED_FENCE_KEYS: u16 = 16;

pub(crate) type Guards = BTreeMap<(String, String), TitoGuard>;

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
        .entry((table.to_string(), id.to_string()))
        .or_insert(guard);
    *held = (*held).max(guard);
}

pub(crate) fn guard_keys(guards: &Guards, wrote: bool, pick: u64) -> Vec<String> {
    let protecting = guards
        .values()
        .any(|guard| matches!(guard, TitoGuard::Exclusive | TitoGuard::ForUpdate));
    if !wrote && !protecting {
        return Vec::new();
    }
    let number = (pick % u64::from(SHARED_FENCE_KEYS)) as u16;
    let mut keys = Vec::new();
    for ((table, id), guard) in guards {
        match guard {
            TitoGuard::Shared => keys.push(shared_fence_key(table, id, number)),
            TitoGuard::Exclusive => keys.push(exclusive_fence_key(table, id)),
            TitoGuard::ForUpdate => keys.extend(record_fence_keys(table, id)),
            TitoGuard::Written => {}
        }
    }
    keys
}
