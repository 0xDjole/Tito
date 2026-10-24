use crate::key_encoder::encode_index_string;
use crate::utils::fnv1a_64;

pub(crate) const FENCE_STRIPE_KEY_PREFIX: &str = "fence:";

pub(crate) const LINK_FENCE_STRIPES: u16 = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FenceStripes {
    Record,
    Striped(u16),
}

impl FenceStripes {
    pub(crate) fn record_keys(self, table: &str, id: &str) -> Vec<String> {
        match self {
            Self::Record => Vec::new(),
            Self::Striped(stripes) => fence_stripe_keys(table, id, stripes),
        }
    }

    pub(crate) fn marker_keys(self, table: &str, id: &str) -> Vec<String> {
        match self {
            Self::Record => fence_stripe_keys(table, id, LINK_FENCE_STRIPES),
            Self::Striped(stripes) => fence_stripe_keys(table, id, stripes.max(LINK_FENCE_STRIPES)),
        }
    }
}

fn fence_stripe_keys(table: &str, id: &str, stripes: u16) -> Vec<String> {
    (0..stripes)
        .map(|stripe| fence_stripe_key(table, id, stripe))
        .collect()
}

pub(crate) fn fence_stripe_key(table: &str, id: &str, stripe: u16) -> String {
    format!(
        "{FENCE_STRIPE_KEY_PREFIX}{}:{}:{stripe}",
        encode_index_string(table),
        encode_index_string(id)
    )
}

pub(crate) fn source_fence_stripe(source_table: &str, source_id: &str, stripes: u16) -> u16 {
    let source = format!(
        "{}:{}",
        encode_index_string(source_table),
        encode_index_string(source_id)
    );
    (fnv1a_64(source.as_bytes()) % u64::from(stripes)) as u16
}

pub(crate) fn link_fence_key(
    target_table: &str,
    target_id: &str,
    source_table: &str,
    source_id: &str,
) -> String {
    fence_stripe_key(
        target_table,
        target_id,
        source_fence_stripe(source_table, source_id, LINK_FENCE_STRIPES),
    )
}

pub(crate) fn shared_fence_stripe(pick: u64, stripes: u16) -> u16 {
    (pick % u64::from(stripes)) as u16
}
