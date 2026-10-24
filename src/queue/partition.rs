use crate::utils::fnv1a_64;

pub fn queue_partition_for_key(key: &str, partition_count: u32) -> u32 {
    (fnv1a_64(key.as_bytes()) % u64::from(partition_count.max(1))) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_key_hash_is_fnv_1a_64_over_the_utf8_bytes() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(
            fnv1a_64("store:4f6c1a52-7e0b-4f43-9d0c-2b4c1a9e8f10".as_bytes()),
            0x22b8_46cc_ff62_aa2b
        );
        assert_eq!(fnv1a_64("order:ž".as_bytes()), 0x0d8f_ca86_6173_a478);
    }

    #[test]
    fn queue_partitions_are_fixed_for_every_key_and_count() {
        assert_eq!(queue_partition_for_key("", 10), 7);
        assert_eq!(queue_partition_for_key("a", 1_024), 140);
        assert_eq!(queue_partition_for_key("foobar", 1_024), 1_000);
        assert_eq!(
            queue_partition_for_key("store:4f6c1a52-7e0b-4f43-9d0c-2b4c1a9e8f10", 10),
            3
        );
        assert_eq!(queue_partition_for_key("entry:due", 10), 9);
        assert_eq!(queue_partition_for_key("order:ž", 7), 5);
        assert_eq!(queue_partition_for_key("foobar", 1), 0);
        assert_eq!(queue_partition_for_key("foobar", 0), 0);
    }
}
