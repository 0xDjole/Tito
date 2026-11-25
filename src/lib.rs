mod connect;
pub use connect::TiKV;

pub mod backend;
pub mod types;

mod base;

pub use base::{TitoModel, MAX_SAVE_BYTES};

mod read;

pub use read::{Batch, BatchAnswers, BatchRequests, Read};

mod utils;

pub mod queue;

pub mod query;

mod key_encoder;
pub use key_encoder::{encode_index_integer, encode_index_string};

mod fence;

mod error;

mod index;

#[cfg(test)]
mod tito_tests;

pub use error::TitoError;
pub use types::{
    Guard, TitoEngine, TitoIndex, TitoIndexBound, TitoIndexField, TitoIndexValue, TitoModelTrait,
    PARTITION_DIGITS,
};

pub use queue::{
    queue_partition_for_key, run_cluster_worker, ClusterPartitionAssignment, ClusterPartitions,
    ClusterWorkerConfig, ClusterWorkerNode, Queue, QueueConfig, QueueDeletePage, QueueEvent,
    QueueHandlerOutcome, QueueHandlerResult, QueueOwner, QueueScanPage, MAX_QUEUE_EVENT_BYTES,
    MAX_QUEUE_OWNER_COMPONENT_BYTES,
};

pub use utils::{key_after, prefix_end};
