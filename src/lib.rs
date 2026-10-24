mod connect;
pub use connect::TiKV;

pub mod backend;
pub mod types;

mod base;

pub use base::{
    GetBuilder, GetManyBuilder, ReplaceManyBuilder, SetBuilder, TitoModel, MAX_REPLACE_MANY_BYTES,
    MAX_REPLACE_MANY_KEYS, MAX_REPLACE_MANY_RECORDS,
};

mod utils;

pub mod queue;

pub mod query;

mod key_encoder;
pub use key_encoder::{encode_index_integer, encode_index_string};

mod reference;

mod fence;

mod error;

mod event;
pub use event::TitoEvent;

pub mod index;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tito_tests;

pub use error::TitoError;
pub use types::{
    PartitionConfig, TitoEngine, TitoIncomingReference, TitoModelOptions, TitoModelTrait,
    TitoReference, PARTITION_DIGITS,
};

pub use queue::{
    queue_partition_for_key, run_cluster_worker, ClusterCoordinatorLease,
    ClusterPartitionAssignment, ClusterWorkerConfig, ClusterWorkerNode, Queue, QueueConfig,
    QueueDeletePage, QueueEvent, QueueEventStatus, QueueHandlerOutcome, QueueHandlerResult,
    QueueOwner, QueueScanPage, WorkerConfig, MAX_QUEUE_EVENT_BYTES,
    MAX_QUEUE_OWNER_COMPONENT_BYTES,
};

pub use utils::{key_after, prefix_end};
