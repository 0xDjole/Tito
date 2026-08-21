mod connect;
pub use connect::TiKV;

pub mod backend;
pub mod types;

mod base;

pub use base::{GetBuilder, GetManyBuilder, SetBuilder, TitoModel};

mod utils;

pub mod queue;

pub mod query;

mod key_encoder;

mod error;

mod event;
pub use event::TitoEvent;

pub mod index;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tito_tests;

pub use error::TitoError;
pub use types::{PartitionConfig, TitoEngine, TitoModelOptions, TitoModelTrait, PARTITION_DIGITS};

pub use queue::{
    run_cluster_worker, ClusterCoordinatorLease, ClusterPartitionAssignment, ClusterWorkerConfig,
    ClusterWorkerNode, Queue, QueueConfig, QueueDeletePage, QueueEvent, QueueEventState,
    QueueHandlerOutcome, QueueHandlerResult, QueueOwner, QueueScanPage, WorkerConfig,
    MAX_QUEUE_EVENT_BYTES, MAX_QUEUE_OWNER_COMPONENT_BYTES,
};

pub use utils::{key_after, next_string_lexicographically, prefix_end};
