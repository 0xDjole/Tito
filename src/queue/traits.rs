use serde::{de::DeserializeOwned, Serialize};

/// App's event enum implements this trait to define event type metadata
pub trait EventType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// Queue name / event type identifier (e.g., "events")
    fn event_type_name() -> &'static str;

    /// Variant name for logging/routing (e.g., "order_created")
    fn variant_name(&self) -> &'static str;
}
