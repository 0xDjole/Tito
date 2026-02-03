use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

/// App's event enum implements this trait to define event type metadata
pub trait EventType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    /// Queue name / event type identifier (e.g., "events")
    fn event_type_name() -> &'static str;

    /// Variant name for logging/routing (e.g., "order_created")
    fn variant_name(&self) -> &'static str;
}

/// App's handlers implement this trait to process events
#[async_trait]
pub trait EventHandler<E: EventType>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    type Context: Send + Sync + Clone;

    /// Handle the event
    async fn handle(
        &self,
        event: &super::QueueEvent<E>,
        ctx: &Self::Context,
    ) -> Result<(), Self::Error>;

    /// Filter which event variants this handler accepts (default: all)
    fn accepts(&self, _event: &E) -> bool {
        true
    }
}
