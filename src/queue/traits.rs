use serde::{de::DeserializeOwned, Serialize};

pub trait EventType: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    fn event_type_name() -> &'static str;
    fn variant_name(&self) -> &'static str;
}
