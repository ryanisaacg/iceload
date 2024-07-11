use serde::{Deserialize, Serialize};
use serde_json::Value;

// TODO: should reads / writes be over the websocket or in a different band?

#[derive(Debug, Deserialize, Serialize)]
pub enum ClientMessage {
    Get(Ref),
    Set(Ref, Value),
    Remove(Ref),
    Subscribe(Ref),
    Unsubscribe(Ref),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerMessage {
    Value(Value),
    Error(String),
    SubscriptionUpdate(Ref, Option<String>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct Ref(pub Vec<RefComponent>);

pub type RefComponent = String;
