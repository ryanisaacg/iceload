use serde::{Deserialize, Serialize};

// TODO: should reads / writes be over the websocket or in a different band?

#[derive(Debug, Deserialize, Serialize)]
pub enum ClientMessage {
    Get(String),
    Set(String, Option<String>),
    Subscribe(String),
    Unsubscribe(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerMessage {
    Value(Option<String>),
    ValueChanged(String, Option<String>),
}
