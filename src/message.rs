use serde::{Deserialize, Serialize};

// TODO: should reads / writes be over the websocket or in a different band?

#[derive(Debug, Deserialize, Serialize)]
pub enum ClientMessage {
    Get(Ref),
    Set(Ref, Option<String>),
    Subscribe(Ref),
    Unsubscribe(Ref),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerMessage {
    Value(Option<String>),
    ValueChanged(Ref, Option<String>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct Ref(pub Vec<RefComponent>);

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum RefComponent {
    #[serde(rename = "col")]
    Collection(String),
    #[serde(rename = "doc")]
    Document(String),
}
