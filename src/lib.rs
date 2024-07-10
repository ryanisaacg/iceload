use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sled::{Db, Subscriber};
use thiserror::Error;
use tokio::sync::mpsc::{Receiver, Sender};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
}

#[derive(Clone)]
pub struct Server {
    store: Db,
}

impl Server {
    pub fn open(path: &str) -> Result<Server, Error> {
        let store = sled::open(path)?;
        Ok(Server { store })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, Error> {
        Ok(match self.store.get(key.as_bytes())? {
            Some(val) => {
                let val = val.to_vec();
                let string = String::from_utf8(val).expect("string value");
                Some(string)
            }
            None => None,
        })
    }

    pub fn set(&self, key: &str, val: Option<&str>) -> Result<Option<String>, Error> {
        let val = match val {
            Some(val) => self.store.insert(key.as_bytes(), val.as_bytes())?,
            None => self.store.remove(key.as_bytes())?,
        };
        Ok(val.map(|val| String::from_utf8(val.to_vec()).expect("string value")))
    }

    pub fn subscribe(&self, key: &str) -> Subscriber {
        self.store.watch_prefix(key.as_bytes())
    }
}

pub struct ClientManager {
    next: u64,
    resp_senders: HashMap<ClientId, Sender<ServerMessage>>,
}

impl ClientManager {
    pub fn new() -> ClientManager {
        ClientManager {
            next: 0,
            resp_senders: HashMap::new(),
        }
    }

    pub fn new_client(&mut self) -> (ClientId, Receiver<ServerMessage>) {
        let id = ClientId(self.next);
        self.next += 1;
        let (send, recv) = tokio::sync::mpsc::channel(64);
        self.resp_senders.insert(id, send);

        (id, recv)
    }

    pub fn sender(&self, client_id: ClientId) -> &Sender<ServerMessage> {
        &self.resp_senders[&client_id]
    }

    pub fn remove(&mut self, client_id: ClientId) {
        self.resp_senders.remove(&client_id);
    }
}

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq)]
pub struct ClientId(u64);

// TODO: should reads / writes be over the websocket or in a different band?

pub struct ClientMessage {
    pub id: ClientId,
    pub msg: ClientMessageParams,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ClientMessageParams {
    Get(String),
    Set(String, Option<String>),
    Subscribe(String),
    Unsubscribe(String),
    Disconnect,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerMessage {
    Value(Option<String>),
    ValueChanged(String, Option<String>),
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
