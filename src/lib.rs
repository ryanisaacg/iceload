use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use sled::{Db, IVec, Subscriber};
use thiserror::Error;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    RwLock,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
}

pub struct Server {
    db: Database,
    subscribers: HashMap<String, Vec<ClientId>>,

    clients: Arc<RwLock<ClientManager>>,
}

impl Server {
    pub fn open(clients: Arc<RwLock<ClientManager>>, path: &str) -> Result<Server, Error> {
        todo!()
    }

    fn write(&mut self) {}
}

struct Client {}

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
    Set(String, String),
    Subscribe(String),
    Disconnect,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum ServerMessage {
    Value(String),
    ValueChanged(String, String),
}

pub struct Database {
    store: Db,
}

impl Database {
    pub fn open(path: &str) -> Result<Database, Error> {
        let store = sled::open(path)?;
        Ok(Database { store })
    }

    pub fn read(&self, key: &str) -> Result<Option<IVec>, Error> {
        Ok(self.store.get(key.as_bytes())?)
    }

    pub fn write(&self, key: &str, val: &str) -> Result<(), Error> {
        self.store.insert(key.as_bytes(), val.as_bytes())?;
        Ok(())
    }

    pub fn subscribe(&self, key: &str) -> Subscriber {
        self.store.watch_prefix(key.as_bytes())
    }
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
