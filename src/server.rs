use sled::{Db, Subscriber};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
}

#[derive(Clone)]
pub struct Server {
    store: Db,
}

impl Server {
    pub fn open(path: &str) -> Result<Server, ServerError> {
        let store = sled::open(path)?;
        Ok(Server { store })
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, ServerError> {
        Ok(match self.store.get(key.as_bytes())? {
            Some(val) => {
                let val = val.to_vec();
                let string = String::from_utf8(val).expect("string value");
                Some(string)
            }
            None => None,
        })
    }

    pub fn set(&self, key: &str, val: Option<&str>) -> Result<Option<String>, ServerError> {
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

#[cfg(test)]
mod tests {
    use sled::Config;

    use super::Server;

    fn test_server() -> Server {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        Server { store: db }
    }

    #[test]
    fn values() {
        let server = test_server();
        server.set("hello", Some("world")).unwrap();
        assert_eq!(server.get("hello").unwrap(), Some("world".to_string()));
    }
}
