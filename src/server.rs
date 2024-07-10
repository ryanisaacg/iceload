use futures_util::{FutureExt, Stream};
use sled::{Db, Event, Subscriber};
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

    pub fn subscribe(&self, key: &str) -> SubscriptionStream {
        SubscriptionStream {
            sub: self.store.watch_prefix(key.as_bytes()),
        }
    }
}

pub struct SubscriptionStream {
    sub: Subscriber,
}

impl Stream for SubscriptionStream {
    type Item = Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.sub.poll_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use sled::{Config, Event};

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

    #[tokio::test]
    async fn subscription() {
        let server = test_server();

        let mut subscription = server.subscribe("value");

        let count_up_to = 5;

        let write_server = server.clone();
        let handle = tokio::spawn(async move {
            for i in 0..count_up_to {
                write_server
                    .set("value", Some(i.to_string().as_str()))
                    .unwrap();
            }
        });

        let mut expected = 0;
        while let Some(event) = subscription.next().await {
            let Event::Insert { key, value } = event else {
                panic!("expected insert event");
            };
            assert_eq!(std::str::from_utf8(key.as_ref()), Ok("value"));
            assert_eq!(String::from_utf8(value.to_vec()), Ok(expected.to_string()));

            expected += 1;
            if expected >= count_up_to {
                break;
            }
        }

        handle.await.unwrap();
    }
}
