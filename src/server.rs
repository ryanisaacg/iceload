use std::sync::Arc;

use futures_util::{FutureExt, Stream};
use sled::{Db, IVec, Subscriber};
use thiserror::Error;

use crate::{message::Ref, schema::Schema};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
}

#[derive(Clone)]
pub struct Server {
    store: Db,
    schema: Arc<Schema>,
}

impl Server {
    // TODO: read the schema out of the store
    pub fn open(path: &str, schema: Schema) -> Result<Server, ServerError> {
        let store = sled::open(path)?;
        Ok(Server {
            store,
            schema: Arc::new(schema),
        })
    }

    pub fn get(&self, key: &Ref) -> Result<Option<String>, ServerError> {
        let encoded_ref = self.schema.encode_ref(&key.0);
        Ok(match self.store.get(&encoded_ref)? {
            Some(val) => {
                let val = val.to_vec();
                let string = String::from_utf8(val).expect("string value");
                Some(string)
            }
            None => None,
        })
    }

    pub fn set(&self, key: &Ref, val: Option<&str>) -> Result<Option<String>, ServerError> {
        let encoded_ref = self.schema.encode_ref(&key.0);
        let val = match val {
            Some(val) => self.store.insert(&encoded_ref, val.as_bytes())?,
            None => self.store.remove(&encoded_ref)?,
        };
        Ok(val.map(|val| String::from_utf8(val.to_vec()).expect("string value")))
    }

    pub fn subscribe(&self, key: &Ref) -> SubscriptionStream {
        let encoded_ref = self.schema.encode_ref(&key.0);
        SubscriptionStream {
            sub: self.store.watch_prefix(&encoded_ref),
            schema: self.schema.clone(),
        }
    }
}

pub struct SubscriptionStream {
    sub: Subscriber,
    schema: Arc<Schema>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A new complete (key, value) pair
    Insert {
        /// The key that has been set
        key: Ref,
        /// The value that has been set
        value: IVec,
    },
    /// A deleted key
    Remove {
        /// The key that has been removed
        key: Ref,
    },
}

impl Stream for SubscriptionStream {
    type Item = Event;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.sub.poll_unpin(cx).map(|evt| {
            evt.map(|evt| match evt {
                sled::Event::Insert { key, value } => Event::Insert {
                    key: Ref(self.schema.decode_ref(key.as_ref())),
                    value,
                },
                sled::Event::Remove { key } => Event::Remove {
                    key: Ref(self.schema.decode_ref(key.as_ref())),
                },
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::StreamExt;
    use sled::Config;

    use crate::{
        message::{Ref, RefComponent},
        schema::Schema,
        server::Event,
    };

    use super::Server;

    fn test_server() -> Server {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        Server {
            store: db,
            schema: Arc::new(Schema::empty()),
        }
    }

    #[test]
    fn values() {
        let server = test_server();
        let r = Ref(vec![RefComponent::Document("hello".to_string())]);
        server.set(&r, Some("world")).unwrap();
        assert_eq!(server.get(&r).unwrap(), Some("world".to_string()));
    }

    #[tokio::test]
    async fn subscription() {
        let server = test_server();
        let r = Ref(vec![RefComponent::Document("value".to_string())]);

        let mut subscription = server.subscribe(&r);

        let count_up_to = 5;

        let write_server = server.clone();
        let r_ = r.clone();
        let handle = tokio::spawn(async move {
            for i in 0..count_up_to {
                write_server.set(&r_, Some(i.to_string().as_str())).unwrap();
            }
        });

        let mut expected = 0;
        while let Some(event) = subscription.next().await {
            let Event::Insert { key, value } = event else {
                panic!("expected insert event");
            };
            assert_eq!(&key, &r);
            assert_eq!(String::from_utf8(value.to_vec()), Ok(expected.to_string()));

            expected += 1;
            if expected >= count_up_to {
                break;
            }
        }

        handle.await.unwrap();
    }
}
