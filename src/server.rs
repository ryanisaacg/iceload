use std::sync::Arc;

use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value};
use sled::{Db, IVec, Subscriber};
use thiserror::Error;

use crate::{
    message::Ref,
    schema::{Schema, SchemaItem, SchemaResolutionError},
};

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
    #[error("{}", .0)]
    SchemaError(#[from] SchemaResolutionError),
    #[error("key not found")]
    KeyNotFound,
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

    pub fn get(&self, key: &Ref) -> Result<Value, ServerError> {
        let schema = self.schema.resolve(&key.0)?;
        match schema {
            SchemaItem::Collection(_) => todo!("getting a collection"),
            SchemaItem::Document(fields) => {
                let mut values = Map::new();
                for field in fields.keys() {
                    let mut sub_key = key.clone();
                    sub_key.0.push(field.clone());
                    let sub_value = self.get(&sub_key)?;
                    values.insert(sub_key.0.pop().unwrap(), sub_value);
                }
                Ok(Value::Object(values))
            }
            SchemaItem::Scalar => {
                let encoded_ref = self.schema.encode_ref(&key.0);
                match self.store.get(&encoded_ref)? {
                    Some(val) => {
                        let val = val.to_vec();
                        let string = String::from_utf8(val).expect("string value");
                        Ok(Value::String(string))
                    }
                    None => Err(ServerError::KeyNotFound),
                }
            }
        }
    }

    pub fn set(&self, key: &Ref, val: Value) -> Result<(), ServerError> {
        let schema = dbg!(self.schema.resolve(&key.0)?);
        match schema {
            SchemaItem::Collection(_) => todo!("return error: can't set a collection?"),
            SchemaItem::Document(_) => todo!(),
            SchemaItem::Scalar => {
                let Value::String(val) = val else { todo!() };
                let encoded_ref = self.schema.encode_ref(&key.0);
                self.store.insert(&encoded_ref, val.as_bytes())?;
            }
        }

        Ok(())
    }

    pub fn remove(&self, key: &Ref) -> Result<(), ServerError> {
        let schema = self.schema.resolve(&key.0)?;
        match schema {
            SchemaItem::Collection(_) => todo!("return error: can't remove a collection?"),
            SchemaItem::Document(_) => todo!(),
            SchemaItem::Scalar => {
                let encoded_ref = self.schema.encode_ref(&key.0);
                self.store.remove(&encoded_ref)?;
            }
        }
        Ok(())
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
    use serde_json::Value;
    use sled::Config;

    use crate::{
        message::Ref,
        schema::{Schema, SchemaItem},
        server::Event,
    };

    use super::Server;

    fn test_server() -> Server {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let test_schema = Schema::new(SchemaItem::Document(
            [(
                "hello".to_string(),
                SchemaItem::Document(
                    [
                        ("world".to_string(), SchemaItem::Scalar),
                        ("new york".to_string(), SchemaItem::Scalar),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )]
            .into_iter()
            .collect(),
        ));

        Server {
            store: db,
            schema: Arc::new(test_schema),
        }
    }

    #[test]
    fn values() {
        let server = test_server();
        let r = Ref(vec!["hello".to_string(), "world".to_string()]);
        server.set(&r, Value::String("value".to_string())).unwrap();
        assert_eq!(server.get(&r).unwrap().as_str().unwrap(), "value");
    }

    #[tokio::test]
    async fn subscription() {
        let server = test_server();
        let r = Ref(vec!["hello".to_string(), "world".to_string()]);

        let mut subscription = server.subscribe(&r);

        let count_up_to = 5;

        let write_server = server.clone();
        let r_ = r.clone();
        let handle = tokio::spawn(async move {
            for i in 0..count_up_to {
                write_server.set(&r_, Value::String(i.to_string())).unwrap();
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
