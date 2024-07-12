use std::{collections::HashSet, sync::Arc};

use futures_util::{FutureExt, Stream};
use serde_json::{Map, Value};
use sled::{Db, IVec, Subscriber};
use thiserror::Error;

use crate::{
    message::Ref,
    schema::{Schema, SchemaItem, SchemaResolutionError},
};

// TODO: error context
#[derive(Debug, Error)]
pub enum ServerError {
    #[error("{}", .0)]
    SledError(#[from] sled::Error),
    #[error("{}", .0)]
    SchemaError(#[from] SchemaResolutionError),
    #[error("key not found")]
    KeyNotFound,
    #[error("extra key found")]
    ExtraKeyFound,
    #[error("schema mismatch")]
    SchemaMismatch,
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
            SchemaItem::Collection(_inner) => {
                let encoded_ref = self.schema.encode_ref(&key.0);
                let Some(value) = self.store.get(&encoded_ref)? else {
                    return Ok(Value::Object(Map::new()));
                };
                let keys: HashSet<String> = bincode::deserialize(value.as_ref())
                    .expect("collections are encoded via bincode");
                let mut result = Map::new();
                for child in keys {
                    let mut sub_key = key.clone();
                    sub_key.0.push(child.clone());
                    let sub_value = self.get(&sub_key)?;
                    result.insert(sub_key.0.pop().unwrap(), sub_value);
                }
                Ok(Value::Object(result))
            }
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

    // TODO: back off mutations if schema fails to match
    pub fn set(&self, key: &Ref, val: Value) -> Result<(), ServerError> {
        let schema = self.schema.resolve(&key.0)?;
        match schema {
            SchemaItem::Collection(_) => todo!("return error: can't set a collection?"),
            SchemaItem::Document(fields) => {
                let Value::Object(obj) = val else {
                    return Err(ServerError::SchemaMismatch);
                };
                for entry in obj.keys() {
                    if !fields.contains_key(entry) {
                        return Err(ServerError::ExtraKeyFound);
                    }
                }
                for field in fields.keys() {
                    if !obj.contains_key(field) {
                        return Err(ServerError::SchemaMismatch);
                    }
                }
                for (obj_key, obj_value) in obj {
                    let mut sub_key = key.clone();
                    sub_key.0.push(obj_key);
                    self.set(&sub_key, obj_value)?;
                }
            }
            SchemaItem::Scalar => {
                // TODO: ensure the document is initialized
                let Value::String(val) = val else {
                    return Err(ServerError::SchemaMismatch);
                };
                let encoded_ref = self.schema.encode_ref(&key.0);
                self.store.insert(&encoded_ref, val.as_bytes())?;
            }
        }

        if key.0.len() > 1 {
            let parent_ref = &key.0[..key.0.len() - 1];
            let parent_schema = self.schema.resolve(parent_ref)?;
            if let SchemaItem::Collection(_) = parent_schema {
                let encoded_collection_key = self.schema.encode_ref(parent_ref);
                let mut keys: HashSet<String> = self
                    .store
                    .get(&encoded_collection_key)?
                    .map(|collection_value| {
                        bincode::deserialize(collection_value.as_ref()).expect("keys are bincoded")
                    })
                    .unwrap_or(HashSet::new());
                if !keys.contains(key.0.last().unwrap()) {
                    keys.insert(key.0.last().unwrap().clone());
                    let keys_encoded = bincode::serialize(&keys).unwrap();
                    self.store.insert(&encoded_collection_key, keys_encoded)?;
                }
            }
        }

        Ok(())
    }

    pub fn remove(&self, key: &Ref) -> Result<(), ServerError> {
        // TODO: transactional
        let schema = self.schema.resolve(&key.0)?;
        match schema {
            SchemaItem::Collection(_inner) => {
                let encoded_ref = self.schema.encode_ref(&key.0);
                let Some(value) = self.store.get(&encoded_ref)? else {
                    return Err(ServerError::KeyNotFound);
                };
                let keys: HashSet<String> = bincode::deserialize(value.as_ref())
                    .expect("collections are encoded via bincode");
                for child in keys {
                    let mut sub_key = key.clone();
                    sub_key.0.push(child.clone());
                    self.remove(&sub_key)?;
                }
                self.store.remove(&encoded_ref)?;
            }
            SchemaItem::Document(fields) => {
                for field in fields.keys() {
                    let mut sub_key = key.clone();
                    sub_key.0.push(field.clone());
                    self.remove(&sub_key)?;
                }
            }
            SchemaItem::Scalar => {
                let encoded_ref = self.schema.encode_ref(&key.0);
                self.store.remove(&encoded_ref)?;
            }
        }
        if key.0.len() > 1 {
            let parent_ref = &key.0[..key.0.len() - 1];
            let parent_schema = self.schema.resolve(parent_ref)?;
            if let SchemaItem::Collection(_) = parent_schema {
                let encoded_collection_key = self.schema.encode_ref(parent_ref);
                let mut keys: HashSet<String> = self
                    .store
                    .get(&encoded_collection_key)?
                    .map(|collection_value| {
                        bincode::deserialize(collection_value.as_ref()).expect("keys are bincoded")
                    })
                    .unwrap_or(HashSet::new());
                keys.remove(key.0.last().unwrap());
                let keys_encoded = bincode::serialize(&keys).unwrap();
                self.store.insert(&encoded_collection_key, keys_encoded)?;
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
    use serde_json::{Map, Value};
    use sled::Config;

    use crate::{
        message::Ref,
        schema::{Schema, SchemaItem},
        server::Event,
    };

    use super::Server;

    #[test]
    fn values() {
        let server = document_server();
        let r = create_ref(&["hello", "world"]);
        server.set(&r, Value::String("value".to_string())).unwrap();
        assert_eq!(server.get(&r).unwrap().as_str().unwrap(), "value");
    }

    #[tokio::test]
    async fn subscription() {
        let server = document_server();
        let r = create_ref(&["hello", "world"]);

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

    #[test]
    fn set_object() {
        let server = document_server();
        let r = Ref(vec!["hello".to_string()]);
        let obj = map(&[("world", "1"), ("new york", "2")]);
        server.set(&r, obj).unwrap();

        assert_eq!(
            server.get(&create_ref(&["hello", "world"])).unwrap(),
            Value::String("1".to_string()),
        );
        assert_eq!(
            server.get(&create_ref(&["hello", "new york"])).unwrap(),
            Value::String("2".to_string()),
        );
    }

    #[test]
    fn get_object() {
        let server = document_server();
        let r = Ref(vec!["hello".to_string()]);
        let obj = map(&[("world", "1"), ("new york", "2")]);
        server.set(&r, obj.clone()).unwrap();
        let result_obj = server.get(&Ref(vec!["hello".to_string()])).unwrap();

        assert_eq!(obj, result_obj);
    }

    #[test]
    fn get_collection() {
        let server = collection_server();

        server
            .set(&create_ref(&["fruits", "apple"]), map(&[("color", "red")]))
            .unwrap();
        server
            .set(
                &create_ref(&["fruits", "banana"]),
                map(&[("color", "yellow")]),
            )
            .unwrap();
        server
            .set(
                &create_ref(&["fruits", "blueberry"]),
                map(&[("color", "purple")]),
            )
            .unwrap();
        let all_fruits = server.get(&create_ref(&["fruits"])).unwrap();
        assert_eq!(
            all_fruits,
            map(&[
                ("apple", map(&[("color", "red")])),
                ("banana", map(&[("color", "yellow")])),
                ("blueberry", map(&[("color", "purple")])),
            ])
        );
    }

    #[test]
    fn delete_document() {
        let server = collection_server();

        server
            .set(&create_ref(&["fruits", "apple"]), map(&[("color", "red")]))
            .unwrap();

        server.remove(&create_ref(&["fruits", "apple"])).unwrap();

        let all_fruits = server.get(&create_ref(&["fruits"])).unwrap();
        assert_eq!(all_fruits, Value::Object(Map::new()));
    }

    #[test]
    fn delete_collection() {
        let server = collection_server();

        server
            .set(&create_ref(&["fruits", "apple"]), map(&[("color", "red")]))
            .unwrap();
        server
            .set(
                &create_ref(&["fruits", "banana"]),
                map(&[("color", "yellow")]),
            )
            .unwrap();

        server.remove(&create_ref(&["fruits"])).unwrap();

        let all_fruits = server.get(&create_ref(&["fruits"])).unwrap();
        assert_eq!(all_fruits, Value::Object(Map::new()));
    }

    fn collection_server() -> Server {
        let db = Config::new()
            .temporary(true)
            .flush_every_ms(None)
            .open()
            .unwrap();

        let test_schema = Schema::new(SchemaItem::Document(
            [(
                "fruits".to_string(),
                SchemaItem::Collection(Box::new(SchemaItem::Document(
                    [("color".to_string(), SchemaItem::Scalar)]
                        .into_iter()
                        .collect(),
                ))),
            )]
            .into_iter()
            .collect(),
        ));

        Server {
            store: db,
            schema: Arc::new(test_schema),
        }
    }

    fn document_server() -> Server {
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

    fn create_ref(components: &[&str]) -> Ref {
        Ref(components
            .iter()
            .map(|component| component.to_string())
            .collect())
    }

    fn map<T: Clone + Into<Value>>(items: &[(&str, T)]) -> Value {
        Value::Object(
            items
                .iter()
                .map(|(key, value)| (key.to_string(), value.clone().into()))
                .collect(),
        )
    }
}
