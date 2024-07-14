use std::collections::HashMap;

use futures_util::{SinkExt, StreamExt};
use schema::{Schema, SchemaItem};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{self, Error},
};

mod message;
use message::{ClientMessage, ServerMessage};
mod permission;
mod schema;
mod server;
use server::Server;

use crate::{
    permission::{Operation, Permissions},
    server::Event,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = "127.0.0.1:9002";
    let listener = TcpListener::bind(&addr).await?;

    let source = std::fs::read_to_string("permission.luau")?;
    let permission_bytecode = Permissions::load_bytecode(&source)?;

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

    let server = Server::open("data", test_schema)?;

    while let Ok((stream, _)) = listener.accept().await {
        let server = server.clone();
        tokio::spawn(async move {
            client_task(server, stream, permission_bytecode)
                .await
                .unwrap()
        });
    }

    Ok(())
}

async fn client_task(
    server: Server,
    stream: TcpStream,
    permission_bytecode: &[u8],
) -> anyhow::Result<()> {
    let permissions = Permissions::new(permission_bytecode);

    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    let (mut ws_send, mut ws_recv) = ws_stream.split();

    let (send_resp, mut recv_resp) = tokio::sync::mpsc::unbounded_channel();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = recv_resp.recv().await {
            let resp_str = serde_json::to_string(&msg).unwrap();
            ws_send
                .send(tungstenite::Message::Text(resp_str))
                .await
                .unwrap();
        }
    });

    let mut subscriptions = HashMap::new();

    while let Some(msg) = ws_recv.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(Error::ConnectionClosed) => break,
            Err(err) => return Err(err.into()),
        };
        let msg = msg.to_text()?;
        let msg: ClientMessage = serde_json::from_str(msg)?;
        match msg {
            ClientMessage::Get(key) => {
                if !permissions.check(Operation::Read, &key)? {
                    send_resp.send(ServerMessage::Error("permissions".into()))?;
                }
                let value = server.get(&key).unwrap();
                println!("Get result {value:?}");
                send_resp.send(ServerMessage::Value(value)).unwrap();
            }
            ClientMessage::Insert(key, value) => {
                if !permissions.check(Operation::Insert, &key)? {
                    send_resp.send(ServerMessage::Error("permissions".into()))?;
                }
                match server.insert(&key, value) {
                    Ok(_) => send_resp.send(ServerMessage::Value(Value::Null)).unwrap(),
                    Err(e) => send_resp
                        .send(ServerMessage::Error(format!("{e}")))
                        .unwrap(),
                }
            }
            ClientMessage::Update(key, value) => {
                if !permissions.check(Operation::Update, &key)? {
                    send_resp.send(ServerMessage::Error("permissions".into()))?;
                }
                match server.update(&key, value) {
                    Ok(_) => send_resp.send(ServerMessage::Value(Value::Null)).unwrap(),
                    Err(e) => send_resp
                        .send(ServerMessage::Error(format!("{e}")))
                        .unwrap(),
                }
            }
            ClientMessage::Remove(key) => {
                if !permissions.check(Operation::Remove, &key)? {
                    send_resp.send(ServerMessage::Error("permissions".into()))?;
                }
                match server.remove(&key) {
                    Ok(_) => send_resp.send(ServerMessage::Value(Value::Null)).unwrap(),
                    Err(e) => send_resp
                        .send(ServerMessage::Error(format!("{e}")))
                        .unwrap(),
                }
            }
            ClientMessage::Subscribe(key) => {
                if !permissions.check(Operation::Read, &key)? {
                    send_resp.send(ServerMessage::Error("permissions".into()))?;
                }
                let mut subscriber = server.subscribe(&key);
                let sender = send_resp.clone();
                let key_ = key.clone();
                let handle = tokio::spawn(async move {
                    while let Some(event) = subscriber.next().await {
                        match event {
                            Event::Insert { key: _, value } => {
                                let value = String::from_utf8(value.to_vec()).unwrap();
                                sender
                                    .send(ServerMessage::SubscriptionUpdate(
                                        key_.clone(),
                                        Some(value),
                                    ))
                                    .unwrap();
                            }
                            Event::Remove { key: _ } => {
                                sender
                                    .send(ServerMessage::SubscriptionUpdate(key_.clone(), None))
                                    .unwrap();
                            }
                        }
                    }
                });
                subscriptions.insert(key, handle);
            }
            ClientMessage::Unsubscribe(key) => {
                if let Some(handle) = subscriptions.get(&key) {
                    handle.abort();
                }
            }
        }
    }

    send_task.abort();
    for subscriber in subscriptions.values() {
        subscriber.abort();
    }

    Ok(())
}
