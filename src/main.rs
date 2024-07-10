use std::collections::HashMap;

use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{self, Error},
};

mod message;
use message::{ClientMessage, ServerMessage};
mod server;
use server::Server;

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:9002";
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");

    let server = Server::open("data").unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(client_task(server.clone(), stream));
    }
}

async fn client_task(server: Server, stream: TcpStream) -> anyhow::Result<()> {
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
        println!("{msg:?}");
        let msg = match msg {
            Ok(msg) => msg,
            Err(Error::ConnectionClosed) => break,
            Err(err) => return Err(err.into()),
        };
        let msg = msg.to_text()?;
        let msg: ClientMessage = serde_json::from_str(msg)?;
        match msg {
            ClientMessage::Get(key) => {
                let value = server.get(key.as_str()).unwrap();
                println!("Get result {value:?}");
                send_resp.send(ServerMessage::Value(value)).unwrap();
            }
            ClientMessage::Set(key, value) => {
                let value = server.set(key.as_str(), value.as_deref()).unwrap();
                println!("Set result {value:?}");
                send_resp.send(ServerMessage::Value(value)).unwrap();
            }
            ClientMessage::Subscribe(key) => {
                let mut subscriber = server.subscribe(key.as_str());
                let sender = send_resp.clone();
                let key_ = key.clone();
                let handle = tokio::spawn(async move {
                    while let Some(event) = subscriber.next().await {
                        match event {
                            sled::Event::Insert { key: _, value } => {
                                let value = String::from_utf8(value.to_vec()).unwrap();
                                sender
                                    .send(ServerMessage::ValueChanged(key_.clone(), Some(value)))
                                    .unwrap();
                            }
                            sled::Event::Remove { key: _ } => {
                                sender
                                    .send(ServerMessage::ValueChanged(key_.clone(), None))
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
            ClientMessage::Disconnect => {
                // TODO: unsubscribe from all
                todo!()
            }
        }
    }

    send_task.abort();
    for subscriber in subscriptions.values() {
        subscriber.abort();
    }

    Ok(())
}
