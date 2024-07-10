use std::sync::Arc;

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use iceload::{ClientId, ClientManager, ClientMessage, ClientMessageParams, Server, ServerMessage};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{self, Error, Message},
    WebSocketStream,
};

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:9002";
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");

    let (request_send, request_recv) = tokio::sync::mpsc::channel(1024);
    let clients = Arc::new(RwLock::new(ClientManager::new()));

    let server = Server::open("data").unwrap();
    tokio::spawn(server_task(request_recv, clients.clone(), server));

    while let Ok((stream, _)) = listener.accept().await {
        let mut client_writer = clients.write().await;
        let (client_id, resp_recv) = client_writer.new_client();

        let request_send = request_send.clone();
        tokio::spawn(client_task(client_id, request_send, resp_recv, stream));
    }
}

async fn server_task(
    mut recv: Receiver<ClientMessage>,
    clients: Arc<RwLock<ClientManager>>,
    server: Server,
) {
    loop {
        let Some(ClientMessage { id, msg }) = recv.recv().await else {
            break;
        };
        match msg {
            ClientMessageParams::Get(key) => {
                let value = server.get(key.as_str()).unwrap();
                println!("Get result {value:?}");
                clients
                    .read()
                    .await
                    .sender(id)
                    .send(ServerMessage::Value(value))
                    .await
                    .unwrap();
            }
            ClientMessageParams::Set(key, value) => {
                let value = server
                    .set(key.as_str(), value.as_ref().map(|s| s.as_str()))
                    .unwrap();
                println!("Set result {value:?}");
                clients
                    .read()
                    .await
                    .sender(id)
                    .send(ServerMessage::Value(value))
                    .await
                    .unwrap();
            }
            ClientMessageParams::Subscribe(_) => todo!(),
            ClientMessageParams::Disconnect => {
                clients.write().await.remove(id);
            }
        }
    }
}

async fn client_task(
    id: ClientId,
    send: Sender<ClientMessage>,
    recv: Receiver<ServerMessage>,
    stream: TcpStream,
) -> anyhow::Result<()> {
    let ws_stream = accept_async(stream).await.expect("Failed to accept");
    let (ws_send, ws_recv) = ws_stream.split();

    let recv_task = tokio::spawn(client_task_recv(id, send.clone(), ws_recv));
    tokio::spawn(client_task_send(recv, ws_send));

    recv_task.await??;
    send.send(ClientMessage {
        id,
        msg: ClientMessageParams::Disconnect,
    })
    .await?;

    Ok(())
}

async fn client_task_recv(
    id: ClientId,
    send: Sender<ClientMessage>,
    mut ws_recv: SplitStream<WebSocketStream<TcpStream>>,
) -> anyhow::Result<()> {
    while let Some(msg) = ws_recv.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(Error::ConnectionClosed) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let msg = msg.to_text()?;
        let msg: ClientMessageParams = serde_json::from_str(msg)?;
        send.send(ClientMessage { id, msg }).await?;
    }
    Ok(())
}

async fn client_task_send(
    mut recv: Receiver<ServerMessage>,
    mut ws_send: SplitSink<WebSocketStream<TcpStream>, Message>,
) -> anyhow::Result<()> {
    while let Some(resp) = recv.recv().await {
        let resp_str = serde_json::to_string(&resp)?;
        println!("{resp_str}");
        ws_send.send(tungstenite::Message::Text(resp_str)).await?;
    }
    Ok(())
}
