use std::{net::SocketAddr, sync::Arc};

use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
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
    tungstenite::{self, Error},
    WebSocketStream,
};

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:9002";
    let listener = TcpListener::bind(&addr).await.expect("Can't listen");

    let (request_send, request_recv) = tokio::sync::mpsc::channel(1024);
    let clients = Arc::new(RwLock::new(ClientManager::new()));

    let server = Server::open(clients.clone(), "data").unwrap();
    tokio::spawn(server_task(request_recv, server));

    while let Ok((stream, _)) = listener.accept().await {
        /*let peer = stream
        .peer_addr()
        .expect("connected streams should have a peer address");*/

        let mut client_writer = clients.write().await;
        let (client_id, resp_recv) = client_writer.new_client();

        let request_send = request_send.clone();
        tokio::spawn(client_task(client_id, request_send, resp_recv, stream));
    }
}

async fn server_task(mut recv: Receiver<ClientMessage>, server: Server) {
    loop {
        let Some(ClientMessage { id, msg }) = recv.recv().await else {
            break;
        };
        match msg {
            iceload::ClientMessageParams::Get(_) => todo!(),
            iceload::ClientMessageParams::Set(_, _) => todo!(),
            iceload::ClientMessageParams::Subscribe(_) => todo!(),
            iceload::ClientMessageParams::Disconnect => todo!(),
        }
    }
}

async fn client_task(
    id: ClientId,
    send: Sender<ClientMessage>,
    recv: Receiver<ServerMessage>,
    stream: TcpStream,
) -> anyhow::Result<()> {
    let result = client_task_inner(id, send.clone(), recv, stream).await;
    send.send(ClientMessage {
        id,
        msg: ClientMessageParams::Disconnect,
    })
    .await?;
    result
}

async fn client_task_inner(
    id: ClientId,
    send: Sender<ClientMessage>,
    mut recv: Receiver<ServerMessage>,
    stream: TcpStream,
) -> anyhow::Result<()> {
    let mut ws_stream = accept_async(stream).await.expect("Failed to accept");

    while let Some(msg) = ws_stream.next().await {
        let msg = match msg {
            Ok(msg) => msg,
            Err(Error::ConnectionClosed) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let msg = msg.to_text()?;
        let msg: ClientMessageParams = serde_json::from_str(msg)?;
        send.send(ClientMessage { id, msg }).await?;

        while let Some(resp) = recv.recv().await {
            let resp_str = serde_json::to_string(&resp)?;
            ws_stream.send(tungstenite::Message::Text(resp_str)).await?;
        }
    }

    Ok(())
}
