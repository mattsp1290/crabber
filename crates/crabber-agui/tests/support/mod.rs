use ag_ui_client::{Agent as _, HttpAgent, RunAgentParams};
use crabber_agui::{ag_ui_core::event::Event, encode_sse};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

/// Exercise the pinned client's real transport, normalizer, protocol state and
/// reducer. The server only delivers the already-produced adapter frames.
pub async fn consume(events: &[Event]) -> ag_ui_client::RunAgentResult<Value> {
    consume_with_messages(events, Vec::new()).await
}

pub async fn consume_with_messages(
    events: &[Event],
    messages: Vec<crabber_agui::ag_ui_core::types::Message>,
) -> ag_ui_client::RunAgentResult<Value> {
    let mut frames = Vec::new();
    for event in events {
        frames.extend(encode_sse(event, 1_048_576).unwrap());
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            assert!(request.len() <= 16_384);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = std::str::from_utf8(&request[..end]).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    break;
                }
            }
        }
        let header = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            frames.len()
        );
        socket.write_all(header.as_bytes()).await.unwrap();
        socket.write_all(&frames).await.unwrap();
        socket.shutdown().await.unwrap();
    });
    let client = HttpAgent::builder()
        .with_url_str(&format!("http://{address}/run"))
        .unwrap()
        .with_timeout(10)
        .build()
        .unwrap();
    let mut params = RunAgentParams::new()
        .with_thread_id("thread".into())
        .with_run_id("wire".into());
    if messages.is_empty() {
        params = params.user("request");
    } else {
        params.messages = messages;
    }
    let result = client.run_agent(&params, ()).await;
    // The same finite client observation also owns the server task. Always reap
    // it before evaluating the client result.
    server.abort();
    let joined = server.await;
    assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
    result.unwrap()
}
