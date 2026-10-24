//! Shared loopback HTTP server fixture for provider wire tests; no test cases live here.
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

pub const LIMIT: Duration = Duration::from_secs(10);
pub type Captured = (BTreeMap<String, String>, Value);

async fn read_request(socket: &mut TcpStream, expected_path: &str) -> Captured {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before headers");
        raw.extend_from_slice(&buffer[..n]);
        if let Some(end) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(raw.len() < 65536);
    };
    assert!(raw.starts_with(format!("POST {expected_path} HTTP/1.1\r\n").as_bytes()));
    let headers: BTreeMap<_, _> = std::str::from_utf8(&raw[..end])
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length < 65536);
    while raw.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before body");
        raw.extend_from_slice(&buffer[..n]);
    }
    (
        headers,
        serde_json::from_slice(&raw[end..end + length]).unwrap(),
    )
}

/// Serves one scripted `text/event-stream` response per connection and captures each request.
pub async fn server(
    responses: Vec<String>,
    expected_path: &'static str,
) -> (String, tokio::task::JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(LIMIT, async move {
            let mut captured = Vec::new();
            for body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                captured.push(read_request(&mut socket, expected_path).await);
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        })
        .await
        .expect("loopback server timed out")
    });
    (url, task)
}
