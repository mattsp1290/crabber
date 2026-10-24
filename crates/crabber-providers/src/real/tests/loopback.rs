//! Loopback HTTP scaffolding shared by the adapter tests.
use super::*;

pub(super) use crate::RequestIdentity;
pub(super) use crabber_core::{RunId, SessionId, TurnId};
pub(super) use std::time::Duration;
pub(super) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

pub(in crate::real) const LOOPBACK_TIMEOUT: Duration = Duration::from_secs(5);

pub(in crate::real) async fn accept_loopback_request(
    listener: &TcpListener,
) -> (TcpStream, String) {
    let (socket, headers, _) = accept_loopback_request_with_body(listener).await;
    (socket, headers)
}

pub(super) async fn accept_loopback_request_with_body(
    listener: &TcpListener,
) -> (TcpStream, String, Vec<u8>) {
    let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
        .await
        .expect("timed out waiting for provider request")
        .expect("failed to accept provider request");
    let mut raw = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
            .await
            .expect("timed out reading provider request")
            .expect("failed to read provider request");
        assert_ne!(n, 0, "request ended before its headers");
        raw.extend_from_slice(&buffer[..n]);
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8(raw[..header_end].to_vec())
        .expect("provider request headers were not UTF-8");
    let content_length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| {
            value
                .trim()
                .parse::<usize>()
                .expect("invalid request Content-Length")
        });
    let request_end = header_end
        .checked_add(content_length)
        .expect("request length overflowed");
    while raw.len() < request_end {
        let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
            .await
            .expect("timed out reading provider request body")
            .expect("failed to read provider request body");
        assert_ne!(n, 0, "request ended before its declared body");
        raw.extend_from_slice(&buffer[..n]);
    }
    let body = raw[header_end..request_end].to_vec();
    (socket, headers, body)
}

pub(in crate::real) async fn write_loopback_response(socket: &mut TcpStream, response: &[u8]) {
    timeout(LOOPBACK_TIMEOUT, socket.write_all(response))
        .await
        .expect("timed out writing provider response")
        .expect("failed to write provider response");
}

pub(in crate::real) fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        context: Vec::new(),
        identity: RequestIdentity {
            session_id: SessionId::from("session-1"),
            run_id: RunId::from("run-1"),
            turn_id: TurnId::from("turn-1"),
        },
        selection: Selection {
            provider_id: provider.into(),
            model_id: "model".into(),
        },
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        reasoning_effort: None,
        tool_choice: None,
    }
}
