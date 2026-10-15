use super::tests::{Credentials, request};
use super::*;
use crate::{HttpAdapter, Protocol, Streamer};
use crabber_core::{MediaKind, MediaSource, Role};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn image_placements_mime_and_source_errors_fail_before_credentials() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let credentials = Arc::new(Credentials(AtomicUsize::new(0)));
    for protocol in [
        Protocol::Responses,
        Protocol::Messages,
        Protocol::ChatCompletions,
    ] {
        let adapter = HttpAdapter::custom("fixture", &endpoint, protocol)
            .with_credential_source(credentials.clone());
        for (role, mime, source) in [
            (
                Role::Assistant,
                "image/png",
                MediaSource::Data {
                    base64: "AQ==".into(),
                },
            ),
            (
                Role::System,
                "image/png",
                MediaSource::Data {
                    base64: "AQ==".into(),
                },
            ),
            (
                Role::Tool,
                "image/png",
                MediaSource::Data {
                    base64: "AQ==".into(),
                },
            ),
            (
                Role::User,
                "image/svg+xml",
                MediaSource::Data {
                    base64: "AQ==".into(),
                },
            ),
            (
                Role::User,
                "image/png",
                MediaSource::Data {
                    base64: "PRIVATE_SENTINEL".into(),
                },
            ),
            (
                Role::User,
                "image/png",
                MediaSource::Url {
                    url: "https://user:PRIVATE_SENTINEL@example.invalid/image".into(),
                },
            ),
        ] {
            let mut input = request(
                ContentBlock::MediaContent {
                    kind: MediaKind::Image,
                    mime_type: mime.into(),
                    source,
                },
                false,
            );
            input.messages[0].role = role;
            let Err(error) = adapter.stream(input).await else {
                panic!("unsupported image must fail before I/O")
            };
            assert_eq!(error.kind, ProviderErrorKind::Invalid);
            assert!(!error.to_string().contains("PRIVATE_SENTINEL"));
            assert_eq!(credentials.0.load(Ordering::SeqCst), 0);
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}
