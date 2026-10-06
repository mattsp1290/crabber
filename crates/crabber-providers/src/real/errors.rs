use crate::{ProviderError, ProviderErrorKind};
use futures::StreamExt;
use reqwest::{Response, header::HeaderMap};

pub(super) const ERROR_EXCERPT_MAX_BYTES: usize = 4096;
/// `"provider HTTP " + 3 status digits + ": "`.
pub(super) const ERROR_MESSAGE_PREFIX_BYTES: usize = 19;

/// Classifies a non-success custom-adapter response from its status and
/// sanitized, opaque, at-most-4096-byte excerpt.
///
/// The terminal second 401 after a dynamic credential refresh bypasses this
/// callback and is always a non-retryable authentication error. This callback
/// is never applied to built-in provider adapters. Hosts should still redact
/// the resulting error according to their own logging policy.
pub trait ErrorClassifier: Send + Sync {
    fn classify(&self, status: reqwest::StatusCode, excerpt: &str) -> (ProviderErrorKind, bool);
}

#[derive(Clone, Copy)]
pub(super) enum TransportCause {
    Connect,
    Timeout,
    Body,
}

pub(super) fn auth() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Auth,
        message: "credentials unavailable".into(),
        retryable: false,
    }
}
pub(super) fn invalid() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: "invalid provider setting".into(),
        retryable: false,
    }
}
pub(super) fn transport(cause: TransportCause) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Transport,
        message: match cause {
            TransportCause::Connect => "provider transport connect",
            TransportCause::Timeout => "provider transport timeout",
            TransportCause::Body => "provider transport body",
        }
        .into(),
        retryable: true,
    }
}
pub(super) fn transport_from_reqwest(error: &reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        transport(TransportCause::Timeout)
    } else if error.is_connect() {
        transport(TransportCause::Connect)
    } else {
        transport(TransportCause::Body)
    }
}
pub(super) fn credentials_from_headers(headers: &HeaderMap) -> Vec<String> {
    let mut credentials = Vec::new();
    for name in headers.keys() {
        let canonical = name == "authorization" || name == "x-api-key";
        for value in headers.get_all(name) {
            if !canonical && !value.is_sensitive() {
                continue;
            }
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            if value.is_empty() {
                continue;
            }
            if let Some(raw) = value.strip_prefix("Bearer ").filter(|raw| !raw.is_empty()) {
                credentials.push(raw.to_owned());
            }
            credentials.push(value);
        }
    }
    credentials
}
pub(super) fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}
pub(super) fn sanitize_excerpt(
    source: &[u8],
    credentials: &[String],
    may_be_truncated: bool,
) -> String {
    let mut representations = credentials
        .iter()
        .filter(|value| !value.is_empty())
        .flat_map(|credential| {
            let serialized = serde_json::to_string(credential).unwrap_or_default();
            let escaped_payload = serialized
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or_default()
                .to_owned();
            [credential.clone(), serialized, escaped_payload]
        })
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    representations.sort_unstable();
    representations.dedup();
    representations.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    let terminal_prefix_start = may_be_truncated
        .then(|| {
            let terminal_full_match_start = representations
                .iter()
                .map(String::as_bytes)
                .filter(|representation| source.ends_with(representation))
                .map(|representation| source.len() - representation.len())
                .min()
                .unwrap_or(source.len());
            representations
                .iter()
                .map(String::as_bytes)
                .filter_map(|representation| {
                    let max_prefix = source.len().min(representation.len().saturating_sub(1));
                    (1..=max_prefix).rev().find_map(|prefix_len| {
                        source
                            .ends_with(&representation[..prefix_len])
                            .then_some(source.len() - prefix_len)
                    })
                })
                .filter(|start| *start < terminal_full_match_start)
                .min()
        })
        .flatten();
    let source = terminal_prefix_start.map_or(source, |start| &source[..start]);
    let mut excerpt = String::from_utf8_lossy(source).into_owned();
    for representation in &representations {
        excerpt = excerpt.replace(representation, "[REDACTED]");
    }
    if terminal_prefix_start.is_some() {
        excerpt.push_str("[REDACTED]");
    }
    truncate_utf8(&mut excerpt, ERROR_EXCERPT_MAX_BYTES);
    excerpt
}
pub(super) async fn response_error(
    response: Response,
    classifier: Option<&dyn ErrorClassifier>,
    force_auth: bool,
    credentials: &[String],
) -> ProviderError {
    let status = response.status();
    let mut source = Vec::with_capacity(ERROR_EXCERPT_MAX_BYTES);
    let mut stream = response.bytes_stream();
    while source.len() < ERROR_EXCERPT_MAX_BYTES {
        match stream.next().await {
            Some(Ok(chunk)) => {
                let remaining = ERROR_EXCERPT_MAX_BYTES - source.len();
                source.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Some(Err(error)) => {
                if force_auth {
                    return ProviderError {
                        kind: ProviderErrorKind::Auth,
                        message: format!(
                            "provider HTTP {}: response body unavailable",
                            status.as_u16()
                        ),
                        retryable: false,
                    };
                }
                return transport_from_reqwest(&error);
            }
            None => break,
        }
    }
    let excerpt = sanitize_excerpt(
        &source,
        credentials,
        source.len() == ERROR_EXCERPT_MAX_BYTES,
    );
    let (kind, retryable) = if force_auth {
        (ProviderErrorKind::Auth, false)
    } else if let Some(classifier) = classifier {
        classifier.classify(status, &excerpt)
    } else {
        status_classification(status)
    };
    let message = format!("provider HTTP {}: {excerpt}", status.as_u16());
    debug_assert!(message.len() <= ERROR_MESSAGE_PREFIX_BYTES + ERROR_EXCERPT_MAX_BYTES);
    ProviderError {
        kind,
        message,
        retryable,
    }
}
pub(super) fn status_classification(status: reqwest::StatusCode) -> (ProviderErrorKind, bool) {
    let kind = match status.as_u16() {
        401 | 403 => ProviderErrorKind::Auth,
        429 => ProviderErrorKind::RateLimited,
        500..=599 => ProviderErrorKind::Server,
        _ => ProviderErrorKind::Invalid,
    };
    let retryable = matches!(
        kind,
        ProviderErrorKind::RateLimited | ProviderErrorKind::Server
    );
    (kind, retryable)
}
