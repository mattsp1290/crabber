//! Canonical record accounting and authenticated continuations for SQL stores.
// Removed in W3 when SqliteStore calls these.
#![cfg_attr(not(feature = "postgres"), allow(dead_code))]

use crate::{SnapshotContinuation, StoreError};
use crabber_core::{EventCursor, Message, ToolCallRecord};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) struct Accounting {
    pub(crate) record: String,
    pub(crate) parts: i64,
    pub(crate) text: i64,
    pub(crate) bytes: i64,
}
fn accounting<T: Serialize>(
    record: &T,
    parts: usize,
    text: usize,
) -> Result<Accounting, StoreError> {
    let record = serde_json::to_string(record).map_err(|_| invalid())?;
    Ok(Accounting {
        parts: i64::try_from(parts).map_err(|_| invalid())?,
        text: i64::try_from(text).map_err(|_| invalid())?,
        bytes: i64::try_from(record.len()).map_err(|_| invalid())?,
        record,
    })
}
pub(crate) fn message_record(message: &Message) -> Result<Accounting, StoreError> {
    accounting(
        message,
        message.parts.len(),
        message.parts.iter().fold(0usize, |n, p| {
            n.saturating_add(crate::snapshot::text_bytes(&p.content))
        }),
    )
}
pub(crate) fn call_record(call: &ToolCallRecord) -> Result<Accounting, StoreError> {
    accounting(
        call,
        0,
        call.result.as_ref().map_or(0, |r| {
            r.content.iter().fold(0usize, |n, b| {
                n.saturating_add(crate::snapshot::text_bytes(b))
            })
        }),
    )
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Boundary {
    pub(crate) session: String,
    pub(crate) revision: i64,
    pub(crate) messages: i64,
    pub(crate) calls: i64,
    pub(crate) message_after: i64,
    pub(crate) call_after: i64,
    pub(crate) high_water: EventCursor,
}
pub(crate) fn invalid() -> StoreError {
    StoreError::Validation("invalid snapshot continuation or record".into())
}
pub(crate) fn digest(parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    for part in parts {
        hash.update(part.len().to_le_bytes());
        hash.update(part.as_bytes());
    }
    format!("{:x}", hash.finalize())
}
fn signature(key: &str, json: &str) -> Result<String, StoreError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).map_err(|_| invalid())?;
    mac.update(json.as_bytes());
    Ok(format!("{:x}", mac.finalize().into_bytes()))
}
pub(crate) fn token(boundary: &Boundary, key: &str) -> Result<SnapshotContinuation, StoreError> {
    let json = serde_json::to_string(boundary).map_err(|_| invalid())?;
    Ok(SnapshotContinuation(format!(
        "{}:{json}",
        signature(key, &json)?
    )))
}
pub(crate) fn parse(
    token: &SnapshotContinuation,
    key: &str,
    session: &str,
) -> Result<Boundary, StoreError> {
    if token.0.len() > 2048 {
        return Err(invalid());
    }
    let (supplied_signature, json) = token.0.split_once(':').ok_or_else(invalid)?;
    let expected = signature(key, json)?;
    if supplied_signature.len() != expected.len()
        || supplied_signature
            .bytes()
            .zip(expected.bytes())
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            != 0
    {
        return Err(invalid());
    }
    let b: Boundary = serde_json::from_str(json).map_err(|_| invalid())?;
    if b.session != session
        || b.revision < 0
        || b.message_after < 0
        || b.call_after < 0
        || b.message_after > b.messages
        || b.call_after > b.calls
    {
        return Err(invalid());
    }
    Ok(b)
}
#[cfg(test)]
mod token_tests {
    use super::*;
    #[test]
    fn another_database_key_cannot_authenticate_a_boundary() {
        let boundary = Boundary {
            session: digest(&["session"]),
            revision: 0,
            messages: 1,
            calls: 0,
            message_after: 0,
            call_after: 0,
            high_water: EventCursor(7),
        };
        let token = token(&boundary, "database-one-secret").unwrap();
        assert!(parse(&token, "database-one-secret", &boundary.session).is_ok());
        assert!(parse(&token, "database-two-secret", &boundary.session).is_err());
    }
}
