//! Canonical record accounting and authenticated continuations for SQL stores.

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

pub(crate) fn imported_call_record(
    call: &crabber_core::ImportedToolCall,
) -> Result<Accounting, StoreError> {
    accounting(
        call,
        0,
        call.result.content.iter().fold(0usize, |n, block| {
            n.saturating_add(crate::snapshot::text_bytes(block))
        }),
    )
}
pub(crate) fn provenance_record(
    record: &crabber_core::ImportedMessageProvenance,
) -> Result<Accounting, StoreError> {
    accounting(record, 0, 0)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Boundary {
    pub(crate) session: String,
    pub(crate) revision: i64,
    pub(crate) public_revision: i64,
    pub(crate) messages: i64,
    pub(crate) calls: i64,
    pub(crate) imported: i64,
    pub(crate) provenance: i64,
    pub(crate) message_after: i64,
    pub(crate) call_after: i64,
    pub(crate) imported_after: i64,
    pub(crate) provenance_after: i64,
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
        "v2:{}:{json}",
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
    let (supplied_signature, json) = token
        .0
        .strip_prefix("v2:")
        .ok_or_else(invalid)?
        .split_once(':')
        .ok_or_else(invalid)?;
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
        || b.public_revision < 0
        || b.message_after < 0
        || b.call_after < 0
        || b.imported_after < 0
        || b.provenance_after < 0
        || b.message_after > b.messages
        || b.call_after > b.calls
        || b.imported_after > b.imported
        || b.provenance_after > b.provenance
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
            public_revision: 0,
            messages: 1,
            calls: 0,
            imported: 0,
            provenance: 0,
            message_after: 0,
            call_after: 0,
            imported_after: 0,
            provenance_after: 0,
            high_water: EventCursor(7),
        };
        let token = token(&boundary, "database-one-secret").unwrap();
        assert!(parse(&token, "database-one-secret", &boundary.session).is_ok());
        assert!(parse(&token, "database-two-secret", &boundary.session).is_err());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordKind {
    Message,
    Native,
    Imported,
    Provenance,
}

/// Validate newly introduced presentation carriers against exact canonical
/// accounting and scope before publishing them in a raw or public page.
pub(crate) fn append_imported(
    page: &mut crate::SnapshotPage,
    kind: RecordKind,
    record: &str,
    session: &crabber_core::SessionId,
    identity: &str,
    expected: (i64, i64, i64),
) -> Result<(), StoreError> {
    let accounting = match kind {
        RecordKind::Imported => {
            let call: crabber_core::ImportedToolCall =
                serde_json::from_str(record).map_err(|_| invalid())?;
            if call.session_id != *session || call.id.0 != identity {
                return Err(invalid());
            }
            let accounting = imported_call_record(&call)?;
            if accounting.record != record
                || (accounting.parts, accounting.text, accounting.bytes) != expected
            {
                return Err(invalid());
            }
            page.imported_calls.push(call);
            accounting
        }
        RecordKind::Provenance => {
            let origin: crabber_core::ImportedMessageProvenance =
                serde_json::from_str(record).map_err(|_| invalid())?;
            if origin.session_id != *session || origin.message_id.0 != identity {
                return Err(invalid());
            }
            let accounting = provenance_record(&origin)?;
            if accounting.record != record
                || (accounting.parts, accounting.text, accounting.bytes) != expected
            {
                return Err(invalid());
            }
            page.imported_provenance.push(origin);
            accounting
        }
        RecordKind::Message | RecordKind::Native => return Err(invalid()),
    };
    drop(accounting);
    Ok(())
}
