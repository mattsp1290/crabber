//! Shared private codecs and integrity checks for typed state transactions.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use crate::PublicStateSnapshot;
use crate::{PublicStateError, PublicStateMutation, PublicStateReceipt, StoreError};
use crabber_core::{
    ControlId, EventKind, EventRecord, InputFingerprint, PublicStateContents, Run, RunFence, RunId,
    SessionId, SessionRevision,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// New display writes must match the SDK's object-valued activity content.
/// Keep this separate from fingerprint/decode validation: an actual retained
/// legacy receipt must remain reconcilable before applying new-write rules.
pub(crate) fn validate_fresh_activities(
    contents: &PublicStateContents,
) -> Result<(), PublicStateError> {
    if contents
        .activities()
        .iter()
        .any(|activity| !activity.content().is_object())
    {
        return Err(PublicStateError::InvalidRequest);
    }
    Ok(())
}

pub(crate) fn validate_id(id: &ControlId) -> Result<(), PublicStateError> {
    if id.0.is_empty()
        || id.0.len() > 128
        || !id
            .0
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    {
        return Err(PublicStateError::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn verify_owner(
    run: &Run,
    fence: &RunFence,
    now: OffsetDateTime,
) -> Result<(), PublicStateError> {
    if run.id != fence.run_id
        || run.claim_token != fence.claim_token
        || run.status.is_terminal()
        || run.lease_until <= now
    {
        return Err(PublicStateError::StaleOwner);
    }
    Ok(())
}
pub(crate) fn event(
    run: &Run,
    mutation: &PublicStateMutation,
    revision: SessionRevision,
    now: OffsetDateTime,
) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind: EventKind::PublicStateSnapshot,
        payload: serde_json::json!({"version":1,"revision":revision,"contents":mutation.contents}),
        correlation: Some(mutation.id.0.clone()),
        live_only: false,
        created_at: now,
    }
}
pub(crate) fn unknown(_: StoreError) -> PublicStateError {
    PublicStateError::UnknownStoreOutcome
}

// Explicit database record codec, not user-content interpretation. The inner
// JSON text preserves NUL and finite-number forms that JSONB cannot represent.
#[cfg(feature = "postgres")]
pub(crate) fn encode_event(
    event: &EventRecord,
) -> Result<sqlx::types::Json<serde_json::Value>, StoreError> {
    let text = serde_json::to_string(event)
        .map_err(|_| StoreError::Validation("event encoding failed".into()))?;
    Ok(sqlx::types::Json(
        serde_json::json!({"encoding":"crabber.event.json-text.v1","record":text}),
    ))
}
#[cfg(feature = "postgres")]
pub(crate) fn decode_event(
    value: sqlx::types::Json<serde_json::Value>,
) -> Result<EventRecord, StoreError> {
    if value.0.get("encoding").is_some() {
        if value.0.get("encoding").and_then(serde_json::Value::as_str)
            != Some("crabber.event.json-text.v1")
        {
            return Err(StoreError::Validation("unsupported event encoding".into()));
        }
        let text = value
            .0
            .get("record")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| StoreError::Validation("invalid event record".into()))?;
        serde_json::from_str(text)
            .map_err(|_| StoreError::Validation("invalid event record".into()))
    } else {
        serde_json::from_value(value.0)
            .map_err(|_| StoreError::Validation("invalid event record".into()))
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn snapshot(
    session: &SessionId,
    revision: i64,
    format: Option<i64>,
    state: Option<String>,
    activities: Option<String>,
) -> Result<PublicStateSnapshot, PublicStateError> {
    let revision = SessionRevision::new(
        u64::try_from(revision).map_err(|_| PublicStateError::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    let contents = match format {
        None => PublicStateContents::default(),
        Some(1) => {
            let state = serde_json::from_str(&state.ok_or(PublicStateError::UnknownStoreOutcome)?)
                .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
            let activities =
                serde_json::from_str(&activities.ok_or(PublicStateError::UnknownStoreOutcome)?)
                    .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
            PublicStateContents::new(state, activities).map_err(unknown)?
        }
        _ => return Err(PublicStateError::UnknownStoreOutcome),
    };
    Ok(PublicStateSnapshot {
        session_id: session.clone(),
        revision,
        contents,
    })
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn receipt(
    session: &SessionId,
    id: &ControlId,
    kind: &str,
    digest: &str,
    event_seq: i64,
    data: Option<&str>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<PublicStateReceipt, PublicStateError> {
    if kind != "public_state" {
        return Err(PublicStateError::SemanticConflict);
    }
    let raw = data.ok_or(PublicStateError::UnknownStoreOutcome)?;
    proof
        .charge(raw.len())
        .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    let receipt: PublicStateReceipt =
        serde_json::from_str(raw).map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    if receipt.session_id != *session
        || receipt.operation_id != *id
        || receipt.semantic_digest.as_str() != digest
        || u64::try_from(event_seq).ok() != Some(receipt.event_cursor.0)
    {
        return Err(PublicStateError::UnknownStoreOutcome);
    }
    Ok(receipt)
}

pub(crate) fn semantic_digest(
    session: &SessionId,
    run: &RunId,
    expected: SessionRevision,
    contents: &PublicStateContents,
) -> Result<InputFingerprint, PublicStateError> {
    let mut value =
        serde_json::to_value(("crabber.public-state.v1", session, run, expected, contents))
            .map_err(|_| PublicStateError::InvalidRequest)?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
    .map_err(|_| PublicStateError::InvalidRequest)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateEvent {
    version: u32,
    revision: SessionRevision,
    contents: PublicStateContents,
}
pub(crate) fn verify_receipt_event(
    receipt: &PublicStateReceipt,
    event: &EventRecord,
) -> Result<(), PublicStateError> {
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(2)
    {
        return super::verify_source_event(receipt, event);
    }
    if event.kind != EventKind::PublicStateSnapshot
        || event.live_only
        || event.session_id != receipt.session_id
        || event.run_id != receipt.run_id
        || event.correlation.as_deref() != Some(&receipt.operation_id.0)
        || event
            .cursor
            .is_some_and(|cursor| cursor != receipt.event_cursor)
    {
        return Err(PublicStateError::UnknownStoreOutcome);
    }
    let saved: StateEvent = serde_json::from_value(event.payload.clone())
        .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    if saved.version != 1 || saved.revision != receipt.revision {
        return Err(PublicStateError::UnknownStoreOutcome);
    }
    let previous = receipt
        .revision
        .get()
        .checked_sub(1)
        .and_then(|v| SessionRevision::new(v).ok())
        .ok_or(PublicStateError::UnknownStoreOutcome)?;
    if semantic_digest(
        &receipt.session_id,
        &receipt.run_id,
        previous,
        &saved.contents,
    )? != receipt.semantic_digest
    {
        return Err(PublicStateError::UnknownStoreOutcome);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryStore, Store, public_state_contract};
    use crabber_core::EventCursor;

    #[tokio::test]
    async fn receipt_rejects_changed_event_semantics_and_scope() {
        let store = MemoryStore::new();
        let admitted = public_state_contract::admit(&store, OffsetDateTime::now_utc()).await;
        let mutation = PublicStateMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
            contents: public_state_contract::contents(),
        };
        let receipt = store
            .execution(admitted.fence)
            .await
            .unwrap()
            .mutate_public_state(mutation)
            .await
            .unwrap();
        let event = store
            .list_events(&admitted.session.id, None, 10)
            .await
            .unwrap()
            .remove(0);
        assert_eq!(verify_receipt_event(&receipt, &event), Ok(()));
        for field in ["version", "revision", "contents"] {
            let mut changed = event.clone();
            changed.payload[field] = serde_json::Value::Null;
            assert_eq!(
                verify_receipt_event(&receipt, &changed),
                Err(PublicStateError::UnknownStoreOutcome)
            );
        }
        let mut changed = event.clone();
        changed.payload["contents"] = serde_json::to_value(PublicStateContents::default()).unwrap();
        assert_eq!(
            verify_receipt_event(&receipt, &changed),
            Err(PublicStateError::UnknownStoreOutcome)
        );
        let mut variants = vec![event.clone(); 6];
        variants[0].run_id = RunId::new();
        variants[1].session_id = SessionId::new();
        variants[2].correlation = Some(ControlId::new().0);
        variants[3].kind = EventKind::RunSettled;
        variants[4].live_only = true;
        variants[5].cursor = Some(EventCursor(receipt.event_cursor.0 + 1));
        for changed in variants {
            assert_eq!(
                verify_receipt_event(&receipt, &changed),
                Err(PublicStateError::UnknownStoreOutcome)
            );
        }
    }
}

#[cfg(feature = "postgres")]
#[derive(serde::Serialize)]
struct Encoded<'a> {
    encoding: &'static str,
    record: &'a str,
}
/// New source publication meters the text carrier and JSON wrapper separately.
#[cfg(feature = "postgres")]
pub(crate) fn encode_source_event(
    event: &EventRecord,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<sqlx::types::Json<serde_json::Value>, PublicStateError> {
    proof
        .measure(event)
        .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    let text = serde_json::to_string(event).map_err(|_| PublicStateError::InvalidRequest)?;
    let wire = Encoded {
        encoding: "crabber.event.json-text.v1",
        record: &text,
    };
    proof
        .measure(&wire)
        .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    let value = serde_json::to_value(wire).map_err(|_| PublicStateError::InvalidRequest)?;
    // The final database wire encoding is independent of its retained Value.
    proof
        .measure(&value)
        .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
    Ok(sqlx::types::Json(value))
}
#[cfg(feature = "postgres")]
pub(crate) fn decode_source_event(
    value: sqlx::types::Json<serde_json::Value>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<EventRecord, PublicStateError> {
    if value.0.get("encoding").is_some() {
        if value.0.get("encoding").and_then(serde_json::Value::as_str)
            != Some("crabber.event.json-text.v1")
        {
            return Err(PublicStateError::UnknownStoreOutcome);
        }
        let raw = value
            .0
            .get("record")
            .and_then(serde_json::Value::as_str)
            .ok_or(PublicStateError::UnknownStoreOutcome)?;
        proof
            .charge(raw.len())
            .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
        serde_json::from_str(raw).map_err(|_| PublicStateError::UnknownStoreOutcome)
    } else {
        proof
            .measure(&value.0)
            .map_err(|_| PublicStateError::UnknownStoreOutcome)?;
        serde_json::from_value(value.0).map_err(|_| PublicStateError::UnknownStoreOutcome)
    }
}
