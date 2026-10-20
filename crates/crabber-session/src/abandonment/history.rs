//! Narrow original abandonment proof. No checkpoint is decoded or fabricated.
use crate::StoreError;
use crate::admission_journal::{Init, NoEffect};
use crate::native_parent::{RunIssuance, RunState};
use crabber_core::{
    AbandonRequest, EpochId, EventCursor, EventKind, RunId, RunStatus, SessionId, TurnId, Usage,
};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunView {
    pub id: RunId,
    pub session_id: SessionId,
    pub status: RunStatus,
    pub owner: String,
    pub claim_token: String,
    pub lease_until: OffsetDateTime,
    pub epoch_id: EpochId,
    pub config_hash: String,
    pub plan_fingerprint: String,
    pub usage: Usage,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub checkpoint_absent: bool,
    pub error_absent: bool,
}
#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Evidence {
    pub request: AbandonRequest,
    pub run: RunView,
    pub no_effect_v1: Option<NoEffect>,
    pub interrupted: i64,
}
#[derive(PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Event {
    pub cursor: Option<EventCursor>,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub turn_id: Option<TurnId>,
    pub kind: EventKind,
    pub correlation: Option<String>,
    pub live_only: bool,
    pub created_at: OffsetDateTime,
    pub status: RunStatus,
    pub usage: Usage,
    pub evidence: Evidence,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Commit {
    pub request: AbandonRequest,
    pub no_effect_v1: Option<NoEffect>,
    pub run: RunView,
    pub event: Event,
    pub interrupted: i64,
}
fn invalid() -> StoreError {
    StoreError::AdmissionConflict
}
/// The referenced pre-abandon issuer and the two actual fence transitions must
/// come from the independently retained journal, not from the commit projection.
pub(crate) fn verify(
    current: &RunView,
    commit: &Commit,
    event: &Event,
    cursor: EventCursor,
    init: &Init,
    issuers: [&RunIssuance; 3],
) -> Result<bool, StoreError> {
    let Some(marker) = &commit.no_effect_v1 else {
        if event.evidence.no_effect_v1.is_some() || commit.event.evidence.no_effect_v1.is_some() {
            return Err(invalid());
        }
        return Ok(false);
    };
    marker.verify(init, &commit.request)?;
    let [original, revoked, terminal] = issuers;
    for issuance in issuers {
        issuance.validate().map_err(|_| invalid())?;
    }
    let expected = RunState::new(
        &current.session_id,
        &current.id,
        RunStatus::Interrupted,
        &current.claim_token,
    )
    .map_err(|_| invalid())?;
    let mut revoked_state = expected.clone();
    revoked_state.status = RunStatus::Running;
    if original != &marker.issuance
        || revoked.sequence != original.sequence.checked_add(1).ok_or_else(invalid)?
        || terminal.sequence != revoked.sequence.checked_add(1).ok_or_else(invalid)?
        || revoked.state != revoked_state
        || terminal.state != expected
        || current.status != RunStatus::Interrupted
        || current.usage != Usage::default()
        || !current.checkpoint_absent
        || !current.error_absent
        || current != &commit.run
        || current != &event.evidence.run
        || current != &commit.event.evidence.run
        || commit.request != event.evidence.request
        || commit.request != commit.event.evidence.request
        || commit.request.expected.run_id != current.id
        || commit.request.expected_owner != current.owner
        || commit.request.expected.claim_token == current.claim_token
        || commit.interrupted != 0
        || event.evidence.interrupted != 0
        || commit.event.evidence.interrupted != 0
        || event.evidence.no_effect_v1.as_ref() != Some(marker)
        || commit.event.evidence.no_effect_v1.as_ref() != Some(marker)
        || event.cursor.is_some()
        || commit.event.cursor != Some(cursor)
        || event.session_id != current.session_id
        || event.run_id != current.id
        || event.kind != EventKind::RunSettled
        || event.turn_id.is_some()
        || event.correlation.is_some()
        || event.live_only
        || event.status != RunStatus::Interrupted
        || event.usage != current.usage
        || event.created_at != current.updated_at
        || current.lease_until != current.updated_at
    {
        return Err(invalid());
    }
    // Cursor is supplied by the store after insertion; all other event fields
    // are required to match the independent original publication exactly.
    let mut stored = serde_json::to_value(&commit.event).map_err(|_| invalid())?;
    stored["cursor"] = serde_json::Value::Null;
    if stored != serde_json::to_value(event).map_err(|_| invalid())? {
        return Err(invalid());
    }
    Ok(true)
}
