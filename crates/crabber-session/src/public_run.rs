//! Bounded safe latest-run metadata; no execution authority or private Run decode.
use crabber_core::{RunId, RunStatus, SessionId, SessionRevision, Usage};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Safe run metadata for public session displays. Provider errors, checkpoints,
/// backend configuration, owner identity and fences are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunSummary {
    pub run_id: RunId,
    pub session_id: SessionId,
    pub status: RunStatus,
    pub usage: Usage,
    pub created_at: OffsetDateTime,
}
/// One coherent native metadata read. A missing run is distinct from a missing
/// session. Combine with other metadata/history only at the same revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicRunState {
    pub session_id: SessionId,
    pub revision: SessionRevision,
    pub run: Option<PublicRunSummary>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicRunOutcome {
    State(PublicRunState),
    /// The complete indivisible metadata record exceeds the requested byte cap.
    Limited,
}

pub(crate) const MAX_BYTES: usize = 8192;
pub(crate) fn validate_session(session: &SessionId) -> Result<(), crate::StoreError> {
    validate_id(&session.0, 512)
}
fn validate_id(id: &str, cap: usize) -> Result<(), crate::StoreError> {
    if id.is_empty() || id.len() > cap || id.chars().any(char::is_control) {
        return Err(invalid());
    }
    Ok(())
}
fn invalid() -> crate::StoreError {
    crate::StoreError::Validation("invalid public run metadata".into())
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn decode(
    session: &SessionId,
    text: Option<String>,
) -> Result<PublicRunOutcome, crate::StoreError> {
    let Some(text) = text else {
        return Ok(PublicRunOutcome::Limited);
    };
    let state: PublicRunState = serde_json::from_str(&text).map_err(|_| invalid())?;
    if state.session_id != *session {
        return Err(invalid());
    }
    if let Some(run) = &state.run {
        validate_id(&run.run_id.0, 128)?;
        if run.session_id != *session {
            return Err(invalid());
        }
    }
    Ok(PublicRunOutcome::State(state))
}

#[cfg(test)]
#[path = "public_run/tests.rs"]
pub(crate) mod tests;
