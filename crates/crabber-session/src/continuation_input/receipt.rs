use super::{Domain, Error, Kind};
use crate::{ApprovalReceipt, ToolContinuationReceipt};
use crabber_core::{
    ControlId, EventCursor, EventRecord, InputFingerprint, InteractionState, RunId, SessionId,
};
#[derive(Clone, Copy, serde::Serialize)]
#[serde(tag = "domain", content = "receipt", rename_all = "snake_case")]
pub(crate) enum Receipt<'a> {
    Approval(&'a ApprovalReceipt),
    Tool(&'a ToolContinuationReceipt),
}
type Facts<'a> = (
    Domain,
    Kind,
    &'a crate::ContinuationTarget,
    Option<(&'a crabber_core::InteractionId, InteractionState)>,
);
impl<'a> Receipt<'a> {
    pub(super) fn verify(self, event: &EventRecord) -> Result<(), Error> {
        if crate::snapshot::encoded_bytes(&self, 32768).is_none()
            || crate::snapshot::encoded_bytes(event, 65536).is_none()
        {
            return Err(Error);
        }
        match self {
            Self::Approval(r) => crate::approval::verify_receipt(r, event).map_err(|_| Error),
            Self::Tool(r) => crate::tool_continuation::verify_receipt(r, event).map_err(|_| Error),
        }
    }
    pub(super) fn session(self) -> &'a SessionId {
        match self {
            Self::Approval(r) => &r.session_id,
            Self::Tool(r) => &r.session_id,
        }
    }
    pub(super) fn run(self) -> &'a RunId {
        match self {
            Self::Approval(r) => &r.run_id,
            Self::Tool(r) => &r.run_id,
        }
    }
    pub(super) fn operation(self) -> &'a ControlId {
        match self {
            Self::Approval(r) => &r.operation_id,
            Self::Tool(r) => &r.operation_id,
        }
    }
    pub(super) fn semantic(self) -> &'a InputFingerprint {
        match self {
            Self::Approval(r) => &r.semantic_digest,
            Self::Tool(r) => &r.semantic_digest,
        }
    }
    pub(crate) fn cursor(self) -> EventCursor {
        match self {
            Self::Approval(r) => r.event_cursor,
            Self::Tool(r) => r.event_cursor,
        }
    }
    pub(super) fn facts(self) -> Result<Facts<'a>, Error> {
        Ok(match self {
            Self::Approval(r) => match &r.outcome {
                crate::ApprovalOutcome::Suspended { continuation } => {
                    (Domain::Approval, Kind::Suspension, continuation, None)
                }
                crate::ApprovalOutcome::Answered {
                    continuation,
                    interaction_id,
                    state,
                } => (
                    Domain::Approval,
                    Kind::Answer,
                    continuation,
                    Some((interaction_id, *state)),
                ),
                _ => return Err(Error),
            },
            Self::Tool(r) => match &r.outcome {
                crate::ToolContinuationOutcome::Suspended { continuation } => {
                    (Domain::Tool, Kind::Suspension, continuation, None)
                }
                crate::ToolContinuationOutcome::Answered {
                    continuation,
                    interaction_id,
                    state,
                } => (
                    Domain::Tool,
                    Kind::Answer,
                    continuation,
                    Some((interaction_id, *state)),
                ),
                _ => return Err(Error),
            },
        })
    }
}
