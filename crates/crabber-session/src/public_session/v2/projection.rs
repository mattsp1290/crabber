//! Borrowed metadata projections are measured before any public strings clone.
use crate::public_session::budget::Budget;
use crate::public_session::v2::proof;
use crate::public_session::v2::proof::{Failure, unknown};
use crate::tool_continuation::ToolContinuationRecord;
use crate::{
    AttemptCleanup, AttemptSnapshot, AttemptState, AttemptTarget, ContinuationTarget, PhaseTarget,
    PublicControlHeaderV2, PublicFrontendRequirement, PublicSessionLimitV2, PublicToolRequirement,
    PublicToolSummary, ToolContinuationState,
};
use crabber_core::RunStatus;
use crabber_core::{ControlId, InteractionId, InteractionState, RunId, ToolCallId};
use serde::Serialize;
use time::OffsetDateTime;

pub(crate) fn validate_tool_state(
    record: &ToolContinuationRecord,
    barrier: bool,
    status: RunStatus,
    attempt: Option<&AttemptSnapshot>,
) -> Result<(), Failure> {
    match record.state {
        ToolContinuationState::Waiting
            if !barrier
                || status != RunStatus::Paused
                || attempt.is_none_or(|a| {
                    a.target != record.batch.attempt
                        || a.state != AttemptState::Suspended
                        || a.cleanup != AttemptCleanup::Complete
                }) =>
        {
            Err(unknown("waiting tool"))
        }
        ToolContinuationState::Claimed
            if !barrier
                || status != RunStatus::Running
                || attempt.is_none_or(|a| {
                    a.target != record.batch.next_attempt
                        || !matches!(
                            a.state,
                            AttemptState::Unstarted
                                | AttemptState::Started
                                | AttemptState::OutcomeUnknown
                        )
                }) =>
        {
            Err(unknown("claimed tool"))
        }
        ToolContinuationState::Released if barrier => Err(unknown("released tool")),
        ToolContinuationState::Released
            if record.termination.is_some()
                && (!status.is_terminal()
                    || attempt.is_none_or(|a| {
                        a.target != record.batch.attempt
                            || a.state != AttemptState::Suspended
                            || a.cleanup != AttemptCleanup::Complete
                    })) =>
        {
            Err(unknown("terminated tool"))
        }
        _ => Ok(()),
    }
}
#[derive(Serialize)]
struct Requirement<'a> {
    id: &'a InteractionId,
    call_id: &'a ToolCallId,
    phase: &'a PhaseTarget,
    state: InteractionState,
    expires_at: Option<OffsetDateTime>,
    consumed: bool,
}
#[derive(Serialize)]
struct Frontend<'a> {
    call_id: &'a ToolCallId,
    phase: &'a PhaseTarget,
    name: &'a str,
    descriptor_fingerprint: &'a crabber_core::InputFingerprint,
    consumed: bool,
}
#[derive(Serialize)]
struct View<'a> {
    run_id: &'a RunId,
    continuation: &'a ContinuationTarget,
    attempt: &'a AttemptTarget,
    next_attempt: &'a AttemptTarget,
    state: ToolContinuationState,
    original_cancel_id: Option<&'a ControlId>,
    requirements: Vec<Requirement<'a>>,
    frontend: Vec<Frontend<'a>>,
}
pub(crate) fn tool(
    record: &ToolContinuationRecord,
    max: u16,
    safe: &mut Budget,
) -> Result<PublicToolSummary, Failure> {
    if record
        .questions
        .len()
        .checked_add(record.batch.frontend.len())
        .is_none_or(|n| n > usize::from(max))
    {
        return Err(Failure::Limited(PublicSessionLimitV2::ToolRequirements));
    }
    let consumed = record.state != ToolContinuationState::Waiting;
    let view = View {
        run_id: &record.run_id,
        continuation: &record.batch.continuation,
        attempt: &record.batch.attempt,
        next_attempt: &record.batch.next_attempt,
        state: record.state,
        original_cancel_id: record.termination.as_ref().map(|t| &t.original.id),
        requirements: record
            .questions
            .iter()
            .map(|q| Requirement {
                id: &q.view.id,
                call_id: &q.view.call_id,
                phase: &q.view.phase,
                state: q.view.state,
                expires_at: q.view.expires_at,
                consumed: q.view.consumed,
            })
            .collect(),
        frontend: record
            .batch
            .frontend
            .iter()
            .map(|f| Frontend {
                call_id: &f.target.call_id,
                phase: &f.target,
                name: &f.descriptor.name,
                descriptor_fingerprint: &f.descriptor_fingerprint,
                consumed,
            })
            .collect(),
    };
    safe.measure(&view)?;
    Ok(PublicToolSummary {
        run_id: record.run_id.clone(),
        continuation: record.batch.continuation.clone(),
        attempt: record.batch.attempt.clone(),
        next_attempt: record.batch.next_attempt.clone(),
        state: record.state,
        original_cancel_id: view.original_cancel_id.cloned(),
        requirements: view
            .requirements
            .iter()
            .map(|q| PublicToolRequirement {
                id: q.id.clone(),
                call_id: q.call_id.clone(),
                phase: q.phase.clone(),
                state: q.state,
                expires_at: q.expires_at,
                consumed: q.consumed,
            })
            .collect(),
        frontend: view
            .frontend
            .iter()
            .map(|f| PublicFrontendRequirement {
                call_id: f.call_id.clone(),
                phase: f.phase.clone(),
                name: f.name.into(),
                descriptor_fingerprint: f.descriptor_fingerprint.clone(),
                consumed: f.consumed,
            })
            .collect(),
    })
}
pub(crate) fn finish(
    mut header: PublicControlHeaderV2,
    mut safe: Budget,
    proof: &proof::Proof,
) -> Result<PublicControlHeaderV2, Failure> {
    header.read_usage.proof_bytes = proof.used();
    for _ in 0..8 {
        safe.finish(&header)?;
        if header.read_usage.encoded_bytes == safe.used {
            return Ok(header);
        }
        header.read_usage.encoded_bytes = safe.used;
    }
    Err(unknown("header accounting"))
}
