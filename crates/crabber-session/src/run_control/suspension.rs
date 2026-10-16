//! Private native suspension evidence; a host cancellation claim is not join proof.
use crate::run_control::{RunControlError as Error, RunControlRequest, unknown};
use crate::{
    AttemptCleanup, AttemptConclusion, AttemptReceipt, AttemptSnapshot, AttemptState, RunBoundary,
};
use crabber_core::{Run, RunStatus, ToolCallId};
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SuspensionProof {
    pub conclusion: AttemptConclusion,
    pub receipt: AttemptReceipt,
}
pub(crate) const MAX_SUSPENSION_PROOF_BYTES: usize = crate::MAX_RUN_BOUNDARY_BYTES + 32_768;
impl SuspensionProof {
    pub(crate) fn verify(
        &self,
        run: &Run,
        latest: &AttemptSnapshot,
    ) -> Result<Vec<ToolCallId>, Error> {
        if crate::snapshot::encoded_bytes(self, MAX_SUSPENSION_PROOF_BYTES).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        self.conclusion
            .verify_replay(&self.receipt)
            .map_err(unknown)?;
        if run.status != RunStatus::Paused
            || latest != &self.receipt.attempt
            || latest.run_id != run.id
            || latest.session_id != run.session_id
            || latest.state != AttemptState::Suspended
            || latest.cleanup != AttemptCleanup::Complete
        {
            return Err(Error::Busy);
        }
        let RunBoundary::Suspended { checkpoint, usage } = &self.conclusion.boundary else {
            return Err(Error::UnknownStoreOutcome);
        };
        if run.checkpoint.as_ref() != Some(checkpoint) || run.usage != *usage {
            return Err(Error::UnknownStoreOutcome);
        }
        if checkpoint
            .get("version")
            .and_then(serde_json::Value::as_u64)
            != Some(4)
        {
            return Err(Error::Busy);
        }
        let pending = checkpoint.get("pending_calls").ok_or(Error::Busy)?;
        if pending.as_array().is_none_or(|calls| calls.len() > 64) {
            return Err(Error::UnknownStoreOutcome);
        }
        let calls: Vec<ToolCallId> = serde_json::from_value(pending.clone()).map_err(unknown)?;
        if calls
            .iter()
            .any(|id| id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control))
            || calls
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != calls.len()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(calls)
    }
}
pub(crate) fn cancelled_run(run: &Run, now: time::OffsetDateTime) -> Run {
    let mut terminal = run.clone();
    terminal.status = RunStatus::Interrupted;
    terminal.claim_token = uuid::Uuid::new_v4().to_string();
    terminal.lease_until = now;
    terminal.updated_at = now;
    terminal.error = Some("host cancelled the suspended run".into());
    terminal
}
pub(crate) fn cancel_call(
    request: &RunControlRequest,
    run: &Run,
    call: &crabber_core::ToolCallRecord,
    now: time::OffsetDateTime,
) -> Result<crate::PhaseSettlementRecords, Error> {
    use sha2::{Digest, Sha256};
    let (result, mut message, mut event) = crate::abandonment::interrupted_tool(run, call, now);
    let digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(
                "crabber.run-control.checkpoint-cancel.v1",
                &request.target,
                &request.id,
                &call.id
            ))
            .map_err(unknown)?
        )
    );
    message.id = crabber_core::MessageId(format!("control-cancel-{digest}"));
    message.parts[0].id = crabber_core::PartId(format!("control-cancel-part-{digest}"));
    message.parts[0].message_id = message.id.clone();
    event.turn_id = source_turn(run)?;
    event.payload["message_id"] = serde_json::to_value(&message.id).map_err(unknown)?;
    Ok(crate::PhaseSettlementRecords {
        result,
        message,
        event,
        call_status: crabber_core::ToolCallStatus::Interrupted,
    })
}
pub(crate) fn source_turn(run: &Run) -> Result<Option<crabber_core::TurnId>, Error> {
    run.checkpoint
        .as_ref()
        .and_then(|c| c.get("source_turn_id"))
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value(v.clone()).map_err(unknown))
        .transpose()
}
pub(crate) fn terminal_event(
    run: &Run,
    now: time::OffsetDateTime,
) -> Result<crabber_core::EventRecord, Error> {
    let mut event = crate::attempt_control::terminal_event(run, now, source_turn(run)?);
    event.payload["reason"] = serde_json::json!("host_cancelled_suspension");
    Ok(event)
}

#[path = "suspension/history.rs"]
mod history;
