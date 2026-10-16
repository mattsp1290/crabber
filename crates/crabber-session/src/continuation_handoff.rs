//! Immutable cross-domain suspension ownership transfer; contains no grants.
use crabber_core::{ControlId, EventCursor, InputFingerprint};
use serde::{Deserialize, Serialize};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Domain {
    Approval,
    Tool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub domain: Domain,
    pub continuation: crate::ContinuationTarget,
    pub operation_id: ControlId,
    pub digest: InputFingerprint,
    pub cursor: EventCursor,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Handoff {
    pub predecessor: Source,
    pub successor_domain: Domain,
    pub successor: crate::ContinuationTarget,
    pub suspension_id: ControlId,
    pub suspension_digest: InputFingerprint,
    pub plan: InputFingerprint,
    pub parent: crate::AttemptTarget,
}
impl Handoff {
    pub fn validate(&self) -> Result<(), ()> {
        if self.predecessor.domain == self.successor_domain || self.predecessor.cursor.0 == 0 {
            return Err(());
        }
        for id in [
            &self.predecessor.continuation.id.0,
            &self.successor.id.0,
            &self.predecessor.operation_id.0,
            &self.suspension_id.0,
            &self.parent.id.0,
        ] {
            crate::attempt::validate_identifier(id).map_err(|_| ())?;
        }
        Ok(())
    }
    pub fn from_event(event: &crabber_core::EventRecord) -> Result<Option<Self>, ()> {
        let Some(value) = event.payload.get("handoff") else {
            return Ok(None);
        };
        if crate::snapshot::encoded_bytes(value, 4096).is_none() {
            return Err(());
        }
        let proof: Self = serde_json::from_value(value.clone()).map_err(|_| ())?;
        proof.validate()?;
        Ok(Some(proof))
    }
}

pub(crate) fn verify_native_origin(
    batch: &crate::ToolContinuationSuspension,
    native: &crate::AttemptReceipt,
    source: &crabber_core::EventRecord,
) -> Result<(), crate::ToolContinuationError> {
    let conclusion = crate::AttemptConclusion {
        finish: crate::AttemptMutation {
            id: crate::tool_continuation::derived_id(
                "tool-continuation-suspend-attempt",
                &batch.id,
            ),
            expected_revision: batch.expected_revision,
            command: crate::AttemptCommand::Finish {
                target: batch.attempt.clone(),
                outcome: crate::AttemptFinish::Suspended,
            },
        },
        boundary: crate::RunBoundary::Suspended {
            checkpoint: crate::tool_continuation::checkpoint(batch),
            usage: batch.usage.clone(),
        },
        created_at: batch.source_created_at,
        turn_id: batch.source_turn_id.clone(),
        elapsed: std::time::Duration::ZERO,
    };
    conclusion
        .verify_replay(native)
        .map_err(|_| crate::ToolContinuationError::UnknownStoreOutcome)?;
    if source.session_id != native.attempt.session_id
        || source.run_id != native.attempt.run_id
        || source.turn_id != batch.source_turn_id
        || source.created_at != batch.source_created_at
        || source.kind != crabber_core::EventKind::RunPaused
        || source.live_only
        || source.correlation.is_some()
        || source.payload != serde_json::json!({"usage":batch.usage})
        || source.cursor.is_none_or(|c| c >= native.event_cursor)
    {
        return Err(crate::ToolContinuationError::UnknownStoreOutcome);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn claimed_fixture(
    store: &impl crate::Store,
    clock: &crabber_core::ManualClock,
    domain: Domain,
) -> (crabber_core::Run, crate::AttemptTarget) {
    use crate::approval_contract::fixture;
    match domain {
        Domain::Tool => {
            let f = fixture::staged(store, clock).await;
            f.execution
                .suspend_for_approval(f.batch.clone(), f.owner.clone())
                .await
                .unwrap();
            for (index, call) in f.batch.calls.iter().enumerate() {
                if matches!(call.decision, crate::CallApproval::Ask { .. }) {
                    store
                        .answer_approval(
                            fixture::answer(store, &f, index, crate::ApprovalReply::Grant).await,
                        )
                        .await
                        .unwrap();
                }
            }
            store
                .claim_approval_continuation(fixture::claim(store, &f).await)
                .await
                .unwrap();
            (
                store.get_run(&f.run.id).await.unwrap().unwrap(),
                f.batch.next_attempt,
            )
        }
        Domain::Approval => {
            let f = crate::tool_continuation_contract::corruption::setup(store, clock, 5).await;
            (
                store.get_run(&f.admitted.run.id).await.unwrap().unwrap(),
                f.suspension.next_attempt,
            )
        }
    }
}
