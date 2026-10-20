//! Genuine original writes followed by coherent mutations of older mutable carriers.
use super::{Domain, Scope};
use crate::{Store, approval::ApprovalRecord, tool_continuation::ToolContinuationRecord};
use crabber_core::{ControlId, ManualClock};
use serde_json::Value;
pub(crate) struct Case {
    pub scope: Scope,
    pub operation: ControlId,
    pub answer: bool,
}
pub(crate) async fn fixture(store: &impl Store, domain: Domain, answer: bool) -> Case {
    let clock = ManualClock::new(time::OffsetDateTime::UNIX_EPOCH);
    let (scope, operation) = match domain {
        Domain::Approval => {
            let f = crate::approval_contract::fixture::staged(store, &clock).await;
            f.execution
                .suspend_for_approval(f.batch.clone(), f.owner.clone())
                .await
                .unwrap();
            let operation = if answer {
                let a = crate::approval_contract::fixture::answer(
                    store,
                    &f,
                    0,
                    crate::ApprovalReply::Grant,
                )
                .await;
                store.answer_approval(a.clone()).await.unwrap();
                a.id
            } else {
                f.batch.id.clone()
            };
            (
                Scope {
                    domain,
                    session: f.run.session_id,
                    run: f.run.id,
                    continuation: f.batch.continuation,
                },
                operation,
            )
        }
        Domain::Tool => {
            let f = crate::tool_continuation_contract::fixture::fixture(store).await;
            f.execution
                .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
                .await
                .unwrap();
            let operation = if answer {
                let a = crate::tool_continuation_contract::fixture::answer(store, &f, 0).await;
                store.answer_tool_interaction(a.clone()).await.unwrap();
                a.id
            } else {
                f.suspension.id.clone()
            };
            (
                Scope {
                    domain,
                    session: f.admitted.session.id,
                    run: f.admitted.run.id,
                    continuation: f.suspension.continuation,
                },
                operation,
            )
        }
    };
    Case {
        scope,
        operation,
        answer,
    }
}
/// Rehash the original semantic retry digest, deliberately leaving the birth source intact.
pub(crate) fn rewrite(case: &Case, record: Value, receipt: Value) -> (Value, Value) {
    match case.scope.domain {
        Domain::Approval => {
            let mut r: ApprovalRecord = serde_json::from_value(record).unwrap();
            let mut receipt: crate::ApprovalReceipt = serde_json::from_value(receipt).unwrap();
            receipt.semantic_digest = if case.answer {
                let q = &mut r.questions[0];
                q.view.state = crabber_core::InteractionState::Denied;
                let a = q.answer.as_mut().unwrap();
                a.reply = crate::ApprovalReply::Deny;
                let crate::ApprovalOutcome::Answered { state, .. } = &mut receipt.outcome else {
                    panic!("answer")
                };
                *state = crabber_core::InteractionState::Denied;
                a.fingerprint().unwrap()
            } else {
                let crate::CallApproval::Ask { description, .. } = &mut r.batch.calls[0].decision
                else {
                    panic!("ask")
                };
                *description = crabber_core::InteractionDescription::new(
                    description.name(),
                    description.kind(),
                    "REPLACED HUMAN PROMPT",
                    description.response_schema().clone(),
                    description.authority().clone(),
                    description.expires_after_ms(),
                )
                .unwrap();
                r.batch.fingerprint(&r.session_id, &r.run_id).unwrap()
            };
            (
                serde_json::to_value(r).unwrap(),
                serde_json::to_value(receipt).unwrap(),
            )
        }
        Domain::Tool => {
            let mut r: ToolContinuationRecord = serde_json::from_value(record).unwrap();
            let mut receipt: crate::ToolContinuationReceipt =
                serde_json::from_value(receipt).unwrap();
            receipt.semantic_digest = if case.answer {
                let a = r.questions[0].answer.as_mut().unwrap();
                a.reply = crate::ToolInteractionReply::Answer {
                    value: serde_json::json!("REPLACED HUMAN ANSWER"),
                };
                a.fingerprint().unwrap()
            } else {
                r.batch.phases[0].checkpoint = crabber_core::ToolCheckpoint::new(
                    "fixture",
                    "1",
                    serde_json::json!({"private":"REPLACED CHECKPOINT"}),
                )
                .unwrap();
                r.batch.fingerprint(&r.session_id, &r.run_id).unwrap()
            };
            (
                serde_json::to_value(r).unwrap(),
                serde_json::to_value(receipt).unwrap(),
            )
        }
    }
}
pub(crate) fn event(case: &Case, receipt: &Value, event: &mut crabber_core::EventRecord) {
    event.payload["semantic_digest"] = receipt["semantic_digest"].clone();
    event.payload["outcome"] = receipt["outcome"].clone();
    match case.scope.domain {
        Domain::Approval => crate::approval::verify_receipt(
            &serde_json::from_value(receipt.clone()).unwrap(),
            event,
        )
        .unwrap(),
        Domain::Tool => crate::tool_continuation::verify_receipt(
            &serde_json::from_value(receipt.clone()).unwrap(),
            event,
        )
        .unwrap(),
    }
}
