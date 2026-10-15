//! Backend fixtures for independently persisted phase liveness predicates.
use super::applied;
use crate::{
    AdmitOutcome, AttemptAuthority, AttemptCommand, AttemptFinish, AttemptMutation, AttemptTarget,
    ExecutionStore, Store,
};
use crabber_core::{
    ControlId, EventKind, EventRecord, SessionRevision, ToolCallId, ToolCallRecord, ToolCallStatus,
};

pub(crate) struct Fixture {
    pub admitted: AdmitOutcome,
    pub execution: Box<dyn ExecutionStore>,
    pub target: AttemptTarget,
    pub authority: Option<AttemptAuthority>,
    pub call: ToolCallId,
    pub revision: SessionRevision,
}
pub(crate) async fn fixture(store: &impl Store) -> Fixture {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::now_utc()).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let request = super::reserve(store.session_revision(&admitted.session.id).await.unwrap());
    let (receipt, authority) = applied(execution.mutate_attempt(request, None).await.unwrap());
    let target = receipt.attempt.target;
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: target.clone(),
                },
            },
            authority.clone(),
        )
        .await
        .unwrap();
    let call = ToolCallId::new();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: call.clone(),
                run_id: admitted.run.id.clone(),
                name: "phase-fixture".into(),
                arguments: serde_json::json!({}),
                status: ToolCallStatus::Pending,
                result: None,
                retry_safe: false,
            },
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({}),
                correlation: None,
                live_only: false,
                created_at: admitted.run.created_at,
            },
        )
        .await
        .unwrap();
    let (finished, _) = applied(
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                    command: AttemptCommand::Finish {
                        target: target.clone(),
                        outcome: AttemptFinish::Suspended,
                    },
                },
                authority.clone(),
            )
            .await
            .unwrap(),
    );
    Fixture {
        admitted,
        execution,
        target,
        authority,
        call,
        revision: finished.revision,
    }
}
