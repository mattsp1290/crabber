//! Positive native evidence decides whether original-turn execution is permitted.
use super::*;
use crabber::session::{AttemptCleanup, AttemptError, AttemptState, PhasePageRequest};

pub(super) async fn resume_without_native(
    agent: &Agent,
    store: &dyn Store,
    receipt: &AdmissionReceipt,
) {
    let before = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_eq!(before.status, RunStatus::Running);
    assert!(before.checkpoint.is_none());
    assert!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&receipt.run_id)
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        store
            .list_events(&receipt.session_id, None, 1000)
            .await
            .unwrap(),
        vec![]
    );
    assert_eq!(
        store
            .list_all_messages(&receipt.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    let retained = store
        .load_admission_execution(&receipt.session_id, &options().key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.state, AdmissionExecutionState::Started);
    assert_eq!(retained.receipt, *receipt);
    let result = agent.resume(&receipt.run_id).await.unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let after = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_ne!(after.claim_token, before.claim_token);
    let native = store
        .latest_execution_attempt(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(native.attempt.state, AttemptState::Completed);
    assert_eq!(native.attempt.cleanup, AttemptCleanup::Complete);
    completed(store, receipt).await;
    let events = store
        .list_events(&receipt.session_id, None, 1000)
        .await
        .unwrap();
    for kind in [
        crabber::core::EventKind::RunAdmitted,
        crabber::core::EventKind::RunStarted,
        crabber::core::EventKind::TurnStarted,
        crabber::core::EventKind::RunSettled,
    ] {
        assert_eq!(events.iter().filter(|event| event.kind == kind).count(), 1);
    }
}
pub(super) async fn refuse_started(agent: &Agent, store: &dyn Store, receipt: &AdmissionReceipt) {
    let before = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    let attempt = store
        .latest_execution_attempt(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt.attempt.state, AttemptState::Started);
    let phases = agent
        .list_tool_phases(PhasePageRequest {
            session_id: receipt.session_id.clone(),
            run_id: receipt.run_id.clone(),
            limit: 64,
            after: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(phases.phases, vec![]);
    let events = store
        .list_events(&receipt.session_id, None, 1000)
        .await
        .unwrap();
    let history = store.list_all_messages(&receipt.session_id).await.unwrap();
    let revision = store.session_revision(&receipt.session_id).await.unwrap();
    assert!(matches!(
        agent.resume(&receipt.run_id).await,
        Err(RuntimeError::Attempt(AttemptError::Busy))
    ));
    assert_eq!(
        store.get_run(&receipt.run_id).await.unwrap().unwrap(),
        before
    );
    assert_eq!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .unwrap(),
        attempt
    );
    assert_eq!(
        store.session_revision(&receipt.session_id).await.unwrap(),
        revision
    );
    assert_eq!(
        store
            .list_events(&receipt.session_id, None, 1000)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        store.list_all_messages(&receipt.session_id).await.unwrap(),
        history
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&receipt.run_id)
            .await
            .unwrap(),
        vec![]
    );
    assert!(
        !events
            .iter()
            .any(|event| event.kind == crabber::core::EventKind::RunSettled)
    );
}
