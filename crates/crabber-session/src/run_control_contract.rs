//! Shared exact-target, ordered boundary and durable status conformance.
#[path = "run_control_contract/bounds.rs"]
mod bounds;
#[path = "run_control_contract/checkpoint.rs"]
mod checkpoint;
#[path = "run_control_contract/faults.rs"]
mod faults;
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "run_control_contract/process.rs"]
pub(crate) mod process;
#[path = "run_control_contract/terminal.rs"]
mod terminal;
#[path = "run_control_contract/tools.rs"]
mod tools;
#[path = "run_control_contract/waiting.rs"]
mod waiting;
use crate::{
    AdmitOutcome, ApprovalReply, AttemptAuthority, AttemptCommand, AttemptConclusion, AttemptError,
    AttemptFinish, AttemptMutation, AttemptMutationOutcome, AttemptTarget, ExecutionStore,
    RunBoundary, RunControlAction, RunControlBoundary, RunControlError, RunControlOutcome,
    RunControlReceipt, RunControlRequest, RunControlStatus, RunControlTarget, Store,
};
use crabber_core::{Clock, ControlId, InputEnvelope, ManualClock, RunStatus, TurnId};
pub use faults::run_fault_contract;
use std::sync::Arc;
use time::OffsetDateTime;
fn cancel_action() -> RunControlAction {
    RunControlAction::Cancel {
        resolver: crate::ApprovalResolverClaim {
            resolver_id: "controller".into(),
            authority: crabber_core::AuthorityRequirement::new("host-cancel", "1").unwrap(),
            authority_claim: "host session cancellation".into(),
        },
    }
}
async fn request(
    store: &impl Store,
    target: &RunControlTarget,
    action: RunControlAction,
) -> RunControlRequest {
    RunControlRequest {
        id: ControlId::new(),
        target: target.clone(),
        expected_revision: store.session_revision(&target.session_id).await.unwrap(),
        action,
    }
}
async fn steer(store: &impl Store, target: &RunControlTarget, text: &str) -> RunControlRequest {
    request(
        store,
        target,
        RunControlAction::Steer {
            input: InputEnvelope::text(
                text,
                store.session_revision(&target.session_id).await.unwrap(),
            )
            .unwrap(),
        },
    )
    .await
}
async fn accept(store: &impl Store, request: RunControlRequest) -> RunControlReceipt {
    match store.control_run(request).await.unwrap() {
        RunControlOutcome::Applied(receipt) => receipt,
        RunControlOutcome::Replayed(_) => panic!("fresh acceptance"),
    }
}
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
pub(crate) async fn corruption_fixture(
    store: &impl Store,
) -> (
    RunControlRequest,
    crate::RunControlSummary,
    Box<dyn ExecutionStore>,
) {
    let admitted = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
    let target = RunControlTarget::of(&admitted.run);
    let request = steer(store, &target, "immutable-origin").await;
    accept(store, request.clone()).await;
    let original = store
        .run_controls(&target)
        .await
        .unwrap()
        .controls
        .remove(0);
    let execution = store.execution(admitted.fence).await.unwrap();
    execution
        .observe_run_controls(Some(&TurnId::new()))
        .await
        .unwrap();
    (request, original, execution)
}
#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
pub(crate) async fn termination_fixture(
    store: &impl Store,
) -> (RunControlRequest, ControlId, crabber_core::ContinuationId) {
    let f = crate::tool_continuation_contract::fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority)
        .await
        .unwrap();
    let target = RunControlTarget::of(&f.admitted.run);
    let request = request(store, &target, cancel_action()).await;
    accept(store, request.clone()).await;
    let termination_id = crate::tool_continuation::derived_id("host-cancel", &request.id);
    (request, termination_id, f.suspension.continuation.id)
}
/// All adapters preserve exact retries, ordering, target binding and worker fences.
/// # Panics
/// Panics if the adapter violates durable control semantics.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    ordered(&store, &clock).await;
    cancellation(&store, &clock).await;
    terminal::run(&store, &clock).await;
    bounds::run(&store, &clock).await;
    bounds::race(&store, &clock).await;
    bounds::byte_capacity(&store, &clock).await;
    bounds::operation_domain(&store, &clock).await;
    bounds::opaque_session(&store, &clock).await;
    waiting::run(&store, &clock).await;
    checkpoint::run(&store, &clock).await;
    tools::run(&store).await;
}
async fn ordered(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let first = steer(store, &target, "PRIVATE_FIRST").await;
    let saved = accept(store, first.clone()).await;
    let second = steer(store, &target, "PRIVATE_SECOND").await;
    let saved2 = accept(store, second).await;
    assert_eq!(saved2.sequence, saved.sequence + 1);
    let before = store.session_revision(&target.session_id).await.unwrap();
    assert_eq!(
        store.control_run(first.clone()).await.unwrap(),
        RunControlOutcome::Replayed(saved.clone())
    );
    assert_eq!(
        store.session_revision(&target.session_id).await.unwrap(),
        before
    );
    let mut changed = first.clone();
    changed.action = cancel_action();
    assert_eq!(
        store.control_run(changed).await.unwrap_err(),
        RunControlError::SemanticConflict
    );
    assert_eq!(
        execution
            .observe_run_controls(None)
            .await
            .unwrap()
            .incorporated,
        0
    );
    let turn = TurnId::new();
    assert_eq!(
        execution.observe_run_controls(Some(&turn)).await.unwrap(),
        RunControlBoundary {
            incorporated: 2,
            cancel_observed: false
        }
    );
    assert_eq!(
        execution
            .observe_run_controls(Some(&turn))
            .await
            .unwrap()
            .incorporated,
        0
    );
    let history = store.list_all_messages(&target.session_id).await.unwrap();
    assert_eq!(history.len(), 3);
    assert_eq!(
        history[1].parts[0].content,
        crabber_core::ContentBlock::Text {
            text: "PRIVATE_FIRST".into()
        }
    );
    assert_eq!(
        history[2].parts[0].content,
        crabber_core::ContentBlock::Text {
            text: "PRIVATE_SECOND".into()
        }
    );
    let page = store.run_controls(&target).await.unwrap();
    assert!(page.controls.iter().all(|c| c.status
        == RunControlStatus::Incorporated {
            turn_id: turn.clone()
        }));
    let json = serde_json::to_string(&page.controls).unwrap();
    assert!(!json.contains("PRIVATE_FIRST") && !json.contains(&admitted.fence.claim_token));
    clock.set(clock.now() + time::Duration::seconds(31));
    assert_eq!(
        execution
            .observe_run_controls(Some(&TurnId::new()))
            .await
            .unwrap_err(),
        RunControlError::StaleOwner
    );
    assert_eq!(
        store.control_run(first).await.unwrap(),
        RunControlOutcome::Replayed(saved)
    );
}
async fn cancellation(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let queued = steer(store, &target, "NEVER_INCORPORATE").await;
    accept(store, queued).await;
    let cancel = request(store, &target, cancel_action()).await;
    let receipt = accept(store, cancel.clone()).await;
    let page = store.run_controls(&target).await.unwrap();
    assert_eq!(page.controls[1].status, RunControlStatus::Accepted);
    let observed = execution
        .observe_run_controls(Some(&TurnId::new()))
        .await
        .unwrap();
    assert!(observed.cancel_observed);
    assert_eq!(observed.incorporated, 0);
    assert_eq!(
        store.run_controls(&target).await.unwrap().controls[1].status,
        RunControlStatus::Observed
    );
    assert!(
        !store
            .get_run(&target.run_id)
            .await
            .unwrap()
            .unwrap()
            .status
            .is_terminal()
    );
    assert_eq!(
        store.control_run(cancel).await.unwrap(),
        RunControlOutcome::Replayed(receipt)
    );
    assert_eq!(
        store
            .list_all_messages(&target.session_id)
            .await
            .unwrap()
            .len(),
        1
    );
    let mut wrong = target.clone();
    wrong.config_hash = "changed".into();
    assert_eq!(
        store.run_controls(&wrong).await.unwrap_err(),
        RunControlError::SemanticConflict
    );
    let mut wrong = target.clone();
    wrong.session_id = crabber_core::SessionId::new();
    assert_eq!(
        store.run_controls(&wrong).await.unwrap_err(),
        RunControlError::SemanticConflict
    );
    assert_eq!(
        store.get_run(&target.run_id).await.unwrap().unwrap().status,
        RunStatus::Running
    );
}
