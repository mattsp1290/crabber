//! Real protected Queue selection and actual Initial admission budget boundaries.
use super::{MemoryStore, QueueMutation, StoreError, selection};
use crate::native_enrollment::caller::initial::{
    input_keyed, with_queue_input, with_queue_input_async,
};
use crate::public_session::v2::proof::Proof;
use crate::{QueueAction, Store};

async fn prepared() -> (MemoryStore, QueueMutation) {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let fixture = crate::queue_contract::fixture(&store).await;
    store.mutate_queue(fixture.enqueue.clone()).await.unwrap();
    let authority = crate::queue_contract::claim(&store, &fixture).await;
    let prepare = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(fixture.prepared.clone()),
        },
    )
    .await;
    store.mutate_queue(prepare).await.unwrap();
    let request = crate::queue_contract::mutation(
        &store,
        &fixture,
        QueueAction::Admit {
            authority,
            owner: "selected-input".into(),
            lease: std::time::Duration::from_secs(30),
        },
    )
    .await;
    (store, request)
}
fn selected<'a>(
    store: &MemoryStore,
    state: &'a crate::memory::State,
    summary: &crate::SessionSummary,
    request: &QueueMutation,
    proof: &mut Proof,
) -> crate::queue::VerifiedAdmission<'a> {
    let selected = selection::select(state, request, proof)
        .unwrap()
        .into_parts(request)
        .unwrap();
    crate::queue::apply_guarded(
        request,
        summary,
        selected.head.as_ref(),
        selected.original.map(|record| (record, selected.prepared)),
        0,
        store.clock.now(),
        None,
    )
    .unwrap()
    .admission
    .unwrap()
}
#[tokio::test]
async fn raw_initial_queue_selected_budget_and_rollback() {
    let (store, request) = prepared().await;
    let before = store.state.lock().unwrap().clone();
    let mut measured = Proof::bounded(32 << 20);
    store
        .queue_mutate_with_proof(&request, &mut measured)
        .unwrap();
    let exact = measured.used();
    *store.state.lock().unwrap() = before.clone();
    let mut proof = Proof::bounded(exact);
    store.queue_mutate_with_proof(&request, &mut proof).unwrap();
    assert_eq!(proof.used(), exact);
    *store.state.lock().unwrap() = before;
    let before = crate::native_enrollment::caller::contract::Harness::initial_retained(
        &store,
        &request.session_id,
    )
    .await;
    store
        .native_raw_probe
        .arm_initial(&request.session_id, false);
    assert!(
        store
            .queue_mutate_with_proof(&request, &mut Proof::bounded(exact - 1))
            .is_err()
    );
    assert_eq!(
        store.native_raw_probe.hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(
        crate::native_enrollment::caller::contract::Harness::initial_retained(
            &store,
            &request.session_id
        )
        .await,
        before
    );
}
#[tokio::test]
async fn raw_initial_queue_selected_reuse_wrong_body_proof_and_replacement() {
    let (store, request) = prepared().await;
    let summary = store.queue_session_snapshot(&request.session_id).unwrap();
    let state = store.state.lock().unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let admission = selected(&store, &state, &summary, &request, &mut proof);
    let used = proof.used();
    with_queue_input(&admission, &mut proof, |selected, proof| {
        input_keyed(&admission.bound_ref().keyed, Some(selected), proof).unwrap();
        assert_eq!(proof.used(), used);
        let independent_copy = admission.bound_ref().keyed.clone();
        assert!(input_keyed(&independent_copy, Some(selected), proof).is_err());
        assert!(
            input_keyed(
                &admission.bound_ref().keyed,
                Some(selected),
                &mut Proof::bounded(32 << 20)
            )
            .is_err()
        );
        Ok(())
    })
    .unwrap();
    let result = with_queue_input(&admission, &mut proof, |selected, proof| {
        *proof = Proof::bounded(32 << 20);
        assert!(input_keyed(&admission.bound_ref().keyed, Some(selected), proof).is_err());
        Ok(())
    });
    assert_eq!(result.unwrap_err(), StoreError::Conflict);
}
#[tokio::test]
async fn raw_initial_queue_selected_panic_and_cancellation_fail_closed() {
    let (store, request) = prepared().await;
    let summary = store.queue_session_snapshot(&request.session_id).unwrap();
    let state = store.state.lock().unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let admission = selected(&store, &state, &summary, &request, &mut proof);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: Result<(), _> =
            with_queue_input(&admission, &mut proof, |_, _| panic!("scoped test panic"));
    }));
    assert!(panic.is_err());
    assert_eq!(
        with_queue_input(&admission, &mut proof, |_, _| Ok(())).unwrap_err(),
        StoreError::Conflict
    );
    let mut proof = Proof::bounded(32 << 20);
    let admission = selected(&store, &state, &summary, &request, &mut proof);
    let mut cancelled = Box::pin(with_queue_input_async::<()>(
        &admission,
        &mut proof,
        |_, _| Box::pin(std::future::pending()),
    ));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(cancelled.as_mut(), &mut context).is_pending());
    drop(cancelled);
    assert_eq!(
        with_queue_input(&admission, &mut proof, |_, _| Ok(())).unwrap_err(),
        StoreError::Conflict
    );
}
