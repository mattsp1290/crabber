use super::{
    Clock, ManualClock, RunControlError, RunControlOutcome, RunControlTarget, Store, TurnId,
    accept, steer,
};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let oversized = steer(store, &target, &"x".repeat(65_536)).await;
    assert_eq!(
        store.control_run(oversized).await.unwrap_err(),
        RunControlError::InvalidRequest
    );
    let mut first = None;
    for index in 0..64 {
        let request = steer(store, &target, "bounded").await;
        let receipt = accept(store, request.clone()).await;
        if index == 0 {
            first = Some((request, receipt));
        }
    }
    let extra = steer(store, &target, "overflow").await;
    assert_eq!(
        store.control_run(extra).await.unwrap_err(),
        RunControlError::Capacity
    );
    let (request, receipt) = first.unwrap();
    assert_eq!(
        store.control_run(request).await.unwrap(),
        RunControlOutcome::Replayed(receipt)
    );
    assert_eq!(
        store.run_controls(&target).await.unwrap().controls.len(),
        64
    );
    let execution = store.execution(admitted.fence).await.unwrap();
    assert_eq!(
        execution
            .observe_run_controls(Some(&TurnId("x".repeat(513))))
            .await
            .unwrap_err(),
        RunControlError::InvalidRequest
    );
    assert_eq!(
        execution
            .observe_run_controls(Some(&TurnId::new()))
            .await
            .unwrap()
            .incorporated,
        64
    );
}
pub(super) async fn race(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let a = steer(store, &target, "a").await;
    let b = steer(store, &target, "b").await;
    let (a, b) = futures::join!(store.control_run(a), store.control_run(b));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(
        matches!(a, Err(RunControlError::RevisionConflict))
            || matches!(b, Err(RunControlError::RevisionConflict))
    );
    let page = store.run_controls(&target).await.unwrap();
    assert_eq!(page.controls.len(), 1);
    assert_eq!(page.controls[0].receipt.sequence, 1);
}
pub(super) async fn byte_capacity(store: &impl Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let target = RunControlTarget::of(&admitted.run);
    let mut accepted = 0;
    loop {
        let request = steer(store, &target, &"x".repeat(60_000)).await;
        let before = store.session_revision(&target.session_id).await.unwrap();
        match store.control_run(request.clone()).await {
            Ok(RunControlOutcome::Applied(receipt)) => {
                accepted += 1;
                assert_eq!(receipt.sequence, accepted);
            }
            Err(RunControlError::Capacity) => {
                assert_eq!(
                    store.session_revision(&target.session_id).await.unwrap(),
                    before
                );
                assert!(
                    store
                        .lookup_run_control(&target.session_id, &request.id)
                        .await
                        .unwrap()
                        .is_none()
                );
                break;
            }
            outcome => panic!("unexpected byte-capacity outcome: {outcome:?}"),
        }
        assert!(accepted < 64);
    }
    assert!(accepted > 0);
    let execution = store.execution(admitted.fence).await.unwrap();
    let turn = TurnId("\\\"".repeat(256));
    assert_eq!(
        execution
            .observe_run_controls(Some(&turn))
            .await
            .unwrap()
            .incorporated as u64,
        accepted
    );
    assert_eq!(
        store.run_controls(&target).await.unwrap().controls.len() as u64,
        accepted
    );
}

pub(super) async fn operation_domain(store: &impl Store, clock: &ManualClock) {
    for control_first in [false, true] {
        let admitted = crate::public_state_contract::admit(store, clock.now()).await;
        let target = RunControlTarget::of(&admitted.run);
        let control = steer(store, &target, "shared-key").await;
        let execution = store.execution(admitted.fence).await.unwrap();
        let mutation = crate::AttemptMutation {
            id: control.id.clone(),
            expected_revision: control.expected_revision,
            command: crate::AttemptCommand::Reserve {
                target: crate::AttemptTarget {
                    id: crabber_core::AttemptId::new(),
                    generation: crabber_core::AttemptGeneration::INITIAL,
                },
            },
        };
        if control_first {
            accept(store, control).await;
            assert_eq!(
                execution.mutate_attempt(mutation, None).await.unwrap_err(),
                crate::AttemptError::SemanticConflict
            );
        } else {
            execution.mutate_attempt(mutation, None).await.unwrap();
            assert_eq!(
                store.control_run(control).await.unwrap_err(),
                RunControlError::SemanticConflict
            );
        }
    }
}
pub(super) async fn opaque_session(store: &impl Store, clock: &ManualClock) {
    let session = crabber_core::SessionId(format!(
        "opaque:λ/{}/{}",
        crabber_core::SessionId::new(),
        "x".repeat(180)
    ));
    let mut prepared =
        crate::typed_admission_contract::fixture(&session, clock.now(), "opaque-controls");
    prepared.keyed.request.session_id = None;
    let admitted = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: prepared.keyed.request,
            binding: prepared.binding,
        })
        .await
        .unwrap();
    let binding = store
        .run_control_binding(&session, &admitted.run.id)
        .await
        .unwrap();
    assert_eq!(binding.target.session_id, session);
    assert_eq!(
        binding.revision,
        store.session_revision(&session).await.unwrap()
    );
    let request = super::request(store, &binding.target, super::cancel_action()).await;
    accept(store, request).await;
    assert_eq!(
        store
            .run_controls(&binding.target)
            .await
            .unwrap()
            .controls
            .len(),
        1
    );
}
