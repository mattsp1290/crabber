//! Original unkeyed input is authenticated without creating a keyed receipt.
use super::*;
#[tokio::test]
async fn unkeyed_history_authenticates_original_capsule_before_policy() {
    let store = MemoryStore::new();
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(&store).await;
    let mut state = store.state.lock().unwrap();
    let mut proof = bounded();
    let run = state.runs.get(&admitted.run.id).unwrap();
    let actual = report(&state, run, &mut proof).unwrap();
    assert_eq!(
        actual.initial_source,
        crate::deletion::history::Fact::Authenticated
    );
    assert_eq!(
        actual.resource_policy,
        crate::deletion::history::Fact::Unproven
    );
    assert_eq!(actual.main_join, crate::deletion::history::Fact::Unproven);
    assert!(!actual.complete());
    let used = proof.used();
    report(&state, run, &mut Proof::bounded(used)).unwrap();
    assert!(matches!(
        report(&state, run, &mut Proof::bounded(used - 1)),
        Err(Failure::Limited(_))
    ));
    assert!(state.receipts.is_empty());
    assert!(!state.admission_executions.contains_key(&run.id));
    let original = state
        .execution_bindings
        .get(&admitted.run.id)
        .unwrap()
        .clone();
    let mut binding: crate::RetainedExecutionBinding = serde_json::from_str(&original).unwrap();
    let original_plan = binding.capsule.plan_fingerprint.clone();
    binding.capsule.plan_fingerprint = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    binding.capsule.input_fingerprint = binding.capsule.computed_input_fingerprint().unwrap();
    state.execution_bindings.insert(
        admitted.run.id.clone(),
        serde_json::to_string(&binding).unwrap(),
    );
    state
        .runs
        .get_mut(&admitted.run.id)
        .unwrap()
        .plan_fingerprint = binding.capsule.plan_fingerprint.as_str().into();
    super::super::super::super::execution_binding::read_with_proof(
        &state,
        &admitted.session.id,
        &admitted.run.id,
        &mut bounded(),
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        report(
            &state,
            state.runs.get(&admitted.run.id).unwrap(),
            &mut bounded()
        ),
        Err(Failure::Error(_))
    ));
    state
        .execution_bindings
        .insert(admitted.run.id.clone(), original);
    state
        .runs
        .get_mut(&admitted.run.id)
        .unwrap()
        .plan_fingerprint = original_plan.as_str().into();
    state.execution_bindings.remove(&admitted.run.id);
    assert!(matches!(
        report(
            &state,
            state.runs.get(&admitted.run.id).unwrap(),
            &mut bounded()
        ),
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn unkeyed_legacy_absence_never_authenticates_an_owned_initial_source() {
    let store = MemoryStore::new();
    let admitted =
        crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
    let state = store.state.lock().unwrap();
    let actual = report(
        &state,
        state.runs.get(&admitted.run.id).unwrap(),
        &mut bounded(),
    )
    .unwrap();
    assert_eq!(
        actual.initial_source,
        crate::deletion::history::Fact::Unproven
    );
    assert!(!actual.complete());
}
