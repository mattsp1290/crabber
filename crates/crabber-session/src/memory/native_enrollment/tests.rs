use super::*;
use crate::{PhaseCommand, PhaseMutation, PhaseMutationOutcome, PhaseWriteAuthority};
use crabber_core::ControlId;

fn proof() -> Proof {
    Proof::bounded(32 << 20)
}

#[tokio::test]
async fn raw_birth_stays_original_through_actual_phase_grants_begin_and_replay() {
    let store = crate::MemoryStore::new();
    let (admitted, execution, attempt, claim) = crate::phase_contract::fixture(&store).await;
    let session = &admitted.session.id;
    let run = &admitted.run.id;
    let scope = (session.clone(), run.clone());
    let (origin, poison) = {
        let state = store.state.lock().unwrap();
        assert_eq!(
            original(&state, session, run, &mut proof()).unwrap(),
            Status::Unclassified
        );
        (
            state.execution_enrollment_origins[&scope].clone(),
            state.execution_enrollment_unclassified[&scope].clone(),
        )
    };
    let (claimed, authority) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(attempt)))
            .await
            .unwrap(),
    );
    let begin = PhaseMutation {
        id: ControlId::new(),
        expected_revision: claimed.revision,
        command: PhaseCommand::Begin {
            target: claimed.phase.target,
        },
    };
    let authority = authority.map(PhaseWriteAuthority::Phase);
    assert!(matches!(
        execution
            .mutate_phase(begin.clone(), authority.clone())
            .await
            .unwrap(),
        PhaseMutationOutcome::Applied { .. }
    ));
    assert!(matches!(
        execution.mutate_phase(begin, authority).await.unwrap(),
        PhaseMutationOutcome::Replayed { .. }
    ));
    let mut state = store.state.lock().unwrap();
    assert!(state.execution_enrollment_origins[&scope] == origin);
    assert!(state.execution_enrollment_unclassified[&scope] == poison);
    assert_eq!(state.execution_enrollment_protocols.len(), 1);
    assert_eq!(
        original(&state, session, run, &mut proof()).unwrap(),
        Status::Unclassified
    );
    // Original negative metadata is independent of later mutable/current head
    // damage. A fresh operation still has the separate strict native19 guard.
    state
        .native_run_source_heads
        .get_mut(&scope)
        .unwrap()
        .version = 999;
    assert_eq!(
        original(&state, session, run, &mut proof()).unwrap(),
        Status::Unclassified
    );
}

#[tokio::test]
async fn original_poison_cohort_budget_and_reverse_run_membership_are_required() {
    let store = crate::MemoryStore::new();
    let (admitted, _, _, _) = crate::phase_contract::fixture(&store).await;
    let session = &admitted.session.id;
    let run = &admitted.run.id;
    let scope = (session.clone(), run.clone());
    let mut state = store.state.lock().unwrap();
    let mut measured = proof();
    original(&state, session, run, &mut measured).unwrap();
    assert!(matches!(
        original(
            &state,
            session,
            run,
            &mut Proof::bounded(measured.used() - 1)
        ),
        Err(Failure::Limited(_))
    ));
    let poison = state
        .execution_enrollment_unclassified
        .remove(&scope)
        .unwrap();
    assert!(original(&state, session, run, &mut proof()).is_err());
    state
        .execution_enrollment_unclassified
        .insert(scope.clone(), poison);
    state.execution_enrollment_protocols.remove(&scope);
    assert!(original(&state, session, run, &mut proof()).is_err());
    state.execution_enrollment_protocols.insert(scope);
    routes(&state, session, &mut proof()).unwrap();
    state.runs.remove(run).unwrap();
    assert!(routes(&state, session, &mut proof()).is_err());
}

#[test]
fn first_raw_admission_uses_remaining_allowance_and_rolls_back_with_its_run() {
    let store = crate::MemoryStore::new();
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let session = SessionId::new();
    let request = crate::admission_execution_contract::keyed(&session, now);
    let mut exhausted = Proof::bounded(32);
    exhausted.charge(32).unwrap();
    assert!(
        store
            .transact(|state| {
                crate::memory::admission::admit_keyed_with_proof(
                    state,
                    &request,
                    now,
                    crate::queue::AdmissionGuard::Ordinary,
                    &mut exhausted,
                )
            })
            .is_err()
    );
    {
        let state = store.state.lock().unwrap();
        assert!(state.runs.is_empty());
        assert!(state.receipts.is_empty());
        assert!(state.execution_enrollment_protocols.is_empty());
        assert!(state.execution_enrollment_origins.is_empty());
        assert!(state.execution_enrollment_unclassified.is_empty());
    }
    let outcome = store
        .transact(|state| {
            crate::memory::admission::admit_keyed_with_proof(
                state,
                &request,
                now,
                crate::queue::AdmissionGuard::Ordinary,
                &mut proof(),
            )
        })
        .unwrap();
    let crate::KeyedAdmitOutcome::Started { admitted, .. } = outcome else {
        panic!("fresh admission")
    };
    let scope = (session.clone(), admitted.run.id.clone());
    let poison = store
        .state
        .lock()
        .unwrap()
        .execution_enrollment_unclassified[&scope]
        .clone();
    let replay = store
        .transact(|state| {
            crate::memory::admission::admit_keyed_with_proof(
                state,
                &request,
                now,
                crate::queue::AdmissionGuard::Ordinary,
                &mut Proof::bounded(0),
            )
        })
        .unwrap();
    assert!(matches!(replay, crate::KeyedAdmitOutcome::Replayed(_)));
    let state = store.state.lock().unwrap();
    assert_eq!(state.execution_enrollment_origins.len(), 1);
    assert!(state.execution_enrollment_unclassified[&scope] == poison);
    assert_eq!(
        original(&state, &session, &admitted.run.id, &mut proof()).unwrap(),
        Status::Unclassified
    );
}
