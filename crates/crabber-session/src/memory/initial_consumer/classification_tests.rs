//! Executed sources, authenticated failed prefixes and unsupported Raw are distinct.
use super::tests::{MAX, completed, source, store};
use super::{Proof, classify_run};
use crate::native_enrollment::initial_consumer::{
    CallKind, InitialConsumption, Original, Projection,
};
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, initial_consumer_fixture,
};
#[tokio::test]
async fn register_activate_ack_loss_is_only_an_authenticated_closed_prefix() {
    for keyed in [false, true] {
        for (kind, boundary) in [
            (CallKind::Register, 1),
            (CallKind::Register, 2),
            (CallKind::Activate, 1),
            (CallKind::Activate, 2),
        ] {
            let store = store();
            let source = source();
            let (context, original) =
                initial_consumer_fixture::prepared(&*store, keyed, &source).await;
            let owner = PendingDriverTasks::default();
            let mut task = PendingInitialProducer::register_scripted(
                &owner,
                super::fault_tests::business_fault(store.clone(), kind, boundary),
                context,
                original,
                source,
            )
            .unwrap();
            task.start().unwrap();
            let observed = task.join().await.unwrap();
            assert!(observed.error().is_some());
            assert!(!observed.observation_failed());
            owner.wait_for_supervision().await;
            let state = store.state.lock().unwrap();
            let run = state.runs.values().next().unwrap();
            assert_eq!(
                classify_run(&state, run, &mut Proof::bounded(MAX)).unwrap(),
                InitialConsumption::AuthenticatedClosedPrefix
            );
        }
    }
}
#[tokio::test]
async fn genuine_raw_preparation_is_unproven_without_retrospective_consumer_adoption() {
    for keyed in [false, true] {
        let store = store();
        let source = source();
        let (_, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        crate::Store::complete_preparation(&*store, original)
            .await
            .unwrap();
        let state = store.state.lock().unwrap();
        let run = state.runs.values().next().unwrap();
        assert_eq!(
            classify_run(&state, run, &mut Proof::bounded(MAX)).unwrap(),
            InitialConsumption::Unproven
        );
        assert!(state.initial_consumer_cohorts.is_empty());
        assert!(state.pending_initial_protocols.is_empty());
    }
}
#[tokio::test]
async fn completed_source_requires_each_independent_native_and_reverse_original() {
    for keyed in [false, true] {
        let (store, _) = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let run = genuine.runs.values().next().unwrap();
        let expected = genuine
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap()
            .correlation();
        let view = super::selected(&genuine, expected, &mut Proof::bounded(MAX)).unwrap();
        for loss in 0..13 {
            let mut changed = genuine.clone();
            erase_independent_original(&mut changed, run, &view, loss);
            let result = classify_run(&changed, run, &mut Proof::bounded(MAX));
            drop(changed); // Every damaged snapshot is gone before assertions.
            assert!(
                matches!(
                    result,
                    Err(crate::public_session::v2::proof::Failure::Error(_))
                ),
                "actual original loss={loss}, keyed={keyed}"
            );
            assert_eq!(
                classify_run(&genuine, run, &mut Proof::bounded(MAX)).unwrap(),
                InitialConsumption::CompletedSource
            );
        }
    }
}

/// Each mutation targets a different real native original or protected reverse
/// projection. The caller owns and discards its separate corrupted snapshot.
fn erase_independent_original(
    changed: &mut crate::memory::State,
    run: &crabber_core::Run,
    view: &crate::native_enrollment::initial_consumer::Selection<'_>,
    loss: usize,
) {
    let first = (run.session_id.clone(), run.id.clone(), 1);
    let expected = view.origin.unwrap().correlation();
    match loss {
        0 => {
            assert!(changed.native_run_issuances.remove(&first).is_some());
        }
        1 => {
            assert!(changed.native_run_sources.remove(&first).is_some());
        }
        2 => {
            let audit = view.close.unwrap().retirement.audit().unwrap();
            changed
                .session_owners
                .erase_historical_audit_for_test(&run.session_id, &audit.receipt.operation_id);
        }
        3 => {
            let conclusion = view
                .results
                .iter()
                .find_map(|result| match &result.original {
                    Original::Conclude(value) => Some(value),
                    _ => None,
                })
                .unwrap();
            assert!(
                changed
                    .operation_receipts
                    .remove(&(
                        run.session_id.clone(),
                        conclusion.attempt.receipt.operation_id.clone()
                    ))
                    .is_some()
            );
        }
        4 => {
            let message = view
                .results
                .iter()
                .find_map(|result| match &result.original {
                    Original::AssistantMessage(value) => Some(&value.message.id),
                    _ => None,
                })
                .unwrap();
            changed
                .messages
                .iter_mut()
                .find(|actual| actual.id == *message)
                .unwrap()
                .parts
                .clear();
        }
        5..=9 => {
            let projection = [
                Projection::Reverse,
                Projection::Publications,
                Projection::Head,
                Projection::Results,
                Projection::Close,
            ][loss - 5];
            changed
                .initial_consumer
                .test_erase(&expected.key, projection);
        }
        10 => {
            changed.initial_consumer =
                crate::native_enrollment::initial_consumer::Ledger::default();
        }
        11 => {
            assert!(
                changed
                    .execution_enrollment_origins
                    .remove(&(run.session_id.clone(), run.id.clone()))
                    .is_some()
            );
        }
        12 => {
            assert!(
                changed
                    .execution_enrollment_origins
                    .remove(&(run.session_id.clone(), run.id.clone()))
                    .is_some()
            );
            assert!(
                changed
                    .execution_enrollment_protocols
                    .remove(&(run.session_id.clone(), run.id.clone()))
            );
        }
        _ => unreachable!(),
    }
}
