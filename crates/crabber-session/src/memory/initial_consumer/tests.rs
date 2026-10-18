//! Genuine fixed-task publications followed by adversarial native inspections.
use super::*;
use crate::MemoryStore;
use crate::native_enrollment::initial_consumer::{
    self as protocol, CallKind, Origin, Original, Projection,
};
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus as Status,
    initial_consumer_fixture,
};
use crate::session_owner::preparation::JoinedPreparation;
use crabber_core::{ManualClock, SessionContext};
use crabber_extension::LibraryScriptedSource;
use std::sync::Arc;
pub(super) const MAX: usize = 32 << 20;
pub(super) fn source() -> LibraryScriptedSource {
    LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "p".into(),
            model_id: "m".into(),
        },
        vec!["actual finite scripted answer".into()],
    )
    .unwrap()
}
pub(super) fn store() -> Arc<MemoryStore> {
    Arc::new(MemoryStore::with_clock(Arc::new(ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ))))
}
pub(super) async fn execute(
    store: Arc<dyn crate::Store>,
    context: SessionContext,
    original: JoinedPreparation,
    source: LibraryScriptedSource,
    stop: bool,
) -> Status {
    let owner = PendingDriverTasks::default();
    let mut task =
        PendingInitialProducer::register_scripted(&owner, store, context, original, source)
            .unwrap();
    if stop {
        task.control().request_stop();
    }
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    let status = observed.status().clone();
    assert!(
        !observed.observation_failed(),
        "actual task failed literal observation: {:?}",
        observed.error()
    );
    assert!(
        observed.error().is_none(),
        "actual fixed consumer failed: {:?}, status={status:?}",
        observed.error()
    );
    owner.wait_for_supervision().await;
    status
}
pub(super) async fn completed(keyed: bool) -> (Arc<MemoryStore>, crabber_core::SessionId) {
    let store = store();
    let source = source();
    let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
    let session = context.session_id().clone();
    assert_eq!(
        execute(store.clone(), context, original, source, false).await,
        Status::ClosedAssignment
    );
    (store, session)
}
#[tokio::test]
async fn actual_initial_consumer_members_are_private_paired_and_complete() {
    for keyed in [false, true] {
        let (store, session) = completed(keyed).await;
        let state = store.state.lock().unwrap();
        assert!(routes(&state, &session, &mut Proof::bounded(MAX)).unwrap());
        let origin = state.initial_consumer_cohorts.values().next().unwrap();
        assert!(matches!(origin.as_ref(), Origin::Opened(_)));
        let view = selected(&state, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
        let birth = state
            .pending_initial
            .selected(
                &origin.correlation().key,
                &origin.correlation().identity,
                &mut Proof::bounded(MAX),
            )
            .unwrap();
        view.verify_birth(birth, &mut Proof::bounded(MAX)).unwrap();
        assert!(view.close.is_some() && birth.close.is_some());
        let assignment = view.assignment.unwrap();
        let run = state.runs.get(assignment.run_id()).unwrap();
        assert_eq!(run.status, crabber_core::RunStatus::Completed);
        assert!(state.execution_enrollment_unclassified.is_empty());
        let kinds: Vec<_> = view.results.iter().map(|r| r.original.kind()).collect();
        for kind in [
            CallKind::Register,
            CallKind::Activate,
            CallKind::ReserveInitial,
            CallKind::BeginInitial,
            CallKind::RunAdmitted,
            CallKind::RunStarted,
            CallKind::TurnStarted,
            CallKind::AssistantMessage,
            CallKind::MessageCommitted,
            CallKind::TurnCompleted,
            CallKind::Conclude,
        ] {
            assert!(kinds.contains(&kind));
        }
        assert_eq!(kinds.contains(&CallKind::KeyedBegin), keyed);
        assert_eq!(view.calls.len(), view.roster.len());
        assert_eq!(view.results.len(), view.reverse.len());
        let conclusion = view
            .results
            .iter()
            .find_map(|r| match &r.original {
                Original::Conclude(v) => Some(v),
                _ => None,
            })
            .unwrap();
        assert!(conclusion.attempt.publication.is_some());
        let messages: Vec<_> = state
            .messages
            .iter()
            .filter(|m| {
                m.run_id.as_ref() == Some(assignment.run_id())
                    && m.role == crabber_core::Role::Assistant
            })
            .collect();
        assert_eq!(messages.len(), 1);
        assert!(state.calls.values().all(|c| c.run_id != run.id));
        assert!(state.phases.values().all(|p| p.snapshot.run_id != run.id));
    }
}
#[tokio::test]
async fn every_erased_consumer_projection_is_refused_by_original_membership() {
    for keyed in [false, true] {
        let (store, session) = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let key = genuine.initial_consumer_cohorts.keys().next().unwrap();
        for projection in [
            Projection::Origin,
            Projection::Assignment,
            Projection::Calls,
            Projection::Results,
            Projection::Roster,
            Projection::Reverse,
            Projection::Publications,
            Projection::Head,
            Projection::Close,
        ] {
            let mut changed = genuine.clone();
            changed.initial_consumer.test_erase(key, projection);
            assert!(routes(&changed, &session, &mut Proof::bounded(MAX)).is_err());
            assert!(
                genuine
                    .initial_consumer
                    .test_shared_payloads(&changed.initial_consumer)
                    || matches!(projection, Projection::Results)
            );
        }
        let mut absent = genuine.clone();
        absent.initial_consumer = protocol::Ledger::default();
        assert!(routes(&absent, &session, &mut Proof::bounded(MAX)).is_err());
        assert!(routes(&genuine, &session, &mut Proof::bounded(MAX)).unwrap());
    }
}
#[tokio::test]
async fn exact_current_consumer_proof_boundary_cannot_erase_originals() {
    for keyed in [false, true] {
        let (store, session) = completed(keyed).await;
        let state = store.state.lock().unwrap();
        let mut measured = Proof::bounded(MAX);
        routes(&state, &session, &mut measured).unwrap();
        let used = measured.used();
        assert!(used > 0);
        let mut exact = Proof::bounded(used);
        routes(&state, &session, &mut exact).unwrap();
        assert_eq!(exact.used(), used);
        let mut short = Proof::bounded(used - 1);
        assert!(matches!(
            routes(&state, &session, &mut short),
            Err(Failure::Limited(_))
        ));
        assert!(routes(&state, &session, &mut Proof::bounded(MAX)).unwrap());
    }
}
#[tokio::test]
async fn stop_before_arming_has_no_native_consumer_or_birth_rows() {
    for keyed in [false, true] {
        let store = store();
        let source = source();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        let session = context.session_id().clone();
        assert!(matches!(
            execute(store.clone(), context, original, source, true).await,
            Status::OriginalJoined { .. }
        ));
        let state = store.state.lock().unwrap();
        assert!(state.runs.is_empty());
        assert!(state.initial_consumer_cohorts.is_empty());
        assert!(!state.initial_consumer.contains_scope(&session));
        assert!(state.pending_initial_protocols.is_empty());
        assert!(state.pending_initial.is_empty());
    }
}
#[tokio::test]
async fn coherent_other_genuine_epoch_cannot_replace_original_consumer_winner() {
    for keyed in [false, true] {
        let first = store();
        let first_source = source();
        let (context, original) =
            initial_consumer_fixture::prepared(&*first, keyed, &first_source).await;
        let second = store();
        *second.state.lock().unwrap() = first.state.lock().unwrap().clone();
        let session = context.session_id().clone();
        assert_eq!(
            execute(
                first.clone(),
                context.clone(),
                original.clone(),
                first_source,
                false
            )
            .await,
            Status::ClosedAssignment
        );
        assert_eq!(
            execute(second.clone(), context, original, source(), false).await,
            Status::ClosedAssignment
        );
        let first_state = first.state.lock().unwrap();
        let second_state = second.state.lock().unwrap();
        let a = first_state
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap();
        let b = second_state
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap();
        assert!(a.correlation().key == b.correlation().key);
        assert_ne!(
            a.correlation().identity.epoch,
            b.correlation().identity.epoch
        );
        let mut replaced = first_state.clone();
        replaced.initial_consumer = second_state.initial_consumer.clone();
        assert!(routes(&replaced, &session, &mut Proof::bounded(MAX)).is_err());
        assert!(routes(&first_state, &session, &mut Proof::bounded(MAX)).unwrap());
        assert!(routes(&second_state, &session, &mut Proof::bounded(MAX)).unwrap());
    }
}
#[tokio::test]
async fn public_raw_driver_replay_refuses_the_actual_consumer_before_metadata() {
    use crate::Store;
    for keyed in [false, true] {
        let store = store();
        let source = source();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        // This is an actual original preparation authority, intentionally for a
        // different target; old receipt replay never checked this authority.
        let actual_preparation = original.observed_grant().owner_authority().clone();
        assert_eq!(
            execute(store.clone(), context, original, source, false).await,
            Status::ClosedAssignment
        );
        let (register, activation, run) = {
            let state = store.state.lock().unwrap();
            let origin = state.initial_consumer_cohorts.values().next().unwrap();
            let view = selected(&state, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
            let registered = view
                .results
                .iter()
                .find_map(|r| match &r.original {
                    Original::Register(v) => Some(v),
                    _ => None,
                })
                .unwrap();
            let activated = view
                .results
                .iter()
                .find_map(|r| match &r.original {
                    Original::Activate(v) => Some(v),
                    _ => None,
                })
                .unwrap();
            (
                crate::session_owner::driver::RegisterExecutionDriver {
                    context: SessionContext::new(
                        registered.request.session.clone(),
                        registered.request.workspace.clone(),
                        registered.request.directory.clone(),
                    ),
                    fence: registered.request.fence.clone(),
                },
                activated.request.clone(),
                registered.request.fence.run_id.clone(),
            )
        };
        assert!(matches!(
            store.register_execution_driver(register).await,
            Err(crate::session_owner::SessionOwnerError::Unsupported)
        ));
        assert!(matches!(
            Store::publish_owned_scope_activation(&*store, activation, actual_preparation, None)
                .await,
            Err(crate::session_owner::SessionOwnerError::Unsupported)
        ));
        assert!(matches!(
            store.get_run(&run).await,
            Err(crate::StoreError::HostUnsupported)
        ));
    }
}
#[tokio::test]
async fn intact_consumer_history_cannot_hide_erased_actual_native_event() {
    for keyed in [false, true] {
        let (store, _) = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let correlation = genuine
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap()
            .correlation();
        let view = selected(&genuine, correlation, &mut Proof::bounded(MAX)).unwrap();
        super::native::verify_members(&genuine, &view, &mut Proof::bounded(MAX)).unwrap();
        let cursor = view
            .results
            .iter()
            .find_map(|r| match &r.original {
                Original::Lifecycle(v) if v.kind == protocol::InitialLifecycleKind::RunStarted => {
                    Some(v.cursor)
                }
                _ => None,
            })
            .unwrap();
        let mut erased = genuine.clone();
        assert!(erased.events.remove_for_test(cursor).is_some());
        let intact = selected(&erased, correlation, &mut Proof::bounded(MAX)).unwrap();
        let birth = erased
            .pending_initial
            .selected(
                &correlation.key,
                &correlation.identity,
                &mut Proof::bounded(MAX),
            )
            .unwrap();
        // Projection, exact cohort, roster, original result bodies and append
        // head are all still genuine; only the independently selected native
        // publisher's event has been removed.
        intact
            .verify_birth(birth, &mut Proof::bounded(MAX))
            .unwrap();
        assert!(super::native::verify_members(&erased, &intact, &mut Proof::bounded(MAX)).is_err());
        assert!(
            genuine
                .initial_consumer
                .test_shared_payloads(&erased.initial_consumer)
        );
        super::native::verify_members(&genuine, &view, &mut Proof::bounded(MAX)).unwrap();
    }
}
#[tokio::test]
async fn genuine_raw_completion_is_read_only_and_never_adopted_by_scripted_consumer() {
    for keyed in [false, true] {
        let store = store();
        let source = source();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        let completed = crate::Store::complete_preparation(&*store, original.clone())
            .await
            .unwrap();
        let run = match completed {
            crate::session_owner::preparation::PreparationCompletion::Admitted(v) => v.run.id,
            crate::session_owner::preparation::PreparationCompletion::Keyed(
                crate::KeyedAdmitOutcome::Started { admitted, .. },
            ) => admitted.run.id,
            _ => panic!("actual fresh Raw preparation result"),
        };
        assert_eq!(
            execute(store.clone(), context, original, source, false).await,
            Status::SourceAlreadyCompletedRaw {
                run_id: run.clone()
            }
        );
        let state = store.state.lock().unwrap();
        assert!(state.initial_consumer_cohorts.is_empty());
        assert!(state.pending_initial_protocols.is_empty());
        assert!(
            !state
                .initial_consumer
                .contains_scope(&state.runs[&run].session_id)
        );
        assert!(state.pending_initial.is_empty());
        assert_eq!(
            crate::memory::native_enrollment::original(
                &state,
                &state.runs[&run].session_id,
                &run,
                &mut Proof::bounded(MAX)
            )
            .unwrap(),
            crate::native_enrollment::Status::Unclassified
        );
        assert!(state.attempts.is_empty());
    }
}
