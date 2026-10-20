//! Real fixed-source execution reaches the production history/readiness path.
use super::*;
use crate::deletion::coverage::{CoverageRead, MAX_PROOF_BYTES};
use crate::native_enrollment::initial_consumer::InitialConsumption;
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
    initial_consumer_fixture,
};
use crate::{DeletionBlocker, MemoryStore};
use std::sync::Arc;
async fn completed(keyed: bool) -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::with_clock(Arc::new(
        crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH),
    )));
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "p".into(),
            model_id: "m".into(),
        },
        vec!["genuine completed source".into()],
    )
    .unwrap();
    let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
    let owner = PendingDriverTasks::default();
    let mut task =
        PendingInitialProducer::register_scripted(&owner, store.clone(), context, original, source)
            .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(
        *observed.status(),
        PendingInitialProducerStatus::ClosedAssignment
    );
    assert!(!observed.observation_failed(), "{:?}", observed.error());
    assert!(observed.error().is_none());
    owner.wait_for_supervision().await;
    store
}
#[tokio::test]
async fn completed_source_authenticates_native_main_but_not_resource_or_deletion() {
    for keyed in [false, true] {
        let store = completed(keyed).await;
        let state = store.state.lock().unwrap();
        let run = state.runs.values().next().unwrap();
        let mut proof = Proof::bounded(MAX_PROOF_BYTES);
        let actual = report(&state, run, &mut proof).unwrap();
        assert_eq!(
            actual.initial_consumption,
            InitialConsumption::CompletedSource
        );
        assert_eq!(
            actual.initial_source,
            crate::deletion::history::Fact::Authenticated
        );
        assert_eq!(
            actual.main_join,
            crate::deletion::history::Fact::Authenticated
        );
        assert_eq!(
            actual.resource_policy,
            crate::deletion::history::Fact::Unproven
        );
        assert!(!actual.complete());
        assert_original_resource_live(&state, run, &mut proof);
        let CoverageRead::Verified(coverage) =
            super::super::coverage(&state, &run.session_id).unwrap()
        else {
            panic!("actual managed creation coverage");
        };
        let mut measured = Proof::bounded(MAX_PROOF_BYTES);
        assert_eq!(
            super::super::readiness::read(&state, &coverage, &mut measured).unwrap(),
            Some(DeletionBlocker::SessionOwner)
        );
        let n = measured.used();
        assert!(n > 0);
        let mut exact = Proof::bounded(n);
        assert_eq!(
            super::super::readiness::read(&state, &coverage, &mut exact).unwrap(),
            Some(DeletionBlocker::SessionOwner)
        );
        assert_eq!(exact.used(), n);
        assert_eq!(
            super::super::readiness::read(&state, &coverage, &mut Proof::bounded(n - 1)).unwrap(),
            Some(DeletionBlocker::ProofLimit)
        );
    }
}
#[tokio::test]
async fn full_classifier_n_and_n_minus_one_share_the_actual_original_proof() {
    for keyed in [false, true] {
        let store = completed(keyed).await;
        let state = store.state.lock().unwrap();
        let run = state.runs.values().next().unwrap();
        let mut measured = Proof::bounded(MAX_PROOF_BYTES);
        assert_eq!(
            crate::memory::initial_consumer::classify_run(&state, run, &mut measured).unwrap(),
            InitialConsumption::CompletedSource
        );
        let n = measured.used();
        assert!(n > 0);
        let mut exact = Proof::bounded(n);
        assert_eq!(
            crate::memory::initial_consumer::classify_run(&state, run, &mut exact).unwrap(),
            InitialConsumption::CompletedSource
        );
        assert_eq!(exact.used(), n);
        assert!(matches!(
            crate::memory::initial_consumer::classify_run(&state, run, &mut Proof::bounded(n - 1)),
            Err(Failure::Limited(_))
        ));
    }
}
#[tokio::test]
async fn child_activation_loss_cannot_hide_behind_ready_closed_source_copies() {
    for keyed in [false, true] {
        let store = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let run = genuine.runs.values().next().unwrap();
        let witness = genuine.preparation_admissions.get(&run.id).unwrap();
        let ids = witness.audit_ids().unwrap();
        let child = genuine
            .session_owners
            .source_audit(&run.session_id, &ids[4])
            .unwrap();
        let activation = crate::session_owner::owned_scope::source::activation_key(
            &run.session_id,
            &child.record.snapshot.target,
        )
        .unwrap();
        let CoverageRead::Verified(coverage) =
            super::super::coverage(&genuine, &run.session_id).unwrap()
        else {
            panic!("actual managed creation coverage");
        };
        let mut changed = genuine.clone();
        changed
            .session_owners
            .erase_historical_audit_for_test(&run.session_id, &activation);
        let result = crate::memory::initial_consumer::classify_run(
            &changed,
            run,
            &mut Proof::bounded(MAX_PROOF_BYTES),
        );
        let readiness = super::super::readiness::read(
            &changed,
            &coverage,
            &mut Proof::bounded(MAX_PROOF_BYTES),
        );
        // Discard the corrupted snapshot before assertions/positive restoration.
        drop(changed);
        assert!(matches!(result, Err(Failure::Error(_))));
        assert_eq!(readiness, Err(crate::DeletionError::UnknownStoreOutcome));
        assert_eq!(
            crate::memory::initial_consumer::classify_run(
                &genuine,
                run,
                &mut Proof::bounded(MAX_PROOF_BYTES)
            )
            .unwrap(),
            InitialConsumption::CompletedSource
        );
    }
}

#[tokio::test]
async fn genuine_head_cannot_hide_one_missing_middle_or_tail_publication() {
    for keyed in [false, true] {
        let store = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let run = genuine.runs.values().next().unwrap();
        let expected = genuine
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap()
            .correlation();
        let view = crate::memory::initial_consumer::selected(
            &genuine,
            expected,
            &mut Proof::bounded(MAX_PROOF_BYTES),
        )
        .unwrap();
        assert!(view.publications.len() > 4);
        let middle = view.publications[view.publications.len() / 2].sequence;
        let tail = view.publications.last().unwrap().sequence;
        let head = view.head.unwrap();
        assert_eq!(head.sequence, tail);
        let CoverageRead::Verified(coverage) =
            super::super::coverage(&genuine, &run.session_id).unwrap()
        else {
            panic!("actual managed creation coverage");
        };
        for sequence in [middle, tail] {
            let mut changed = genuine.clone();
            let removed = changed
                .initial_consumer
                .test_erase_publication(&expected.key, sequence);
            let result = crate::memory::initial_consumer::classify_run(
                &changed,
                run,
                &mut Proof::bounded(MAX_PROOF_BYTES),
            );
            let readiness = super::super::readiness::read(
                &changed,
                &coverage,
                &mut Proof::bounded(MAX_PROOF_BYTES),
            );
            // Only the single selected publication was removed. Discard this
            // corrupted snapshot before assertions and restored positive reads.
            drop(changed);
            assert_eq!(removed.sequence, sequence);
            assert!(
                matches!(result, Err(Failure::Error(_))),
                "missing selected publication={sequence}, genuine head={tail}"
            );
            assert_eq!(readiness, Err(crate::DeletionError::UnknownStoreOutcome));
            assert_eq!(
                crate::memory::initial_consumer::classify_run(
                    &genuine,
                    run,
                    &mut Proof::bounded(MAX_PROOF_BYTES)
                )
                .unwrap(),
                InitialConsumption::CompletedSource
            );
        }
    }
}

/// Driver retirement leaves the independently registered preparation resource
/// child live; its genuine current/native origins remain the earlier blocker.
fn assert_original_resource_live(state: &State, run: &crabber_core::Run, proof: &mut Proof) {
    let witness = state.preparation_admissions.get(&run.id).unwrap();
    let ids = witness.audit_ids().unwrap();
    let child = state
        .session_owners
        .source_audit(&run.session_id, &ids[4])
        .unwrap();
    proof.measure(child).unwrap();
    assert!(child.record.resource.is_some());
    let mut safe = crate::public_session::budget::Budget::new(
        MAX_PROOF_BYTES,
        crate::PublicSessionLimit::HeaderBytes,
    );
    let live = state
        .session_owners
        .public_registered(&run.session_id, 1024, &mut safe, proof)
        .unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].target, child.record.snapshot.target);
    assert_eq!(
        live[0].state,
        crate::session_owner::SessionOwnerState::Active
    );
}

#[derive(Clone, Copy)]
enum IncompleteOriginal {
    Raw,
    BirthOnly,
    ClosedPrefix,
}
async fn incomplete_original(keyed: bool, kind: IncompleteOriginal) -> Arc<MemoryStore> {
    use crate::native_enrollment::initial_consumer::CallKind;
    use crate::session_owner::owned_scope::{ConsumerProofCaps, ConsumerProofUsage};
    let store = Arc::new(MemoryStore::with_clock(Arc::new(
        crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH),
    )));
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "p".into(),
            model_id: "m".into(),
        },
        vec!["actual incomplete original".into()],
    )
    .unwrap();
    let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
    if matches!(kind, IncompleteOriginal::Raw) {
        crate::Store::complete_preparation(&*store, original)
            .await
            .unwrap();
        return store;
    }
    let owner = PendingDriverTasks::default();
    let mut task = match kind {
        IncompleteOriginal::BirthOnly => {
            PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap()
        }
        IncompleteOriginal::ClosedPrefix => {
            // This is the real per-operation budget constructor, not a fake
            // receipt/refusal. Birth commits, actual Register cannot begin,
            // then the original joined task genuinely publishes paired Close.
            let usage: Arc<std::sync::Mutex<Vec<ConsumerProofUsage>>> = Arc::default();
            PendingInitialProducer::register_scripted_with_caps(
                &owner,
                store.clone(),
                context,
                original,
                source,
                ConsumerProofCaps(vec![(CallKind::Register, 0)]),
                usage,
            )
            .unwrap()
        }
        IncompleteOriginal::Raw => unreachable!(),
    };
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert!(!observed.observation_failed());
    assert_eq!(
        *observed.status(),
        PendingInitialProducerStatus::ClosedAssignment
    );
    assert_eq!(
        observed.error().is_some(),
        matches!(kind, IncompleteOriginal::ClosedPrefix)
    );
    owner.wait_for_supervision().await;
    store
}
#[tokio::test]
async fn actual_raw_birth_only_and_closed_prefix_reports_keep_native_main_unproven() {
    for keyed in [false, true] {
        for (kind, expected) in [
            (IncompleteOriginal::Raw, InitialConsumption::Unproven),
            (IncompleteOriginal::BirthOnly, InitialConsumption::Unproven),
            (
                IncompleteOriginal::ClosedPrefix,
                InitialConsumption::AuthenticatedClosedPrefix,
            ),
        ] {
            let store = incomplete_original(keyed, kind).await;
            let state = store.state.lock().unwrap();
            let run = state.runs.values().next().unwrap();
            let actual = report(&state, run, &mut Proof::bounded(MAX_PROOF_BYTES)).unwrap();
            assert_eq!(actual.initial_consumption, expected);
            assert_eq!(actual.main_join, crate::deletion::history::Fact::Unproven);
            assert_eq!(
                actual.resource_policy,
                crate::deletion::history::Fact::Unproven
            );
            assert!(!actual.complete());
        }
    }
}
