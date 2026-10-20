//! Real Register/Activate commit boundaries and independent native Join originals.
use super::fault_tests::business_fault;
use super::tests::{MAX, completed, source, store};
use super::{Proof, selected};
use crate::native_enrollment::initial_consumer::{CallKind, DriverRetirement};
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, initial_consumer_fixture,
};

#[tokio::test]
async fn actual_register_activate_faults_retire_only_the_original_driver() {
    for keyed in [false, true] {
        for (kind, boundary, expected) in [
            (CallKind::Register, 1, 0),
            (CallKind::Register, 2, 1),
            (CallKind::Activate, 1, 1),
            (CallKind::Activate, 2, 2),
        ] {
            let store = store();
            let source = source();
            let (context, original) =
                initial_consumer_fixture::prepared(&*store, keyed, &source).await;
            let owner = PendingDriverTasks::default();
            let mut task = PendingInitialProducer::register_scripted(
                &owner,
                business_fault(store.clone(), kind, boundary),
                context,
                original,
                source,
            )
            .unwrap();
            task.start().unwrap();
            let observed = task.join().await.unwrap();
            assert!(
                observed.error().is_some(),
                "actual commit boundary must remain visible"
            );
            assert!(!observed.observation_failed());
            owner.wait_for_supervision().await;
            let state = store.state.lock().unwrap();
            let origin = state.initial_consumer_cohorts.values().next().unwrap();
            let view = selected(&state, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
            let close = view.close.unwrap();
            let class = match &close.retirement {
                DriverRetirement::NoDriver => 0,
                DriverRetirement::Unactivated { .. } => 1,
                DriverRetirement::Activated { .. } => 2,
            };
            assert_eq!(class, expected);
            if let Some(audit) = close.retirement.audit() {
                assert_eq!(audit.record.token, "");
                assert_eq!(
                    audit.record.snapshot.state,
                    crate::session_owner::SessionOwnerState::Joined
                );
                assert_eq!(audit.record.owned_join.is_some(), expected == 2);
            }
            super::native::verify_members(&state, &view, &mut Proof::bounded(MAX)).unwrap();
            let birth = state
                .pending_initial
                .selected(
                    &origin.correlation().key,
                    &origin.correlation().identity,
                    &mut Proof::bounded(MAX),
                )
                .unwrap();
            assert!(birth.close.is_some());
            view.verify_birth(birth, &mut Proof::bounded(MAX)).unwrap();
            if expected == 0 {
                let mut missing = view;
                missing.assignment = None;
                assert!(
                    super::retirement::verify_absent(
                        &state,
                        origin.correlation(),
                        &missing,
                        &mut Proof::bounded(MAX)
                    )
                    .is_err(),
                    "current-epoch native origin cannot be hidden without any Driver"
                );
            }
        }
    }
}

#[tokio::test]
async fn saved_retirement_requires_actual_join_even_after_token_is_cleared() {
    for keyed in [false, true] {
        let (store, _) = completed(keyed).await;
        let genuine = store.state.lock().unwrap().clone();
        let expected = genuine
            .initial_consumer_cohorts
            .values()
            .next()
            .unwrap()
            .correlation();
        let view = selected(&genuine, expected, &mut Proof::bounded(MAX)).unwrap();
        let audit = view.close.unwrap().retirement.audit().unwrap();
        assert_eq!(audit.record.token, "");
        super::retirement::verify_retained(&genuine, expected, &view, &mut Proof::bounded(MAX))
            .unwrap();
        let mut erased = genuine.clone();
        erased.session_owners.erase_historical_audit_for_test(
            &audit.receipt.session_id,
            &audit.receipt.operation_id,
        );
        let intact = selected(&erased, expected, &mut Proof::bounded(MAX)).unwrap();
        assert!(
            super::retirement::verify_retained(
                &erased,
                expected,
                &intact,
                &mut Proof::bounded(MAX)
            )
            .is_err()
        );
        super::retirement::verify_retained(&genuine, expected, &view, &mut Proof::bounded(MAX))
            .unwrap();
    }
}

#[tokio::test]
async fn unrelated_driver_roster_still_requires_its_actual_first_issuer() {
    for keyed in [false, true] {
        let (store, session) = completed(keyed).await;
        let state = store.state.lock().unwrap();
        let drivers = state
            .session_owners
            .initial_retirement_drivers(&session, &mut Proof::bounded(MAX))
            .unwrap();
        assert_eq!(drivers.len(), 1);
        let original = drivers[0];
        crate::memory::session_owner::drivers::original_read(
            &state,
            original,
            &mut Proof::bounded(MAX),
        )
        .unwrap();
        let mut encoded = serde_json::to_value(original).unwrap();
        encoded["issuer_digest"] =
            serde_json::to_value(crabber_core::InputFingerprint::new("f".repeat(64)).unwrap())
                .unwrap();
        let changed = serde_json::from_value(encoded).unwrap();
        assert!(
            crate::memory::session_owner::drivers::original_read(
                &state,
                &changed,
                &mut Proof::bounded(MAX)
            )
            .is_err()
        );
        crate::memory::session_owner::drivers::original_read(
            &state,
            original,
            &mut Proof::bounded(MAX),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn older_joined_driver_does_not_block_new_genuinely_unopened_epoch() {
    for keyed in [false, true] {
        let (store, session) = completed(keyed).await;
        let source = source();
        let crate::session_owner::audit::RevisionState::Present(revision) =
            crate::memory::session_owner::revision(&store.state.lock().unwrap(), &session)
        else {
            panic!("actual existing Session revision");
        };
        let bound = initial_consumer_fixture::bound(
            &session,
            revision,
            &source,
            crate::AdmissionSelectionPolicy::Explicit,
        );
        let (context, original) = initial_consumer_fixture::observe(&*store, false, bound).await;
        let owner = PendingDriverTasks::default();
        let mut task = PendingInitialProducer::register_scripted(
            &owner,
            super::fault_tests::refuse_open(store.clone()),
            context,
            original,
            source,
        )
        .unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert!(!observed.observation_failed());
        assert!(matches!(observed.status(),crate::session_owner::owned_scope::PendingInitialProducerStatus::ClosedNoPublication),"new epoch: {:?}, {:?}",observed.status(),observed.error());
        owner.wait_for_supervision().await;
        let state = store.state.lock().unwrap();
        assert_eq!(
            state.runs.len(),
            1,
            "new unopened epoch must not invent a Run"
        );
        let unopened: Vec<_> = state
            .initial_consumer_cohorts
            .values()
            .filter(|origin| {
                matches!(
                    origin.as_ref(),
                    crate::native_enrollment::initial_consumer::Origin::NoOpenedMember(_)
                )
            })
            .collect();
        assert_eq!(unopened.len(), 1);
        let view = selected(&state, unopened[0].correlation(), &mut Proof::bounded(MAX)).unwrap();
        super::retirement::verify_retained(
            &state,
            unopened[0].correlation(),
            &view,
            &mut Proof::bounded(MAX),
        )
        .unwrap();
        assert!(matches!(
            view.retirement(),
            Some((DriverRetirement::NoDriver, _))
        ));
    }
}
