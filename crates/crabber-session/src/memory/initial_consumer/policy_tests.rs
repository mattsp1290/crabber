//! Actual fixed originals and independent native retirement/history negatives.
use super::{Proof, selected};
use crate::deletion::history::Fact;
use crate::native_enrollment::initial_consumer::{InitialConsumption, Origin};
use crate::session_owner::owned_scope::{
    FixedMemoryInitialHost, FixedMemoryInitialInput, PendingInitialProducerStatus,
};
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{
    AdmissionKey, ControlId, InputEnvelope, SessionContext, SessionId, SessionRevision,
};
const MAX: usize = 32 << 20;

async fn completed(keyed: bool) -> (FixedMemoryInitialHost, SessionId) {
    let host = FixedMemoryInitialHost::new();
    let session = SessionId::new();
    let receipt = host
        .store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "fixed".into(),
                directory: "/fixed".into(),
                title: "policy originals".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let source = super::tests::source();
    let mut task = host
        .register_scripted(
            FixedMemoryInitialInput {
                context: SessionContext::new(session.clone(), "fixed", "/fixed"),
                title: "fixed original".into(),
                input: InputEnvelope::text("genuine fixed literal", receipt.revision).unwrap(),
                key: keyed.then(|| AdmissionKey::new("fixed-policy").unwrap()),
            },
            source,
        )
        .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(
        observed.status(),
        Some(&PendingInitialProducerStatus::ClosedAssignment)
    );
    assert!(observed.error().is_none(), "{:?}", observed.error());
    assert!(!observed.observation_failed());
    drop(task);
    host.wait_for_supervision().await;
    (host, session)
}
#[tokio::test]
async fn fixed_policy_requires_actual_child_and_driver_native_originals() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let store = host.store();
        let genuine = store.state.lock().unwrap().clone();
        let origin = genuine.initial_consumer_cohorts.values().next().unwrap();
        let view = selected(&genuine, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
        let original = origin.original_policy().unwrap();
        let close = view.close.unwrap();
        let child = &close.policy_terminal.as_ref().unwrap().preparation_join;
        assert_eq!(child.record.token, "");
        assert_eq!(close.version, 3);
        assert_eq!(origin.original_policy().unwrap().version, 1);
        let ids = [
            original.preparation_activation.source.reserve.clone(),
            original.preparation_activation.source.begin.clone(),
            original.preparation_activation.id.clone(),
            child.receipt.operation_id.clone(),
            close
                .retirement
                .audit()
                .unwrap()
                .receipt
                .operation_id
                .clone(),
        ];
        for id in ids {
            let mut changed = genuine.clone();
            changed
                .session_owners
                .erase_historical_audit_for_test(&session, &id);
            *store.state.lock().unwrap() = changed;
            let refused = store.fixed_initial_fixture_report(&session);
            *store.state.lock().unwrap() = genuine.clone();
            assert!(
                refused.is_err(),
                "actual original erasure must not downgrade policy"
            );
            let restored = store.fixed_initial_fixture_report(&session).unwrap();
            assert_eq!(restored.resource_policy, Fact::Authenticated);
        }
        let mut changed = genuine.clone();
        changed
            .session_owners
            .corrupt_retirement_current_token_for_test(&session, &child.record.snapshot.target.id);
        *store.state.lock().unwrap() = changed;
        let refused = store.fixed_initial_fixture_report(&session);
        *store.state.lock().unwrap() = genuine.clone();
        assert!(
            refused.is_err(),
            "saved child Join cannot hide a non-cleared current token"
        );
        let mut changed = genuine.clone();
        changed.initial_consumer_cohorts.clear();
        *store.state.lock().unwrap() = changed;
        let refused = store.fixed_initial_fixture_report(&session);
        *store.state.lock().unwrap() = genuine;
        assert!(refused.is_err());
    }
}
#[tokio::test]
async fn fixed_policy_full_read_uses_one_actual_exact_allowance() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let state = host.store().state.lock().unwrap();
        let run = state
            .runs
            .values()
            .find(|run| run.session_id == session)
            .unwrap();
        let mut baseline = Proof::bounded(MAX);
        assert!(
            crate::memory::MemoryStore::classify_initial_resource_policy(
                &state,
                run,
                &mut baseline
            )
            .unwrap()
        );
        let n = baseline.used();
        assert!(
            crate::memory::MemoryStore::classify_initial_resource_policy(
                &state,
                run,
                &mut Proof::bounded(n)
            )
            .unwrap()
        );
        let refused = crate::memory::MemoryStore::classify_initial_resource_policy(
            &state,
            run,
            &mut Proof::bounded(n - 1),
        );
        assert!(matches!(
            refused,
            Err(crate::public_session::v2::proof::Failure::Limited(_))
        ));
        drop(state);
        let mut proof = Proof::bounded(MAX);
        let actual = host
            .store()
            .fixed_initial_fixture_report_with_proof(&session, &mut proof)
            .unwrap();
        assert_eq!(actual.resource_policy, Fact::Authenticated);
        let whole = proof.used();
        assert!(
            host.store()
                .fixed_initial_fixture_report_with_proof(&session, &mut Proof::bounded(whole))
                .is_ok()
        );
        assert!(matches!(
            host.store()
                .fixed_initial_fixture_report_with_proof(&session, &mut Proof::bounded(whole - 1)),
            Err(crate::public_session::v2::proof::Failure::Limited(_))
        ));
    }
}
#[tokio::test]
async fn generic_originals_keep_exact_v1_v2_wire_and_unproven_policy() {
    #[derive(serde::Serialize)]
    struct LegacyAnchor<'a> {
        version: u32,
        correlation: &'a crate::native_enrollment::pending_initial::Correlation,
        birth_open_digest: &'a crabber_core::InputFingerprint,
    }
    #[derive(serde::Serialize)]
    struct LegacyClose<'a> {
        version: u32,
        retirement: &'a crate::native_enrollment::initial_consumer::DriverRetirement,
        disposition: crate::session_owner::owned_scope::OwnedMainDisposition,
        correlation: &'a crate::native_enrollment::pending_initial::Correlation,
        anchor_digest: &'a crabber_core::InputFingerprint,
        birth_close_digest: &'a crabber_core::InputFingerprint,
        prior_head_digest: &'a crabber_core::InputFingerprint,
        roster_digest: &'a crabber_core::InputFingerprint,
    }
    for keyed in [false, true] {
        let (store, _) = super::tests::completed(keyed).await;
        let state = store.state.lock().unwrap();
        let origin = state.initial_consumer_cohorts.values().next().unwrap();
        let Origin::Opened(anchor) = origin.as_ref() else {
            panic!("genuine generic Open");
        };
        let view = selected(&state, origin.correlation(), &mut Proof::bounded(MAX)).unwrap();
        let close = view.close.unwrap();
        assert_eq!(
            serde_json::to_vec(anchor).unwrap(),
            serde_json::to_vec(&LegacyAnchor {
                version: 1,
                correlation: &anchor.correlation,
                birth_open_digest: &anchor.birth_open_digest
            })
            .unwrap()
        );
        assert_eq!(
            serde_json::to_vec(close).unwrap(),
            serde_json::to_vec(&LegacyClose {
                version: 2,
                retirement: &close.retirement,
                disposition: close.disposition,
                correlation: &close.correlation,
                anchor_digest: &close.anchor_digest,
                birth_close_digest: &close.birth_close_digest,
                prior_head_digest: &close.prior_head_digest,
                roster_digest: &close.roster_digest
            })
            .unwrap()
        );
        let run = state.runs.values().next().unwrap();
        assert_eq!(
            super::classify_run(&state, run, &mut Proof::bounded(MAX)).unwrap(),
            InitialConsumption::CompletedSource
        );
        assert!(
            !crate::memory::MemoryStore::classify_initial_resource_policy(
                &state,
                run,
                &mut Proof::bounded(MAX)
            )
            .unwrap()
        );
    }
}
#[tokio::test]
async fn real_fixed_terminal_cannot_be_swapped_between_original_sources() {
    let (first, first_session) = completed(false).await;
    let (second, second_session) = completed(true).await;
    let a = first.store().state.lock().unwrap();
    let b = second.store().state.lock().unwrap();
    let original = a.initial_consumer_cohorts.values().next().unwrap();
    let foreign = b.initial_consumer_cohorts.values().next().unwrap();
    let foreign_view = selected(&b, foreign.correlation(), &mut Proof::bounded(MAX)).unwrap();
    let terminal = foreign_view
        .close
        .unwrap()
        .policy_terminal
        .as_ref()
        .unwrap();
    assert_ne!(first_session, second_session);
    assert!(
        terminal
            .verify(
                original.original_policy().unwrap(),
                &mut Proof::bounded(MAX)
            )
            .is_err()
    );
    let mut malformed = original.as_ref().clone();
    let Origin::Opened(anchor) = &mut malformed else {
        panic!("actual fixed Open");
    };
    anchor.version = 1;
    assert!(malformed.validate(&mut Proof::bounded(MAX)).is_err());
    let mut wrong_close = foreign_view.close.unwrap().clone();
    wrong_close.version = 2;
    assert!(!wrong_close.valid_policy_version(foreign));
}
