//! Fresh native atomicity and caller ACK allowance are distinct boundaries.
use super::{Proof, selected};
use crate::session_owner::owned_scope::{
    FixedInitialTestHooks, FixedMemoryInitialHost, FixedMemoryInitialInput,
    FixedMemoryInitialTaskControl, LocallyJoinedInitialSeal, NativeInitialOperation,
    PendingInitialProducerStatus,
};
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{
    AdmissionKey, ControlId, InputEnvelope, SessionContext, SessionId, SessionRevision,
};
use std::sync::{Arc, atomic::Ordering};
const MAX: usize = 32 << 20;
struct Fixture {
    host: FixedMemoryInitialHost,
    session: SessionId,
    hooks: Arc<FixedInitialTestHooks>,
    control: FixedMemoryInitialTaskControl,
    retry: LocallyJoinedInitialSeal,
}
async fn values(host: &FixedMemoryInitialHost, keyed: bool) -> FixedMemoryInitialInput {
    let session = SessionId::new();
    let revision = host
        .store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "fixed-budget".into(),
                directory: "/fixed-budget".into(),
                title: "same original".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap()
        .revision;
    FixedMemoryInitialInput {
        context: SessionContext::new(session, "fixed-budget", "/fixed-budget"),
        title: "same original".into(),
        input: InputEnvelope::text("actual fixed Seal budget", revision).unwrap(),
        key: keyed.then(|| AdmissionKey::new("same-fixed-original").unwrap()),
    }
}
impl Fixture {
    async fn new(keyed: bool, fault: u8) -> Self {
        let host = FixedMemoryInitialHost::new();
        let input = values(&host, keyed).await;
        let session = input.context.session_id().clone();
        let hooks = FixedInitialTestHooks::new();
        hooks.enable_fixed_seal_presentation();
        host.store()
            .pending_initial_test_state
            .fixed_policy_seal_fault
            .store(fault, Ordering::SeqCst);
        let mut task = host
            .register_scripted_test(input, super::tests::source(), hooks.clone())
            .unwrap();
        let control = task.control();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::Unresolved)
        );
        assert!(!observed.observation_failed());
        let retry = observed
            .take_retry()
            .expect("same actual joined original remains retained");
        drop(task);
        host.wait_for_supervision().await;
        host.store()
            .pending_initial_test_state
            .fixed_policy_seal_fault
            .store(0, Ordering::SeqCst);
        Self {
            host,
            session,
            hooks,
            control,
            retry,
        }
    }
    fn native_usage(&self) -> Vec<usize> {
        self.host
            .store()
            .pending_initial_test_state
            .precommit_usage
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(op, n)| (*op == NativeInitialOperation::Seal).then_some(*n))
            .collect()
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    child: (crate::session_owner::SessionOwnerState, bool, u64),
    driver: (crate::session_owner::SessionOwnerState, bool, u64),
    closes: (bool, bool),
    publications: (u64, usize, usize),
    revision: crate::session_owner::audit::RevisionState,
}
fn shape(fixture: &Fixture) -> Shape {
    let state = fixture.host.store().state.lock().unwrap();
    let origin = state
        .initial_consumer_cohorts
        .values()
        .find(|origin| origin.correlation().key.session_id == fixture.session)
        .unwrap();
    let expected = origin.correlation();
    let view = selected(&state, expected, &mut Proof::bounded(MAX)).unwrap();
    let birth = state
        .pending_initial
        .selected(&expected.key, &expected.identity, &mut Proof::bounded(MAX))
        .unwrap();
    let child = state
        .session_owners
        .initial_retirement_current(
            &fixture.session,
            &origin
                .original_policy()
                .unwrap()
                .preparation_activation
                .target()
                .id,
            &mut Proof::bounded(MAX),
        )
        .unwrap()
        .unwrap();
    let driver = state
        .session_owners
        .initial_retirement_current(
            &fixture.session,
            &view
                .registered_driver()
                .unwrap()
                .begin
                .record
                .snapshot
                .target
                .id,
            &mut Proof::bounded(MAX),
        )
        .unwrap()
        .unwrap();
    Shape {
        child: (child.snapshot.state, child.token.is_empty(), child.sequence),
        driver: (
            driver.snapshot.state,
            driver.token.is_empty(),
            driver.sequence,
        ),
        closes: (view.close.is_some(), birth.close.is_some()),
        publications: (
            state.session_owners.publication_count(&fixture.session),
            view.publications.len(),
            birth.publications.len(),
        ),
        revision: crate::memory::session_owner::revision(&state, &fixture.session),
    }
}
fn assert_atomic_closed(before: &Shape, after: &Shape) {
    use crate::session_owner::SessionOwnerState;
    assert_eq!(before.closes, (false, false));
    assert_eq!(after.closes, (true, true));
    assert_eq!(
        after.child,
        (SessionOwnerState::Joined, true, before.child.2 + 1)
    );
    assert_eq!(
        after.driver,
        (SessionOwnerState::Joined, true, before.driver.2 + 1)
    );
    assert_eq!(
        after.publications,
        (
            before.publications.0 + 2,
            before.publications.1 + 1,
            before.publications.2 + 1
        )
    );
    let crate::session_owner::audit::RevisionState::Present(revision) = before.revision else {
        panic!("actual managed Session");
    };
    assert_eq!(
        after.revision,
        crate::session_owner::audit::RevisionState::Present(
            revision.next().unwrap().next().unwrap()
        )
    );
}
#[tokio::test]
async fn fixed_native_seal_exact_n_minus_one_rolls_back_all_four_writes() {
    for keyed in [false, true] {
        let mut fixture = Fixture::new(keyed, 1).await;
        let before = shape(&fixture);
        let n = fixture.native_usage()[0];
        fixture.hooks.set_seal_cap(n - 1);
        assert!(fixture.retry.retry().await.is_err());
        assert_eq!(shape(&fixture), before);
        assert_eq!(fixture.native_usage(), vec![n]);
        fixture.hooks.set_seal_cap(n);
        assert!(
            fixture.retry.retry().await.is_err(),
            "postcommit ACK still needs its actual allowance"
        );
        let committed = shape(&fixture);
        assert_atomic_closed(&before, &committed);
        assert_eq!(fixture.native_usage(), vec![n, n]);
        fixture.hooks.set_seal_cap(MAX);
        assert_eq!(
            fixture.retry.retry().await.unwrap(),
            PendingInitialProducerStatus::ClosedAssignment
        );
        assert_eq!(
            shape(&fixture),
            committed,
            "same saved Joins replay without new publications"
        );
    }
}
#[tokio::test]
async fn fixed_seal_lost_ack_replays_the_same_two_native_joins() {
    for keyed in [false, true] {
        let mut fixture = Fixture::new(keyed, 2).await;
        let committed = shape(&fixture);
        assert_eq!(committed.closes, (true, true));
        assert_eq!(
            fixture.retry.retry().await.unwrap(),
            PendingInitialProducerStatus::ClosedAssignment
        );
        assert_eq!(shape(&fixture), committed);
        let report = fixture
            .host
            .store()
            .fixed_initial_fixture_report(&fixture.session)
            .unwrap();
        assert_eq!(
            report.resource_policy,
            crate::deletion::history::Fact::Authenticated
        );
    }
}

#[tokio::test]
async fn counterfactual_fresh_seal_whole_ack_exact_n_keeps_same_actual_loan() {
    for keyed in [false, true] {
        let mut fixture = Fixture::new(keyed, 1).await;
        // The actual precommit fault published nothing. This exact native
        // prestate isolates FRESH-branch budget cost; the same joined/drained
        // loan, private grants, output owner and first real SystemClock
        // presentation survive. This is not a crash/real rollback claim.
        let frozen = fixture.host.store().state.lock().unwrap().clone();
        let before = shape(&fixture);
        fixture.hooks.refuse_next_seal_ack();
        fixture.hooks.set_seal_cap(MAX);
        assert!(fixture.retry.retry().await.is_err());
        let calibrated = *fixture.hooks.usage().lock().unwrap().seal.last().unwrap();
        assert!(
            !calibrated.acknowledged,
            "only the real validated ACK was refused"
        );
        let n = calibrated.used;
        assert!(n > fixture.native_usage()[0]);
        assert_eq!(
            fixture.control.status(),
            Some(PendingInitialProducerStatus::Unresolved)
        );
        assert_atomic_closed(&before, &shape(&fixture));

        *fixture.host.store().state.lock().unwrap() = frozen.clone();
        fixture.hooks.set_seal_cap(n - 1);
        assert!(fixture.retry.retry().await.is_err());
        assert_eq!(
            fixture.control.status(),
            Some(PendingInitialProducerStatus::Unresolved)
        );
        assert_atomic_closed(&before, &shape(&fixture));
        assert!(
            !fixture
                .hooks
                .usage()
                .lock()
                .unwrap()
                .seal
                .last()
                .unwrap()
                .acknowledged
        );

        *fixture.host.store().state.lock().unwrap() = frozen;
        fixture.hooks.set_seal_cap(n);
        assert_eq!(
            fixture.retry.retry().await.unwrap(),
            PendingInitialProducerStatus::ClosedAssignment
        );
        let actual = *fixture.hooks.usage().lock().unwrap().seal.last().unwrap();
        assert!(actual.acknowledged);
        assert_eq!(actual.used, n, "N is actual fresh whole-operation usage");
        assert_atomic_closed(&before, &shape(&fixture));
    }
}

#[tokio::test]
async fn fixed_v3_failed_register_keeps_main_and_resources_unproven() {
    for keyed in [false, true] {
        let host = FixedMemoryInitialHost::new();
        let input = values(&host, keyed).await;
        let session = input.context.session_id().clone();
        let hooks = FixedInitialTestHooks::new();
        hooks.set_consumer_cap(
            crate::native_enrollment::initial_consumer::CallKind::Register,
            0,
        );
        let mut task = host
            .register_scripted_test(input, super::tests::source(), hooks)
            .unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert_eq!(
            observed.status(),
            Some(&PendingInitialProducerStatus::ClosedAssignment)
        );
        assert!(
            observed.error().is_some(),
            "the actual Register Proof refused before business"
        );
        assert!(!observed.observation_failed());
        drop(task);
        host.wait_for_supervision().await;
        let report = host.store().fixed_initial_fixture_report(&session).unwrap();
        assert_eq!(report.initial_consumption, crate::native_enrollment::initial_consumer::InitialConsumption::AuthenticatedClosedPrefix);
        assert_eq!(report.main_join, crate::deletion::history::Fact::Unproven);
        assert_eq!(
            report.resource_policy,
            crate::deletion::history::Fact::Unproven
        );
        assert!(!report.complete());
    }
}
