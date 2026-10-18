//! Native retirement rollback and post-commit Seal ACK have different boundaries.
use super::tests::{MAX, source, store};
use super::{Proof, State, selected};
use crate::{
    MemoryStore,
    session_owner::{
        SessionOwnerState,
        owned_scope::{
            InitialProofCaps, InitialProofUsage, NativeInitialOperation, PendingDriverTasks,
            PendingInitialProducer, initial_consumer_fixture,
        },
        preparation::JoinedPreparation,
    },
};
use crabber_core::SessionContext;
use std::sync::{Arc, Mutex};
struct Fixture {
    state: State,
    context: SessionContext,
    original: JoinedPreparation,
}
struct Observed {
    native: Option<usize>,
    whole: usize,
    acknowledged: bool,
    shape: Shape,
}
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    consumer_close: bool,
    birth_close: bool,
    driver: DriverShape,
}
#[derive(Debug, PartialEq, Eq)]
struct DriverShape {
    state: SessionOwnerState,
    token_cleared: bool,
}
impl Fixture {
    async fn new(keyed: bool) -> Self {
        let store = store();
        let source = source();
        let (context, original) = initial_consumer_fixture::prepared(&*store, keyed, &source).await;
        let state = store.state.lock().unwrap().clone();
        Self {
            state,
            context,
            original,
        }
    }
    async fn run(&self, seal: usize) -> Observed {
        let store = store();
        *store.state.lock().unwrap() = self.state.clone();
        let owner = PendingDriverTasks::default();
        let usage = Arc::new(Mutex::new(InitialProofUsage::default()));
        let caps = InitialProofCaps {
            seal,
            ..InitialProofCaps::default()
        };
        let mut task = PendingInitialProducer::register_scripted_with_initial_caps(
            &owner,
            store.clone(),
            self.context.clone(),
            self.original.clone(),
            source(),
            (caps, usage.clone()),
        )
        .unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert!(
            !observed.observation_failed(),
            "literal observation: {:?}",
            observed.error()
        );
        owner.wait_for_supervision().await;
        let native = store
            .pending_initial_test_state
            .precommit_usage
            .lock()
            .unwrap()
            .iter()
            .find_map(|(operation, used)| {
                (*operation == NativeInitialOperation::Seal).then_some(*used)
            });
        let actual = *usage.lock().unwrap().seal.last().unwrap();
        Observed {
            native,
            whole: actual.used,
            acknowledged: actual.acknowledged,
            shape: shape(&store),
        }
    }
}
fn shape(store: &MemoryStore) -> Shape {
    let state = store.state.lock().unwrap();
    let expected = state
        .initial_consumer_cohorts
        .values()
        .next()
        .unwrap()
        .correlation();
    let view = selected(&state, expected, &mut Proof::bounded(MAX)).unwrap();
    let birth = state
        .pending_initial
        .selected(&expected.key, &expected.identity, &mut Proof::bounded(MAX))
        .unwrap();
    let register = view.registered_driver().unwrap();
    let current = state
        .session_owners
        .initial_retirement_current(
            &expected.key.session_id,
            &register.begin.record.snapshot.target.id,
            &mut Proof::bounded(MAX),
        )
        .unwrap()
        .unwrap();
    Shape {
        consumer_close: view.close.is_some(),
        birth_close: birth.close.is_some(),
        driver: DriverShape {
            state: current.snapshot.state,
            token_cleared: current.token.is_empty(),
        },
    }
}
fn closed() -> Shape {
    Shape {
        consumer_close: true,
        birth_close: true,
        driver: DriverShape {
            state: SessionOwnerState::Joined,
            token_cleared: true,
        },
    }
}
fn open() -> Shape {
    Shape {
        consumer_close: false,
        birth_close: false,
        driver: DriverShape {
            state: SessionOwnerState::Active,
            token_cleared: false,
        },
    }
}
#[tokio::test]
async fn seal_native_n_commits_all_three_and_n_minus_one_rolls_back_all_three() {
    for keyed in [false, true] {
        let fixture = Fixture::new(keyed).await;
        let baseline = fixture.run(MAX).await;
        assert!(baseline.acknowledged);
        assert_eq!(baseline.shape, closed());
        let n = baseline.native.unwrap();
        assert!(n > 0 && n < baseline.whole);
        let exact = fixture.run(n).await;
        assert_eq!(exact.native, Some(n));
        assert_eq!(exact.shape, closed());
        assert!(!exact.acknowledged);
        let short = fixture.run(n - 1).await;
        assert_eq!(short.native, None);
        assert_eq!(short.shape, open());
        assert!(!short.acknowledged);
    }
}
#[tokio::test]
async fn seal_ack_n_and_n_minus_one_preserve_same_atomic_native_retirement() {
    for keyed in [false, true] {
        let fixture = Fixture::new(keyed).await;
        let baseline = fixture.run(MAX).await;
        let n = baseline.whole;
        assert!(baseline.acknowledged && n > baseline.native.unwrap());
        let exact = fixture.run(n).await;
        assert_eq!(exact.whole, n);
        assert!(exact.acknowledged);
        assert_eq!(exact.shape, closed());
        let short = fixture.run(n - 1).await;
        assert!(!short.acknowledged);
        assert!(short.native.is_some());
        assert_eq!(short.shape, closed());
    }
}
