//! Real task Proof boundaries; native rollback and returned ACK are distinct.
use super::*;
use crate::native_enrollment::pending_initial::EpochKind;
use crate::session_owner::owned_scope::{
    InitialOperationUsage, InitialProofCaps, InitialProofUsage, PendingDriverTasks,
    PendingInitialProducer, PendingInitialProducerStatus as Status,
};
use crabber_core::{ManualClock, SessionContext};
use std::sync::{Arc, Mutex};
const MAX: usize = 32 << 20;
struct Fixture {
    state: State,
    context: SessionContext,
    original: JoinedPreparation,
}
impl Fixture {
    async fn new(keyed: bool) -> Self {
        let store =
            MemoryStore::with_clock(Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH)));
        let (context, original) = super::tests::original(&store, keyed).await;
        let state = store.state.lock().unwrap().clone();
        Self {
            state,
            context,
            original,
        }
    }
    async fn run(&self, caps: InitialProofCaps) -> Observed {
        let store = Arc::new(MemoryStore::with_clock(Arc::new(ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ))));
        *store.state.lock().unwrap() = self.state.clone();
        store
            .state
            .lock()
            .unwrap()
            .pending_initial_carrier_observations = Arc::default();
        let usage = Arc::new(Mutex::new(InitialProofUsage::default()));
        let owner = PendingDriverTasks::default();
        let mut task = PendingInitialProducer::register_with_caps(
            &owner,
            store.clone(),
            self.context.clone(),
            self.original.clone(),
            caps,
            usage.clone(),
        )
        .unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        let status = observed.status().clone();
        eprintln!(
            "actual bounded Pending: {status:?}, error={:?}",
            observed.error()
        );
        owner.wait_for_supervision().await;
        Observed {
            store,
            usage,
            status,
        }
    }
}
struct Observed {
    store: Arc<MemoryStore>,
    usage: Arc<Mutex<InitialProofUsage>>,
    status: Status,
}
#[derive(Debug, PartialEq, Eq)]
struct Shape {
    runs: usize,
    opened: bool,
    assigned: bool,
    closed: bool,
}
impl Observed {
    fn usage(&self, operation: NativeInitialOperation) -> InitialOperationUsage {
        let usage = self.usage.lock().unwrap();
        match operation {
            NativeInitialOperation::Open => usage.open.unwrap(),
            NativeInitialOperation::Complete => usage.complete.unwrap(),
            NativeInitialOperation::Seal => *usage.seal.last().unwrap(),
        }
    }
    fn precommit(&self, operation: NativeInitialOperation) -> Option<usize> {
        self.store
            .pending_initial_test_state.precommit_usage
            .lock()
            .unwrap()
            .iter()
            .find(|(op, _)| *op == operation)
            .map(|(_, used)| *used)
    }
    fn shape(&self) -> Shape {
        let state = self.store.state.lock().unwrap();
        let original = state.pending_initial_protocols.values().next().unwrap();
        let epoch = state.pending_initial.test_epoch(&original.key).unwrap();
        let mut proof = Proof::bounded(MAX);
        let view = state
            .pending_initial
            .selected(&original.key, &epoch.correlation.identity, &mut proof)
            .unwrap();
        assert!(routes(&state, &original.key.session_id, &mut proof).unwrap());
        for run in state.runs.values() {
            assert_eq!(
                crate::memory::native_enrollment::original(
                    &state,
                    &run.session_id,
                    &run.id,
                    &mut proof
                )
                .unwrap(),
                crate::native_enrollment::Status::Pending
            );
        }
        assert!(state.execution_enrollment_unclassified.is_empty());
        Shape {
            runs: state.runs.len(),
            opened: epoch.kind == EpochKind::Opened,
            assigned: view.assignment.is_some(),
            closed: view.close.is_some(),
        }
    }
}
fn capped(operation: NativeInitialOperation, cap: usize) -> InitialProofCaps {
    let mut caps = InitialProofCaps::default();
    match operation {
        NativeInitialOperation::Open => caps.open = cap,
        NativeInitialOperation::Complete => caps.complete = cap,
        NativeInitialOperation::Seal => caps.seal = cap,
    }
    caps
}
const OPERATIONS: [NativeInitialOperation; 3] = [
    NativeInitialOperation::Open,
    NativeInitialOperation::Complete,
    NativeInitialOperation::Seal,
];
#[tokio::test]
async fn actual_native_precommit_exact_budget_and_one_less_roll_back() {
    for keyed in [false, true] {
        let fixture = Fixture::new(keyed).await;
        let baseline = fixture.run(InitialProofCaps::default()).await;
        assert_eq!(baseline.status, Status::ClosedAssignment);
        for operation in OPERATIONS {
            let n = baseline.precommit(operation).unwrap();
            let exact = fixture.run(capped(operation, n)).await;
            assert_eq!(exact.precommit(operation), Some(n));
            let short = fixture.run(capped(operation, n - 1)).await;
            assert_eq!(short.precommit(operation), None);
            assert!(!short.usage(operation).acknowledged);
            // A later genuine Seal may create a sealed-before-Open record or
            // close an earlier Open. It cannot make the failed mutation real.
            let (committed, rolled_back) = match operation {
                NativeInitialOperation::Open => (
                    Shape {
                        runs: 0,
                        opened: true,
                        assigned: false,
                        closed: true,
                    },
                    Shape {
                        runs: 0,
                        opened: false,
                        assigned: false,
                        closed: true,
                    },
                ),
                NativeInitialOperation::Complete => (
                    Shape {
                        runs: 1,
                        opened: true,
                        assigned: true,
                        closed: true,
                    },
                    Shape {
                        runs: 0,
                        opened: true,
                        assigned: false,
                        closed: true,
                    },
                ),
                NativeInitialOperation::Seal => (
                    Shape {
                        runs: 1,
                        opened: true,
                        assigned: true,
                        closed: true,
                    },
                    Shape {
                        runs: 1,
                        opened: true,
                        assigned: true,
                        closed: false,
                    },
                ),
            };
            assert_eq!(exact.shape(), committed, "{operation:?}, keyed={keyed}");
            assert_eq!(short.shape(), rolled_back, "{operation:?}, keyed={keyed}");
        }
    }
}
#[tokio::test]
async fn actual_full_operation_budget_includes_post_drop_ack_and_status_tail() {
    for keyed in [false, true] {
        let fixture = Fixture::new(keyed).await;
        let baseline = fixture.run(InitialProofCaps::default()).await;
        assert_eq!(baseline.status, Status::ClosedAssignment);
        for operation in OPERATIONS {
            let n = baseline.usage(operation).used;
            assert!(baseline.usage(operation).acknowledged);
            let exact = fixture.run(capped(operation, n)).await;
            assert_eq!(
                exact.usage(operation),
                InitialOperationUsage {
                    used: n,
                    acknowledged: true
                }
            );
            assert_eq!(exact.status, Status::ClosedAssignment);
            let short = fixture.run(capped(operation, n - 1)).await;
            assert!(!short.usage(operation).acknowledged);
            assert!(short.usage(operation).used > n - 1);
            // N-1 here exhausts after the real native commit, during actual
            // returned ACK/status handling. Refusal is not claimed as rollback.
            assert!(short.precommit(operation).is_some());
            let shape = short.shape();
            assert!(shape.closed);
            if operation == NativeInitialOperation::Open {
                assert_eq!(shape.runs, 0);
                assert!(!shape.assigned);
            } else {
                assert_eq!(shape.runs, 1);
                assert!(shape.assigned);
            }
        }
    }
}

async fn large_fixture() -> Fixture {
    use crate::session_owner::{
        preparation::{
            OWNED_PREPARATION_COMPONENT, PreparationKey, PreparationPurpose, PreparedOperation,
            ReservePreparation,
        },
        preparation_contract::{self, resources},
    };
    let store =
        MemoryStore::with_clock(Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH)));
    let session = SessionId::new();
    let mut bound = preparation_contract::fixture(&session, crabber_core::SessionRevision::INITIAL);
    let prompt = "g".repeat(512 << 10);
    bound.binding.request.system_prompt = Some(prompt.clone());
    bound
        .keyed
        .execution
        .as_mut()
        .unwrap()
        .request
        .system_prompt = Some(prompt.clone());
    bound.binding.runtime_semantics[3] = serde_json::Value::String(prompt);
    let contract = crabber_core::InputFingerprint::new("d".repeat(64)).unwrap();
    resources::declaration::bind(&mut bound, &contract);
    let mut owner = crate::session_owner_contract::reservation(session.clone());
    owner
        .binding
        .workspace_id
        .clone_from(&bound.binding.request.workspace_id);
    owner
        .binding
        .directory
        .clone_from(&bound.binding.request.directory);
    owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    owner.lease_ms = 30_000;
    let request = ReservePreparation {
        key: PreparationKey::Unkeyed(crabber_core::ControlId::new()),
        source: None,
        resource_contract: Some(contract),
        owner,
        expected_revision: crabber_core::SessionRevision::INITIAL,
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: crate::session_owner::preparation::admission_intent(
            &bound.binding.request,
            bound.binding.input_envelope.as_ref().unwrap(),
            Some(&bound.keyed.options),
        )
        .unwrap(),
    }
    .bind_key(PreparationKey::Admission(bound.keyed.options.key.clone()))
    .unwrap();
    let root = preparation_contract::begin(&store, &request).await;
    let original = resources::initial_driver_observation(
        &store,
        root,
        PreparedOperation::AdmitKeyed(Box::new(bound)),
    )
    .await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        session,
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let state = store.state.lock().unwrap().clone();
    Fixture {
        state,
        context,
        original,
    }
}
#[tokio::test]
async fn new_large_execution_carrier_is_charged_before_actual_clone() {
    let fixture = large_fixture().await;
    let baseline = fixture.run(InitialProofCaps::default()).await;
    assert_eq!(baseline.status, Status::ClosedAssignment);
    let n = baseline
        .store
        .state
        .lock()
        .unwrap()
        .pending_initial_carrier_observations
        .lock()
        .unwrap()[0]
        .1;
    let exact = fixture
        .run(capped(NativeInitialOperation::Complete, n))
        .await;
    let short = fixture
        .run(capped(NativeInitialOperation::Complete, n - 1))
        .await;
    assert_eq!(
        exact
            .store
            .state
            .lock()
            .unwrap()
            .pending_initial_carrier_observations
            .lock()
            .unwrap()
            .as_slice(),
        &[(fixture.context.session_id().clone(), n)]
    );
    assert!(
        short
            .store
            .state
            .lock()
            .unwrap()
            .pending_initial_carrier_observations
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(!short.usage(NativeInitialOperation::Complete).acknowledged);
    // Exact record budget reaches its actual clone but cannot pay for a later
    // independent Init source. Neither trial can expose a partial native birth.
    let rolled_back = Shape {
        runs: 0,
        opened: true,
        assigned: false,
        closed: true,
    };
    assert_eq!(exact.shape(), rolled_back);
    assert_eq!(short.shape(), rolled_back);
}
