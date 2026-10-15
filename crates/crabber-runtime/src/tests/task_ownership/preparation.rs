use super::*;
use crabber_core::{AdmissionKey, AdmissionOptions, InputFingerprint};

pub(crate) struct AdmissionResponseGate {
    entered: Mutex<Option<oneshot::Sender<(SessionId, RunId)>>>,
    release: Notify,
}
impl AdmissionResponseGate {
    pub(crate) async fn hold(&self, session: &SessionId, run: &RunId) {
        let sender = self.entered.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send((session.clone(), run.clone()));
            self.release.notified().await;
        }
    }
}
struct Harness {
    runtime: Orchestrator,
    memory: Arc<MemoryStore>,
    clock: Arc<ManualClock>,
    fake: FakeProvider,
    gate: Arc<AdmissionResponseGate>,
}
impl Harness {
    fn new() -> (Self, oneshot::Receiver<(SessionId, RunId)>) {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let memory = Arc::new(MemoryStore::with_clock(clock.clone()));
        let fake = FakeProvider::scripted(vec![text_script("done")]);
        let (entered, receiver) = oneshot::channel();
        let gate = Arc::new(AdmissionResponseGate {
            entered: Mutex::new(Some(entered)),
            release: Notify::new(),
        });
        let store = Arc::new(DelayedTerminalStore {
            inner: memory.clone(),
            gate: Arc::new(SettlementGate {
                admission: Some(gate.clone()),
                ..SettlementGate::default()
            }),
        });
        // This fixture gates admission responses, so leave terminal settlement open.
        store.gate.release.notify_one();
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store)
            .clock(clock.clone())
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
            .build()
            .unwrap();
        (
            Self {
                runtime,
                memory,
                clock,
                fake,
                gate,
            },
            receiver,
        )
    }
    async fn abandon_then_finish(
        &self,
        waiter: tokio::task::JoinHandle<()>,
        session: &SessionId,
        run: &RunId,
    ) {
        assert_eq!(
            self.memory.get_run(run).await.unwrap().unwrap().status,
            RunStatus::Running
        );
        assert_eq!(self.fake.requests().len(), 0);
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let held = self
            .runtime
            .drain_execution_tasks(Duration::from_millis(1))
            .await;
        assert_eq!(held.pending, [] as [RunId; 0]);
        assert_eq!(held.pending_preparations.len(), 1);
        let task_id = held.pending_preparations[0].clone();
        self.gate.release.notify_one();
        let first = self
            .runtime
            .drain_execution_tasks(Duration::from_secs(2))
            .await;
        let second = self
            .runtime
            .drain_execution_tasks(Duration::from_secs(2))
            .await;
        let joined = first
            .joined
            .into_iter()
            .chain(second.joined)
            .collect::<Vec<_>>();
        let preparations = first
            .joined_preparations
            .into_iter()
            .chain(second.joined_preparations)
            .collect::<Vec<_>>();
        assert_eq!(
            joined,
            vec![crate::JoinedExecutionTask {
                run_id: run.clone(),
                termination: crate::TaskTermination::Returned
            }]
        );
        assert_eq!(
            preparations,
            vec![crate::JoinedPreparationTask {
                task_id,
                termination: crate::TaskTermination::Returned
            }]
        );
        assert_eq!(second.pending, [] as [RunId; 0]);
        assert_eq!(
            second.pending_preparations,
            [] as [crate::PreparationTaskId; 0]
        );
        assert_eq!(
            self.memory.get_run(run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(self.fake.requests().len(), 1);
        assert_eq!(
            self.memory.list_all_messages(session).await.unwrap().len(),
            2
        );
    }
}
fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("late-admission").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}
#[tokio::test]
async fn abandoned_initial_waiters_keep_late_ordinary_and_keyed_commits_owned() {
    for keyed in [false, true] {
        let (harness, entered) = Harness::new();
        let mut input = request();
        if keyed {
            input.session_id = Some(SessionId::new());
        }
        let runtime = harness.runtime.clone();
        let original = input.clone();
        let waiter = tokio::spawn(async move {
            if keyed {
                drop(runtime.start_keyed(input, options()).await.unwrap());
            } else {
                drop(runtime.start(input).await.unwrap());
            }
        });
        let (session, run) = tokio::time::timeout(Duration::from_secs(2), entered)
            .await
            .unwrap()
            .unwrap();
        harness.abandon_then_finish(waiter, &session, &run).await;
        if keyed {
            let replay = harness
                .runtime
                .start_keyed(original, options())
                .await
                .unwrap();
            assert!(matches!(replay, crate::Admission::Replayed(receipt) if receipt.run_id == run));
            assert_eq!(harness.fake.requests().len(), 1);
        }
    }
}
#[tokio::test]
async fn abandoned_retained_admission_recovery_waiter_keeps_late_claim_owned() {
    let (harness, entered) = Harness::new();
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let (plan, admission) = harness
        .runtime
        .prepare_keyed_admission(&input, options())
        .await
        .unwrap();
    plan.release();
    let crabber_session::KeyedAdmitOutcome::Started { admitted, receipt } = harness
        .memory
        .admit_bound_keyed_run(admission)
        .await
        .unwrap()
    else {
        panic!("fresh seed must admit")
    };
    harness
        .clock
        .set(admitted.run.lease_until + time::Duration::seconds(1));
    let runtime = harness.runtime.clone();
    let original = input.clone();
    let waiter = tokio::spawn(async move {
        drop(runtime.recover_admission(input, options()).await.unwrap());
    });
    let (session, run) = tokio::time::timeout(Duration::from_secs(2), entered)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run, receipt.run_id);
    harness.abandon_then_finish(waiter, &session, &run).await;
    assert!(
        matches!(harness.runtime.recover_admission(original, options()).await.unwrap(), crate::Admission::Replayed(actual) if actual == receipt)
    );
    assert_eq!(harness.fake.requests().len(), 1);
}
