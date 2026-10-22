//! An already-started control read must progress outside a blocked callback task.
use super::*;

#[derive(Default)]
struct CallbackGate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
struct Policy(Arc<CallbackGate>);
impl PermissionPolicy for Policy {
    fn decide(&self, _: &crabber_core::ToolInfo, _: &Value) -> PermissionDecision {
        tokio::task::block_in_place(|| {
            self.0.entered.notify_one();
            let mut open = self.0.open.lock().unwrap();
            while !*open {
                open = self.0.changed.wait(open).unwrap();
            }
        });
        PermissionDecision::Allow
    }
}
struct Release(Arc<CallbackGate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.open.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_observation_progresses_while_policy_callback_is_blocked() {
    let observation = Arc::new(ObservationGate::default());
    let callback = Arc::new(CallbackGate::default());
    let release = Release(callback.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_version("blocked-control-observation.v1")
        .store(Arc::new(DelayedTerminalStore {
            inner: Arc::new(MemoryStore::new()),
            gate: Arc::new(SettlementGate {
                observation: Some(observation.clone()),
                ..Default::default()
            }),
        }))
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"once"}"#),
            text_script("done"),
        ])))
        .model_stream(Arc::new(HeldModel(observation.clone())))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(effects.clone())))],
            vec![],
        )))
        .policy(Arc::new(Policy(callback.clone())))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    wait(&observation.model_entered, "model before callback").await;
    wait(&observation.observer_entered, "original control read").await;
    observation.release_model.notify_one();
    wait(&callback.entered, "blocked policy callback").await;
    observation.release_observer.notify_one();
    let progressed = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        observation.observer_returned.notified(),
    )
    .await;
    // Release the synchronous callback before any assertion unwinds the runtime.
    drop(release);
    assert!(
        progressed.is_ok(),
        "original control read froze with callback"
    );
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let drained = runtime
        .drain_execution_tasks(std::time::Duration::from_secs(3))
        .await;
    assert_eq!(drained.pending, [] as [RunId; 0]);
}
