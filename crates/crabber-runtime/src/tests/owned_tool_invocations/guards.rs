//! Real permission staging through synchronous owned guard admission.
use super::*;
use crate::RunResult;
use crabber_extension::{GuardContext, GuardDecision, ToolGuard};

#[derive(Default)]
struct Gate {
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
struct Guard {
    gate: Option<Arc<Gate>>,
    observed: Arc<Mutex<Vec<(SessionId, RunId, ToolCallId)>>>,
}
impl ToolGuard for Guard {
    fn id(&self) -> &'static str {
        "context-guard"
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        panic!("actual contextual permission staging required")
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        assert_eq!(context.tool.name, "echo");
        assert_eq!(context.arguments, &json!({"text":"guard"}));
        self.observed.lock().unwrap().push((
            context.session_id.clone(),
            context.run_id.clone(),
            context.call_id.clone(),
        ));
        if let Some(gate) = &self.gate {
            tokio::task::block_in_place(|| {
                gate.entered.notify_one();
                let mut release = gate.release.0.lock().unwrap();
                while !*release {
                    release = gate.release.1.wait(release).unwrap();
                }
            });
        }
        GuardDecision::Allow
    }
}
struct Guards(Arc<Guard>);
#[async_trait]
impl Extension for Guards {
    fn id(&self) -> &'static str {
        "runtime-owned-guards"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.guard(self.0.clone());
        Ok(())
    }
}
pub(super) async fn contract(store: Arc<dyn Store>) {
    for blocked in [false, true] {
        let effects = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let call = ToolCallId::new();
        let gate = blocked.then(|| Arc::new(Gate::default()));
        let release = gate.as_ref().map(|gate| Release(gate.clone()));
        let (runtime, registry) = runtime(
            store.clone(),
            vec![tool(Arc::new(EchoTool(effects.clone())))],
            vec![
                call_script(call.clone(), r#"{"text":"guard"}"#),
                text_script("done"),
            ],
        )
        .await;
        registry
            .mount_owned(
                Arc::new(Guards(Arc::new(Guard {
                    gate: gate.clone(),
                    observed: observed.clone(),
                }))),
                Scope::Global,
            )
            .await
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let pending = if let Some(gate) = &gate {
            cancellation::signal(&gate.entered).await;
            handle.interrupt();
            let pending = runtime
                .drain_execution_tasks(Duration::from_millis(30))
                .await;
            let attempt = store
                .latest_execution_attempt(&run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            Some((pending, attempt, effects.load(Ordering::SeqCst)))
        } else {
            None
        };
        drop(release);
        let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
            .await
            .unwrap();
        registry.close_all().await.unwrap();
        let observed = observed.lock().unwrap();
        assert!(!observed.is_empty());
        assert!(
            observed
                .iter()
                .all(|identity| identity == &(session.clone(), run.clone(), call.clone()))
        );
        if let Some((pending, attempt, before_effects)) = pending {
            assert_eq!(attempt.state, crabber_session::AttemptState::Started);
            assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
            assert!(!pending.pending.is_empty(), "{pending:?}");
            assert_eq!(before_effects, 0);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert!(
                matches!(
                    result,
                    Ok(RunResult {
                        status: RunStatus::Interrupted,
                        ..
                    })
                ),
                "{result:?}"
            );
        } else {
            assert_eq!(result.unwrap().status, RunStatus::Completed);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
        }
    }
}
