//! Actual phase consumers of owned service overrides, including nested disposal.
use super::*;
use crabber_extension::{HostServices, Subprocess, ToolContext, ToolExecutor, WorkspaceFs};
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Default)]
struct Gate {
    entered: Notify,
    disposing: Notify,
    release: (Mutex<bool>, Condvar),
}
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.release.0.lock().unwrap() = true;
        self.0.release.1.notify_all();
    }
}
struct Pending<T>(Arc<Gate>, PhantomData<T>);
impl<T> Future for Pending<T> {
    type Output = Result<T, String>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.notify_one();
        Poll::Pending
    }
}
impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        self.0.disposing.notify_one();
        let mut release = self.0.release.0.lock().unwrap();
        while !*release {
            release = self.0.release.1.wait(release).unwrap();
        }
    }
}
struct Service(Option<Arc<Gate>>);
#[async_trait]
impl Subprocess for Service {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        assert_eq!(program, "program");
        assert_eq!(arguments, &["argument"]);
        if let Some(gate) = &self.0 {
            Pending(gate.clone(), PhantomData).await
        } else {
            Ok(json!("process"))
        }
    }
}
#[async_trait]
impl WorkspaceFs for Service {
    async fn read(&self, path: &str) -> Result<Vec<u8>, String> {
        assert_eq!(path, "file");
        if let Some(gate) = &self.0 {
            Pending(gate.clone(), PhantomData).await
        } else {
            Ok(b"file".to_vec())
        }
    }
}
struct ExtensionServices(Arc<Service>);
#[async_trait]
impl Extension for ExtensionServices {
    fn id(&self) -> &'static str {
        "runtime-owned-host-services"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.host_services(HostServices {
            subprocess: Some(self.0.clone()),
            workspace_fs: Some(self.0.clone()),
        });
        Ok(())
    }
}
struct Consumer(bool);
#[async_trait]
impl ToolExecutor for Consumer {
    async fn execute(&self, _: Value) -> Result<crabber_core::ToolOutput, ExtensionError> {
        panic!("actual phase context required")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<crabber_core::ToolOutput, ExtensionError> {
        let output = if self.0 {
            json!(
                context
                    .host
                    .workspace_fs
                    .unwrap()
                    .read("file")
                    .await
                    .map_err(ExtensionError::Tool)?
            )
        } else {
            context
                .host
                .subprocess
                .unwrap()
                .run("program", &["argument".into()])
                .await
                .map_err(ExtensionError::Tool)?
        };
        crabber_core::ToolOutput::from_json(&output)
            .map_err(|error| ExtensionError::Tool(error.to_string()))
    }
}
pub(super) async fn contract(store: Arc<dyn Store>) {
    for filesystem in [false, true] {
        success(store.clone(), filesystem).await;
        cancelled(store.clone(), filesystem).await;
    }
}
async fn setup(
    store: Arc<dyn Store>,
    filesystem: bool,
    gate: Option<Arc<Gate>>,
) -> (Orchestrator, Arc<Registry>, ToolCallId) {
    let call = ToolCallId::new();
    let (runtime, registry) = runtime(
        store,
        vec![tool(Arc::new(Consumer(filesystem)))],
        vec![
            call_script(call.clone(), r#"{"text":"service"}"#),
            text_script("done"),
        ],
    )
    .await;
    registry
        .mount_owned(
            Arc::new(ExtensionServices(Arc::new(Service(gate)))),
            Scope::Global,
        )
        .await
        .unwrap();
    (runtime, registry, call)
}
async fn success(store: Arc<dyn Store>, filesystem: bool) {
    let (runtime, registry, _) = setup(store.clone(), filesystem, None).await;
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    registry.close_all().await.unwrap();
    assert_eq!(
        store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt
            .cleanup,
        crabber_session::AttemptCleanup::Complete
    );
}
async fn cancelled(store: Arc<dyn Store>, filesystem: bool) {
    let gate = Arc::new(Gate::default());
    let release = Release(gate.clone());
    let (runtime, registry, call) = setup(store.clone(), filesystem, Some(gate.clone())).await;
    let handle = runtime.start(request()).await.unwrap();
    cancellation::signal(&gate.entered).await;
    handle.interrupt();
    cancellation::signal(&gate.disposing).await;
    let pending = runtime
        .drain_execution_tasks(Duration::from_millis(30))
        .await;
    let attempt = store
        .latest_execution_attempt(handle.run_id())
        .await
        .unwrap()
        .unwrap()
        .attempt;
    drop(release);
    let result = tokio::time::timeout(Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    registry.close_all().await.unwrap();
    assert!(
        pending.resources.iter().any(|scope| scope.call_id == call
            && scope.phase.is_some()
            && scope.resources.sealed
            && scope.resources.pending > 0),
        "{pending:?}"
    );
    assert_eq!(attempt.state, crabber_session::AttemptState::Started);
    assert_ne!(attempt.cleanup, crabber_session::AttemptCleanup::Complete);
    assert!(
        matches!(
            result,
            Err(RuntimeError::PhaseOutcomeUnknown {
                reason: crabber_session::PhaseError::Uncertain,
                ..
            })
        ),
        "{result:?}"
    );
}
