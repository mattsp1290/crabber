use super::*;
use crabber_extension::{StateSink, ToolContext, current_state_sink, with_state_sink};
use std::{collections::BTreeMap, sync::Mutex};

struct Sink {
    session: SessionId,
    values: Mutex<BTreeMap<String, String>>,
}
#[async_trait]
impl StateSink for Sink {
    fn session_id(&self) -> &SessionId {
        &self.session
    }
    async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
        Ok(self.values.lock().unwrap().clone())
    }
    async fn apply(&self, _: &str, changes: Vec<(String, Option<String>)>) -> Result<(), String> {
        for (key, value) in changes {
            if let Some(value) = value {
                self.values.lock().unwrap().insert(key, value);
            }
        }
        Ok(())
    }
}
struct Contextual {
    sink: Arc<dyn StateSink>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ToolExecutor for Contextual {
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        Ok(ToolOutput::from_json(&Value::String("no context".into())).unwrap())
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<ToolOutput, ExtensionError> {
        assert_eq!(context.session_id, *self.sink.session_id());
        assert_eq!(context.workspace().directory(), Some("/original/workspace"));
        let sink = current_state_sink().unwrap();
        assert!(Arc::ptr_eq(&sink, &self.sink));
        sink.apply("tool", vec![("state".into(), Some("saved".into()))])
            .await
            .unwrap();
        context.progress(crabber_core::ContentBlock::Text {
            text: "progress".into(),
        });
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ToolOutput::from_json(&arguments).unwrap())
    }
}
#[tokio::test]
async fn original_context_and_exact_fenced_state_sink_survive_retained_task() {
    let session = SessionId::new();
    let sink: Arc<dyn StateSink> = Arc::new(Sink {
        session: session.clone(),
        values: Mutex::new(BTreeMap::new()),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Tools(Arc::new(Contextual {
                sink: sink.clone(),
                calls: calls.clone(),
            }))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&session);
    let progress = Arc::new(AtomicUsize::new(0));
    let observed = progress.clone();
    let context = ToolContext::new(
        session,
        crabber_core::RunId::new(),
        crabber_core::ToolCallId::new(),
        tokio_util::sync::CancellationToken::new(),
        crabber_extension::HostServices::default(),
        crabber_extension::WorkspaceContext::from_persisted("w", "/original/workspace"),
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        }),
    );
    with_state_sink(
        sink.clone(),
        plan.tools[0]
            .executor
            .execute_with_context(context, Value::Bool(true)),
    )
    .await
    .unwrap();
    plan.tools[0].executor.execute(Value::Null).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(progress.load(Ordering::SeqCst), 1);
    assert_eq!(
        sink.snapshot("tool").await.unwrap().get("state").unwrap(),
        "saved"
    );
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
}
