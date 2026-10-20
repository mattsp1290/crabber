use super::*;
use crabber_core::{
    AcceptedToolResponse, InputFingerprint, ResumableToolIdentity, ToolCheckpoint, ToolPhaseOutcome,
};
use crabber_extension::{ResumableTool, ToolContext};
use std::sync::Mutex;

fn identity(version: &str) -> ResumableToolIdentity {
    ResumableToolIdentity {
        component_id: "fixture".into(),
        component_version: version.into(),
        config_fingerprint: InputFingerprint::new("1".repeat(64)).unwrap(),
        checkpoint_schema_id: "fixture".into(),
        checkpoint_schema_version: "1".into(),
    }
}
struct Machine {
    declaration: Mutex<ResumableToolIdentity>,
    capability: Mutex<ResumableToolIdentity>,
    metadata_calls: AtomicUsize,
    starts: AtomicUsize,
    resumes: AtomicUsize,
}
impl Machine {
    fn new() -> Self {
        Self {
            declaration: Mutex::new(identity("1")),
            capability: Mutex::new(identity("1")),
            metadata_calls: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            resumes: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl ToolExecutor for Machine {
    fn resumable_identity(&self) -> Option<ResumableToolIdentity> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Some(self.declaration.lock().unwrap().clone())
    }
    fn resumable(&self) -> Option<&dyn ResumableTool> {
        Some(self)
    }
    async fn execute(&self, _: Value) -> Result<ToolOutput, ExtensionError> {
        panic!("resumable invocation used ordinary execute")
    }
}
#[async_trait]
impl ResumableTool for Machine {
    fn identity(&self) -> ResumableToolIdentity {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        self.capability.lock().unwrap().clone()
    }
    async fn start(&self, _: ToolContext, args: Value) -> Result<ToolPhaseOutcome, ExtensionError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&args).unwrap(),
        ))
    }
    async fn resume(
        &self,
        _: ToolContext,
        args: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        assert_eq!(args, Value::Bool(true));
        assert_eq!(checkpoint, checkpoint_fixture());
        assert_eq!(responses, responses_fixture());
        self.resumes.fetch_add(1, Ordering::SeqCst);
        Ok(ToolPhaseOutcome::Completed(
            ToolOutput::from_json(&args).unwrap(),
        ))
    }
}
fn checkpoint_fixture() -> ToolCheckpoint {
    ToolCheckpoint::new("fixture", "1", serde_json::json!({"private":true})).unwrap()
}
fn responses_fixture() -> Vec<AcceptedToolResponse> {
    vec![AcceptedToolResponse {
        name: "answer".into(),
        state: crabber_core::InteractionState::Answered,
        value: Some(Value::Bool(false)),
    }]
}

#[tokio::test]
async fn frozen_metadata_no_user_requery_and_live_resume_preserves_both_identities() {
    for changed in ["none", "declaration", "capability", "initial_mismatch"] {
        let machine = Arc::new(Machine::new());
        if changed == "initial_mismatch" {
            *machine.declaration.lock().unwrap() = identity("2");
        }
        let registry = Registry::new();
        let mount = registry
            .mount_owned(Arc::new(Tools(machine.clone())), Scope::Global)
            .await
            .unwrap();
        let calls = machine.metadata_calls.load(Ordering::SeqCst);
        let plan = registry.acquire(&SessionId::new());
        let second = registry.acquire(&SessionId::new());
        assert_eq!(
            machine.metadata_calls.load(Ordering::SeqCst),
            calls,
            "plan projection must not invoke original user metadata"
        );
        assert_eq!(plan.fingerprint, second.fingerprint);
        drop(second);
        let executor = plan.tools[0].executor.clone();
        let tool = executor.resumable().unwrap();
        if changed == "declaration" {
            *machine.declaration.lock().unwrap() = identity("2");
        }
        if changed == "capability" {
            *machine.capability.lock().unwrap() = identity("2");
        }
        let resumed = tool
            .resume(
                context(),
                Value::Bool(true),
                checkpoint_fixture(),
                responses_fixture(),
            )
            .await;
        assert_eq!(resumed.is_ok(), changed == "none");
        assert_eq!(machine.starts.load(Ordering::SeqCst), 0);
        assert_eq!(
            machine.resumes.load(Ordering::SeqCst),
            usize::from(changed == "none")
        );
        let started = tool.start(context(), Value::Null).await;
        assert_eq!(started.is_ok(), changed == "none");
        drop(plan);
        mount.close(Duration::from_secs(1)).await.unwrap();
        assert!(tool.start(context(), Value::Null).await.is_err());
        assert!(
            tool.resume(
                context(),
                Value::Bool(true),
                checkpoint_fixture(),
                responses_fixture()
            )
            .await
            .is_err()
        );
        assert_eq!(
            machine.starts.load(Ordering::SeqCst),
            usize::from(changed == "none")
        );
    }
}
