//! Small host-owned durable handoff, deliberately not a production queue policy.
use crate::journey::Capture;
use async_trait::async_trait;
use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, FakeProvider, InputFingerprint,
    PermissionDecision, Selection, SessionId, StaticPolicy, StreamDelta, ToolDefinition,
    ToolExecutor, TraceContext,
    core::{RunStatus, ToolCallId, ToolInfo},
    session::{MemoryStore, Store},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fs, io::Write, path::Path, sync::Arc};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub session: SessionId,
    pub key: AdmissionKey,
    pub fingerprint: InputFingerprint,
    pub behavior_fingerprint: InputFingerprint,
    pub prompt: String,
    pub context: TraceContext,
}
impl Envelope {
    pub fn demo() -> Self {
        Self {
            session: SessionId::new(),
            key: AdmissionKey::new("durable-demo").unwrap(),
            fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
            prompt: "safe demo".into(),
            context: TraceContext::new("1234567890abcdef1234567890abcdef", "1234567890abcdef")
                .unwrap(),
        }
    }
    pub fn options(&self) -> AdmissionOptions {
        AdmissionOptions {
            key: self.key.clone(),
            fingerprint: self.fingerprint.clone(),
            behavior_fingerprint: self.behavior_fingerprint.clone(),
        }
    }
}

// Return from API acceptance only after both content and directory entry are durable.
// create_new prevents replacing the original envelope on duplicate delivery.
pub fn enqueue(path: &Path, envelope: &Envelope) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    file.write_all(&serde_json::to_vec(envelope).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}
pub fn read(path: &Path) -> Envelope {
    serde_json::from_slice(&fs::read(path).unwrap())
        .unwrap_or_else(|_| panic!("invalid durable envelope"))
}
struct Effect;
#[async_trait]
impl ToolExecutor for Effect {
    async fn execute(&self, _: Value) -> Result<Value, crabber::ExtensionError> {
        Ok(json!({"ok":true}))
    }
}
struct PausePolicy;
impl crabber::runtime::PermissionPolicy for PausePolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> crabber::runtime::InterruptPolicy {
        crabber::runtime::InterruptPolicy::Pause
    }
}
pub fn agent(store: Arc<dyn Store>, capture: Arc<Capture>, mode: &str) -> Agent {
    let call = ToolCallId::new();
    let text = vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ];
    let scripts = if mode == "resume" {
        vec![text]
    } else {
        vec![
            vec![
                StreamDelta::ToolCallStart {
                    call_id: call.clone(),
                    name: "effect".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: call.clone(),
                    text: "{}".into(),
                },
                StreamDelta::ToolCallDone { call_id: call },
                StreamDelta::Completed,
            ],
            text,
        ]
    };
    let policy: Arc<dyn crabber::runtime::PermissionPolicy> = if mode == "pause" {
        Arc::new(PausePolicy)
    } else {
        Arc::new(StaticPolicy::new(PermissionDecision::Allow))
    };
    Agent::builder()
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(capture)
        .policy(policy)
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "effect".into(),
                description: "controlled effect".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Effect),
        }))
        .build()
        .unwrap()
}
pub fn next_context(prior: &TraceContext) -> TraceContext {
    // A demo-generated UUID supplies fresh numeric identity without an SDK.
    // Production hosts normally obtain this from their own tracing instrumentation.
    let trace = crabber::core::RunId::new().to_string().replace('-', "");
    TraceContext::new(&trace, &trace[..16])
        .unwrap()
        .linked_to(prior)
        .unwrap()
}
fn persist_attempt(path: &Path, context: &TraceContext) {
    let parent = path.parent().unwrap();
    let temporary = parent.join("attempt.tmp");
    let mut file = fs::File::create(&temporary).unwrap();
    file.write_all(&serde_json::to_vec(context).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    fs::rename(temporary, parent.join("attempt.json")).unwrap();
    fs::File::open(parent).unwrap().sync_all().unwrap();
}

#[allow(clippy::too_many_lines)]
pub async fn worker(store: Arc<dyn Store>, path: &Path, mode: &str, backend: &str) {
    let retained = path.parent().unwrap().join("snapshot.json");
    if retained.exists() {
        let evidence: Value = serde_json::from_slice(&fs::read(retained).unwrap()).unwrap();
        let request: crabber::SnapshotRequest =
            serde_json::from_value(evidence["request"].clone()).unwrap();
        let outcome = store.snapshot(request).await.unwrap();
        assert_eq!(serde_json::to_value(outcome).unwrap(), evidence["outcome"]);
    }
    let envelope = read(path);
    let capture = Arc::new(Capture::default());
    let agent = agent(store.clone(), capture.clone(), mode);
    let current = if mode == "resume" {
        let previous = path.parent().unwrap().join("attempt.json");
        let prior = if previous.exists() {
            serde_json::from_slice(&fs::read(previous).unwrap()).unwrap()
        } else {
            envelope.context.clone()
        };
        next_context(&prior)
    } else {
        envelope.context.clone()
    };
    // This small fixture has one designated resume worker. A production host must
    // coordinate its durable attempt journal with its worker/queue claim policy.
    // Initial admission identity is already durable in the immutable envelope.
    // Redelivery in the original admission mode must not replace a resumed attempt.
    if mode == "resume" {
        persist_attempt(path, &current);
    }
    let (receipt, expected) = if mode == "resume" {
        let receipt = agent
            .lookup_admission(&envelope.session, &envelope.key)
            .await
            .unwrap()
            .unwrap();
        let old = store.get_run(&receipt.run_id).await.unwrap().unwrap();
        let result = agent
            .resume_with_context(&receipt.run_id, Some(current.clone()))
            .await
            .unwrap();
        assert_eq!(result.status, RunStatus::Completed);
        let new = store.get_run(&receipt.run_id).await.unwrap().unwrap();
        assert_ne!(old.claim_token, new.claim_token);
        assert_eq!(
            agent
                .lookup_admission(&envelope.session, &envelope.key)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        (receipt, 1)
    } else {
        let admission = agent
            .prompt_keyed_with_context(
                envelope.session.clone(),
                &envelope.prompt,
                envelope.options(),
                Some(current.clone()),
            )
            .await
            .unwrap();
        let receipt = admission.receipt().clone();
        let expected = match admission {
            Admission::Started { handle, .. } => {
                let result = handle.done().await.unwrap();
                assert_eq!(
                    result.status,
                    if mode == "pause" {
                        RunStatus::Paused
                    } else {
                        RunStatus::Completed
                    }
                );
                if mode == "pause" { 1 } else { 2 }
            }
            Admission::Replayed(_) => 0,
        };
        (receipt, expected)
    };
    let events = capture.events.lock().unwrap();
    let models = capture.models.lock().unwrap();
    assert_eq!(models.len(), expected);
    assert!(
        events
            .iter()
            .chain(models.iter())
            .all(|(event, context)| event.run_id == receipt.run_id
                && context.as_ref() == Some(&current))
    );
    if expected > 0 && mode != "pause" {
        assert!(
            events
                .iter()
                .any(|(event, _)| event.kind == crabber::EventKind::ToolCallSettled)
        );
    }
    println!(
        "backend={} schema={} provider=fake session={} run={} attempt_trace={} predecessor={} callbacks={} models={}",
        backend,
        if backend == "postgres" {
            "3"
        } else {
            "not-applicable"
        },
        envelope.session,
        receipt.run_id,
        current.trace_id(),
        current.predecessor().map_or("none", |link| link.trace_id()),
        events.len(),
        models.len()
    );
}
pub async fn memory(path: &Path) {
    let store: Arc<dyn Store> = Arc::new(MemoryStore::new());
    worker(store.clone(), path, "pause", "memory").await;
    worker(store.clone(), path, "resume", "memory").await;
    let journal = fs::read(path.parent().unwrap().join("attempt.json")).unwrap();
    worker(store.clone(), path, "pause", "memory").await;
    worker(store, path, "duplicate", "memory").await;
    assert_eq!(
        fs::read(path.parent().unwrap().join("attempt.json")).unwrap(),
        journal
    );
}
