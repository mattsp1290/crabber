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
            context: crate::journey::host_context(),
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
struct Effect {
    #[cfg(feature = "datadog")]
    loss_exporter: Option<crabber::obs::DatadogObserver>,
    #[cfg(feature = "datadog")]
    capture: Arc<Capture>,
}
#[async_trait]
impl ToolExecutor for Effect {
    async fn execute(&self, _: Value) -> Result<Value, crabber::ExtensionError> {
        #[cfg(feature = "datadog")]
        if let Some(exporter) = &self.loss_exporter {
            exporter.flush().await.unwrap();
            let path = std::path::PathBuf::from(std::env::var_os("CRABBER_TRACE_QUEUE").unwrap());
            {
                let context = self.capture.events.lock().unwrap()[0].1.clone().unwrap();
                let attempt = self
                    .capture
                    .observation_attempt
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap();
                persist_attempt(&path, &context, &attempt);
            }
            fs::write(path.parent().unwrap().join("loss-ready"), b"ready").unwrap();
            std::future::pending::<()>().await;
        }
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
    let scripts = if matches!(mode, "resume" | "recover") {
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
    #[cfg(feature = "datadog")]
    let loss_exporter = if mode == "loss" {
        capture.exporter.clone()
    } else {
        None
    };
    Agent::builder()
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(capture.clone())
        .policy(policy)
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "effect".into(),
                description: "controlled effect".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Effect {
                #[cfg(feature = "datadog")]
                loss_exporter,
                #[cfg(feature = "datadog")]
                capture,
            }),
        }))
        .build()
        .unwrap()
}
pub fn next_context(prior: &TraceContext) -> TraceContext {
    // A demo-generated UUID supplies fresh numeric identity without an SDK.
    // Production hosts normally obtain this from their own tracing instrumentation.
    if let (Ok(trace), Ok(span)) = (
        std::env::var("CRABBER_RECOVERY_TRACE_ID"),
        std::env::var("CRABBER_RECOVERY_SPAN_ID"),
    ) {
        return TraceContext::new(&trace, &span)
            .unwrap()
            .linked_to(prior)
            .unwrap();
    }
    let trace = crabber::core::RunId::new().to_string().replace('-', "");
    TraceContext::new(&trace, &trace[..16])
        .unwrap()
        .linked_to(prior)
        .unwrap()
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Attempt {
    context: TraceContext,
    observation_attempt: crabber::core::RunId,
}
fn persist_attempt(path: &Path, context: &TraceContext, attempt: &crabber::core::RunId) {
    let parent = path.parent().unwrap();
    let temporary = parent.join("attempt.tmp");
    let mut file = fs::File::create(&temporary).unwrap();
    file.write_all(
        &serde_json::to_vec(&Attempt {
            context: context.clone(),
            observation_attempt: attempt.clone(),
        })
        .unwrap(),
    )
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
    #[cfg(feature = "datadog")]
    let export = crate::export::ExportCapture::new();
    let capture = Arc::new(Capture {
        #[cfg(feature = "datadog")]
        exporter: Some(export.observer.clone()),
        #[cfg(feature = "datadog")]
        live_exporter: export.live.clone(),
        ..Capture::default()
    });
    let agent = agent(store.clone(), capture.clone(), mode);
    let current = if matches!(mode, "resume" | "recover") {
        let previous = path.parent().unwrap().join("attempt.json");
        let prior: Attempt = serde_json::from_slice(&fs::read(previous).unwrap())
            .unwrap_or_else(|_| panic!("invalid attempt journal"));
        next_context(&prior.context)
            .linked_to_attempt(&prior.context, &prior.observation_attempt)
            .unwrap()
    } else {
        envelope.context.clone()
    };
    // This small fixture has one designated resume worker. A production host must
    // coordinate its durable attempt journal with its worker/queue claim policy.
    // Initial admission identity is already durable in the immutable envelope.
    // Redelivery in the original admission mode must not replace a resumed attempt.

    let (receipt, expected) = if matches!(mode, "resume" | "recover") {
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
        assert_eq!(
            result.status,
            if mode == "recover" {
                RunStatus::Interrupted
            } else {
                RunStatus::Completed
            }
        );
        let new = store.get_run(&receipt.run_id).await.unwrap().unwrap();
        assert_ne!(old.claim_token, new.claim_token);
        assert_eq!(
            agent
                .lookup_admission(&envelope.session, &envelope.key)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        (receipt, usize::from(mode != "recover"))
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
    if expected > 0 || mode == "recover" {
        let attempt = capture.observation_attempt.lock().unwrap().clone().unwrap();
        persist_attempt(path, &current, &attempt);
    }
    {
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
            "backend={} schema={} provider=fake session={} run={} attempt_trace={} attempt_span={} observation_attempt={} predecessor={} predecessor_span={} predecessor_attempt={} callbacks={} models={}",
            backend,
            if backend == "postgres" {
                "4"
            } else {
                "not-applicable"
            },
            envelope.session,
            receipt.run_id,
            current.trace_id(),
            current.span_id(),
            capture
                .observation_attempt
                .lock()
                .unwrap()
                .as_ref()
                .map_or_else(|| "none".to_string(), ToString::to_string),
            current.predecessor().map_or("none", |link| link.trace_id()),
            current.predecessor().map_or("none", |link| link.span_id()),
            current
                .predecessor()
                .and_then(|link| link.observation_attempt())
                .map_or_else(|| "none".to_string(), ToString::to_string),
            events.len(),
            models.len()
        );
    }
    #[cfg(feature = "datadog")]
    {
        let payloads = export.finish(&current, expected).await;
        if !payloads.is_empty() {
            fs::write(
                path.parent().unwrap().join(format!("{mode}-exports.json")),
                serde_json::to_vec(&payloads).unwrap(),
            )
            .unwrap();
        }
        if matches!(mode, "resume" | "recover") {
            let prior: Vec<Value> = serde_json::from_slice(
                &fs::read(path.parent().unwrap().join(if mode == "recover" {
                    "loss-exports.json"
                } else {
                    "pause-exports.json"
                }))
                .unwrap(),
            )
            .unwrap();
            let root = |bodies: &[Value]| {
                bodies
                    .iter()
                    .filter_map(Value::as_array)
                    .flatten()
                    .filter_map(|envelope| envelope.get("spans").and_then(Value::as_array))
                    .flatten()
                    .find(|span| span["meta"]["kind"] == "agent")
                    .unwrap()
                    .clone()
            };
            let before = root(&prior);
            let after = root(&payloads);
            assert_eq!(after["span_links"][0]["trace_id"], before["trace_id"]);
            assert_eq!(after["span_links"][0]["span_id"], before["span_id"]);
        }
    }
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
