mod child;
mod process;

use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
    extension::{Extension, Registrar, Scope, TransformOutput},
    session::Store,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::{Semaphore, watch};

pub const RAW_OUTPUT: &str = "unredacted fixture secret";
pub const REDACTED_OUTPUT: &str = "protected fixture output";

#[derive(Clone)]
pub struct Signal(watch::Sender<bool>);
impl Signal {
    fn new() -> Self {
        Self(watch::channel(false).0)
    }
    fn set(&self) {
        self.0.send_replace(true);
    }
    pub fn observed(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn wait(&self) {
        self.0.subscribe().wait_for(|value| *value).await.unwrap();
    }
}

#[derive(Clone)]
pub struct Probes {
    pub ready_pid: watch::Sender<Option<u32>>,
    pub kill_started: Signal,
    pub reaped: Signal,
    pub pipe_closed: Signal,
    pub callback_dropped: Signal,
    pub redacted: Signal,
    pub permits: Arc<Semaphore>,
    callback_gate: Arc<Semaphore>,
    reap_gate: Arc<Semaphore>,
}
impl Probes {
    pub fn new(hold_reap: bool) -> Self {
        Self {
            ready_pid: watch::channel(None).0,
            kill_started: Signal::new(),
            reaped: Signal::new(),
            pipe_closed: Signal::new(),
            callback_dropped: Signal::new(),
            redacted: Signal::new(),
            permits: Arc::new(Semaphore::new(1)),
            callback_gate: Arc::new(Semaphore::new(0)),
            reap_gate: Arc::new(Semaphore::new(usize::from(!hold_reap))),
        }
    }
    pub async fn ready(&self) -> u32 {
        self.ready_pid
            .subscribe()
            .wait_for(Option::is_some)
            .await
            .unwrap()
            .unwrap()
    }
    pub fn release_callback(&self) {
        self.callback_gate.add_permits(1);
    }
    pub fn release_reap(&self) {
        self.reap_gate.add_permits(1);
    }
    pub fn permit_count(&self) -> usize {
        self.permits.available_permits()
    }
}

struct CallbackDrop(Option<Signal>);
impl Drop for CallbackDrop {
    fn drop(&mut self) {
        if let Some(signal) = &self.0 {
            signal.set();
        }
    }
}

pub struct Reducer {
    pub probes: Probes,
    pub accept_before_reduction: bool,
}
#[async_trait]
impl Extension for Reducer {
    fn id(&self) -> &'static str {
        "fixture-reducer"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.accept_before_reduction.to_string()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        if self.accept_before_reduction {
            registrar.on_result_transform(
                -1,
                "fixture-accept",
                Arc::new(|_, result| Box::pin(async move { Ok(TransformOutput::new(result)) })),
            );
        }
        let probes = self.probes.clone();
        registrar.on_result_transform(
            0,
            "fixture-reduce",
            Arc::new(move |context, result| {
                let probes = probes.clone();
                Box::pin(async move {
                    let mut dropped = CallbackDrop(Some(probes.callback_dropped.clone()));
                    let permit = probes.permits.clone().acquire_owned().await.unwrap();
                    let ready = process::spawn(&context, permit, probes.clone())?;
                    ready
                        .await
                        .map_err(|_| ExtensionError::Tool("fixture readiness lost".into()))??;
                    probes
                        .callback_gate
                        .clone()
                        .acquire_owned()
                        .await
                        .unwrap()
                        .forget();
                    // This probe reports cancellation drops, not successful completion.
                    dropped.0 = None;
                    Ok(TransformOutput::new(result))
                })
            }),
        );
        let probes = self.probes.clone();
        registrar.on_final_redaction(
            -100,
            "fixture-redact",
            Arc::new(move |context, _| {
                let probes = probes.clone();
                Box::pin(async move {
                    assert_eq!(
                        context.phase(),
                        crabber::extension::TransformPhase::FinalRedaction
                    );
                    probes.redacted.set();
                    Ok(TransformOutput::new(json!(REDACTED_OUTPUT)))
                })
            }),
        );
        Ok(())
    }
}

struct FixtureTool;
#[async_trait]
impl ToolExecutor for FixtureTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(RAW_OUTPUT))
    }
}

pub fn agent(store: Arc<dyn Store>, probes: Probes, accept_before_reduction: bool) -> Agent {
    let call = ToolCallId::new();
    Agent::builder()
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(vec![
            vec![
                StreamDelta::ToolCallStart {
                    call_id: call.clone(),
                    name: "fixture".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: call.clone(),
                    text: "{}".into(),
                },
                StreamDelta::ToolCallDone { call_id: call },
                StreamDelta::Completed,
            ],
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "fixture".into(),
                description: "source-only reduction fixture".into(),
                parameters: json!({"type": "object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(FixtureTool),
        }))
        .extension(
            Arc::new(Reducer {
                probes,
                accept_before_reduction,
            }),
            Scope::Global,
        )
        .build()
        .unwrap()
}
