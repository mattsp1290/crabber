//! Public contract gate for nested runtime-owned invocations before delegation uses it.
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExecutionContext, FakeProvider, JsonToolExecutor, PermissionDecision,
    Selection, StaticPolicy, StreamDelta, ToolDefinition,
    core::{
        ControlId, EventKind, InputFingerprint, PublicActivity, PublicSourceChange,
        PublicSourceEvent, RunStatus, SessionContext, SessionId, ToolCallId, ToolInfo,
    },
    extension::{
        ExtensionError, PublicSourceWriter, SessionInvocationAdmission, SessionInvocationRequest,
        ToolContext,
    },
    session::{MemoryStore, Store, session_owner::SessionOwnerState},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

#[derive(Default)]
struct Fixture {
    parent: Mutex<Option<SessionInvocationRequest>>,
    nested: Mutex<Option<SessionInvocationRequest>>,
    nested_ready: Notify,
    parent_release: Notify,
    nested_release: Notify,
}
struct Probe(Arc<Fixture>);
async fn publish(writer: &PublicSourceWriter, name: &str) {
    let snapshot = writer.snapshot().await.unwrap();
    writer
        .publish(
            ControlId::from(format!("contract.{name}")),
            snapshot.revision,
            PublicSourceChange::ActivitySnapshot {
                activity: PublicActivity::new(name, "contract", json!({"status":"running"}))
                    .unwrap(),
            },
        )
        .await
        .unwrap();
}
fn request(name: &str) -> SessionInvocationRequest {
    SessionInvocationRequest::new(
        format!("contract.{name}"),
        InputFingerprint::new("a".repeat(64)).unwrap(),
    )
}
#[async_trait]
impl JsonToolExecutor for Probe {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("runtime context required")
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let parent = request("parent");
        *self.0.parent.lock().unwrap() = Some(parent.clone());
        let fixture = self.0.clone();
        assert_eq!(
            context
                .session_invocations()
                .spawn(parent.clone(), move |inv| async move {
                    publish(&inv.public_source, "parent").await;
                    let nested = request("nested");
                    *fixture.nested.lock().unwrap() = Some(nested.clone());
                    let nested_fixture = fixture.clone();
                    assert_eq!(
                        inv.invocations
                            .spawn(nested, move |child| async move {
                                publish(&child.public_source, "nested").await;
                                nested_fixture.nested_ready.notify_one();
                                nested_fixture.nested_release.notified().await;
                                Ok(())
                            })
                            .await
                            .unwrap(),
                        SessionInvocationAdmission::Started
                    );
                    fixture.parent_release.notified().await;
                    Ok(())
                })
                .await
                .unwrap(),
            SessionInvocationAdmission::Started
        );
        assert_eq!(
            context
                .session_invocations()
                .spawn(parent, |_| async { panic!("replay must not dispatch") })
                .await
                .unwrap(),
            SessionInvocationAdmission::Replayed
        );
        tokio::time::timeout(Duration::from_secs(3), self.0.nested_ready.notified())
            .await
            .unwrap();
        Ok(json!({"started":true}))
    }
}
#[tokio::test]
async fn nested_public_source_keeps_parent_call_and_actual_invocation_identity() {
    let store = Arc::new(MemoryStore::new());
    let fixture = Arc::new(Fixture::default());
    let call = ToolCallId::from("hostile / call é");
    let provider = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "contract".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone {
                call_id: call.clone(),
            },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ]);
    let agent = Agent::builder()
        .behavior_version("nested-invocation-contract.v1")
        .store(store.clone())
        .provider(Arc::new(provider))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "contract".into(),
                description: "public contract gate".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Probe(fixture.clone())),
        }))
        .build()
        .unwrap();
    let execution = ExecutionContext::new(
        SessionContext::new(SessionId::new(), "workspace", "/workspace"),
        Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
    );
    let handle = agent_support::first_prompt(&agent, &execution, "nested contract")
        .await
        .unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    let parent = fixture.parent.lock().unwrap().clone().unwrap();
    let nested = fixture.nested.lock().unwrap().clone().unwrap();
    assert_eq!(
        store
            .get_session_owner(&session, &parent.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert_eq!(
        store
            .get_session_owner(&session, &nested.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let publications = events
        .iter()
        .filter(|e| e.kind == EventKind::ActivitySnapshot)
        .map(|e| serde_json::from_value::<PublicSourceEvent>(e.payload.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(publications.len(), 2);
    for p in &publications {
        assert_eq!(p.source.session_id, session);
        assert_eq!(p.source.run_id, run);
        let invocation = p.source.invocation.as_ref().unwrap();
        assert_eq!(invocation.parent_call_id.as_ref(), Some(&call));
        if invocation.id.0 == nested.id.0 {
            assert_eq!(
                invocation.parent_invocation.as_ref().unwrap().0,
                parent.id.0
            );
        } else {
            assert_eq!(invocation.id.0, parent.id.0);
            assert!(invocation.parent_invocation.is_none());
        }
    }
    assert!(
        agent
            .drain_session_invocations(&SessionId::new(), true, Duration::ZERO)
            .await
            .is_empty()
    );
    fixture.parent_release.notify_one();
    fixture.nested_release.notify_one();
    let joined = agent
        .drain_session_invocations(&session, false, Duration::from_secs(3))
        .await;
    assert_eq!(joined.len(), 2);
    assert!(joined.iter().all(|j| j.local_joined));
    assert_eq!(
        store
            .list_events(&session, None, 1000)
            .await
            .unwrap()
            .iter()
            .filter(|e| e.kind == EventKind::InvocationFinished)
            .count(),
        2
    );
    assert!(agent.close(Duration::from_secs(3)).await.local_drained());
}

#[path = "support/agent.rs"]
mod agent_support;
