//! A genuine frontend call and built-in input question share one native wait.
use std::sync::Arc;

use crabber::{
    Agent, AgentConfig, PermissionDecision, StaticPolicy, ToolContinuation, ToolDefinition,
};
use crabber_core::{
    AdmissionKey, AdmissionOptions, AuthorityRequirement, ContentBlock, ControlId, EventKind,
    InputFingerprint, RunStatus, SessionContext, SessionId, SessionRevision, ToolCallId,
    ToolOutput, continuation_source::ResumeSource,
};
use crabber_session::{
    ApprovalResolverClaim, MemoryStore, Store, ToolInteractionAnswer, ToolInteractionReply,
};
use serde_json::json;

use super::{PRIVATE, external_request, frontend_input, projection, selection, support};

fn agent(store: Arc<MemoryStore>, provider: crabber::FakeProvider) -> Agent {
    Agent::builder()
        .behavior_version("native-mixed-projection.v1")
        .store(store)
        .provider(Arc::new(provider))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(
            ToolDefinition::input(
                "request_input",
                AuthorityRequirement::new("operator", "1").unwrap(),
            )
            .unwrap(),
        ))
        .build()
        .unwrap()
}

fn provider(frontend: &ToolCallId, question: &ToolCallId) -> crabber::FakeProvider {
    let mut calls = Vec::new();
    for (id, name, arguments) in [
        (
            question,
            "request_input",
            json!({"questions":[{"name":"color","prompt":"Choose","choices":["blue","red"]}]}),
        ),
        (
            frontend,
            "continuation_echo",
            json!({"text":"native result"}),
        ),
    ] {
        calls.extend([
            crabber::StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: name.into(),
            },
            crabber::StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: arguments.to_string(),
            },
            crabber::StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
        ]);
    }
    calls.push(crabber::StreamDelta::Completed);
    crabber::FakeProvider::scripted(vec![
        calls,
        vec![
            crabber::StreamDelta::TextDelta("finished".into()),
            crabber::StreamDelta::Completed,
        ],
    ])
}

async fn start(agent: &Agent) -> (SessionId, crabber_core::RunId) {
    let session = SessionId::new();
    let context = crabber::ExecutionContext::new(
        SessionContext::new(session.clone(), "native-mixed", "/native-mixed"),
        selection(),
    );
    let crabber::Admission::Started { receipt, handle } = agent
        .prompt_input_keyed_in(
            &context,
            frontend_input(),
            AdmissionOptions {
                key: AdmissionKey::new("mixed-native-wait").unwrap(),
                fingerprint: InputFingerprint::new("e".repeat(64)).unwrap(),
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("fresh native mixed admission")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    (session, receipt.run_id)
}

#[tokio::test]
async fn native_mixed_input_and_frontend_wait_survive_two_sdk_exchanges_and_exact_retry() {
    let store = Arc::new(MemoryStore::new());
    let frontend = ToolCallId::new();
    let question = ToolCallId::new();
    let provider = provider(&frontend, &question);
    let agent = agent(store.clone(), provider.clone());
    let (session, run) = start(&agent).await;
    assert_eq!(provider.requests().len(), 1);
    let first =
        projection::exchange(&store, &session, None, crabber_agui::Completion::Paused).await;
    assert_eq!(first.waiting.frontend_pending, vec![frontend.clone()]);
    assert_eq!(first.waiting.backend_pending, vec![question.clone()]);
    assert_eq!(first.waiting.interrupts.len(), 1);
    assert_eq!(first.waiting.interrupts[0].call_id, question);
    let terminal = serde_json::to_value(first.output.last().unwrap()).unwrap();
    let interrupt = &terminal["outcome"]["interrupts"][0];
    assert_eq!(interrupt["toolCallId"], json!(question));
    assert_eq!(
        interrupt["metadata"]["crabber"]["pendingToolCallIds"],
        json!([frontend])
    );
    let sdk = support::consume(&first.output).await;
    assert_eq!(
        serde_json::to_value(&sdk.outcome).unwrap()["interrupts"][0]["metadata"]["crabber"]["pendingToolCallIds"],
        json!([frontend])
    );
    let snapshot = agent
        .tool_continuation_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(interrupt["id"], json!(snapshot.interactions[0].id));
    assert_eq!(snapshot.interactions[0].call_id, question);
    let revision = answer(&agent, &snapshot).await;
    let mut request = external_request(
        &snapshot,
        ToolOutput::from_json(&json!({"external":"native result"})).unwrap(),
    );
    request.continuation.expected_revision = revision;
    let ToolContinuation::Started { receipt, handle } =
        agent.continue_frontend(request.clone()).await.unwrap()
    else {
        panic!("fresh mixed continuation")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(provider.requests().len(), 2);
    continued(&store, &session, first, sdk.messages, &[frontend, question]).await;
    let revision = store.session_revision(&session).await.unwrap();
    let messages = store.list_all_messages(&session).await.unwrap();
    let events = store.list_events(&session, None, 1000).await.unwrap();
    assert!(
        matches!(agent.continue_frontend(request).await.unwrap(), ToolContinuation::Replayed(saved) if saved == receipt)
    );
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(store.list_all_messages(&session).await.unwrap(), messages);
    assert_eq!(
        store.list_events(&session, None, 1000).await.unwrap(),
        events
    );
    assert_eq!(provider.requests().len(), 2);
    agent.close_extensions().await.unwrap();
}

async fn answer(
    agent: &Agent,
    snapshot: &crabber_session::ToolContinuationSnapshot,
) -> SessionRevision {
    let interaction = &snapshot.interactions[0];
    agent
        .answer_tool_interaction(ToolInteractionAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: snapshot.session_id.clone(),
            run_id: snapshot.run_id.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: interaction.id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "authenticated-host".into(),
                authority: interaction.description.authority().clone(),
                authority_claim: PRIVATE.into(),
            },
            reply: ToolInteractionReply::Answer {
                value: json!("blue"),
            },
        })
        .await
        .unwrap()
        .revision
}

async fn continued(
    store: &MemoryStore,
    session: &SessionId,
    first: projection::Exchange,
    messages: Vec<crabber_agui::ag_ui_core::types::Message>,
    calls: &[ToolCallId],
) {
    let records = projection::records(store, session, Some(first.cursor)).await;
    let resume: ResumeSource = serde_json::from_value(
        records
            .iter()
            .find(|record| record.kind == EventKind::RunResumed)
            .unwrap()
            .payload
            .clone(),
    )
    .unwrap();
    resume.validate().unwrap();
    assert_eq!(resume.calls.len(), 2);
    assert_eq!(resume.continuation, Some(first.waiting.continuation));
    let history = store.list_all_messages(session).await.unwrap();
    for call in calls {
        let frozen = resume
            .calls
            .iter()
            .find(|item| &item.call_id == call)
            .unwrap();
        let parent = history
            .iter()
            .find(|message| message.id == frozen.message_id)
            .unwrap();
        assert!(parent.parts.iter().any(|part| matches!(&part.content,
            ContentBlock::ToolCall { call_id, name, arguments } if call_id == call
                && name == &frozen.name && arguments == &frozen.arguments)));
        assert_eq!(
            records
                .iter()
                .filter(|record| record.kind == EventKind::ToolCallSettled
                    && record.payload["call_id"] == json!(call))
                .count(),
            1
        );
    }
    let output = projection::project(&records, &history, crabber_agui::Completion::Completed);
    projection::terminal(&output);
    for event in first.output.iter().chain(&output) {
        assert!(
            !String::from_utf8(crabber_agui::encode_sse(event, 1_048_576).unwrap())
                .unwrap()
                .contains(PRIVATE)
        );
    }
    let sdk = support::consume_with_messages(&output, messages).await;
    let public = serde_json::to_value(&sdk.messages).unwrap();
    assert!(!public.to_string().contains(PRIVATE));
    let items = public.as_array().unwrap();
    for call in calls {
        let frozen = resume
            .calls
            .iter()
            .find(|item| &item.call_id == call)
            .unwrap();
        assert_eq!(
            items
                .iter()
                .filter(|message| message["id"] == json!(frozen.message_id))
                .count(),
            1
        );
        let settled = records
            .iter()
            .find(|record| {
                record.kind == EventKind::ToolCallSettled
                    && record.payload["call_id"] == json!(call)
            })
            .unwrap();
        assert_eq!(
            items
                .iter()
                .filter(|message| message["id"] == settled.payload["message_id"]
                    && message["toolCallId"] == json!(call))
                .count(),
            1
        );
    }
    assert!(matches!(
        sdk.outcome,
        Some(
            crabber_agui::ag_ui_core::types::RunFinishedOutcome::Success {
                pending_tool_call_ids: None
            }
        )
    ));
}
