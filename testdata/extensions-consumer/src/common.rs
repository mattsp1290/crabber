use crabber::{
    Selection, StreamDelta,
    core::{ContentBlock, SessionId, ToolCallId, ToolCallRecord},
    session::{SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use serde_json::{Value, json};
pub fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
pub fn batch(items: &[(&str, Value)]) -> Vec<StreamDelta> {
    let mut result = Vec::new();
    for (name, args) in items {
        let id = ToolCallId::new();
        result.extend([
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: args.to_string(),
            },
            StreamDelta::ToolCallDone { call_id: id },
        ]);
    }
    result.push(StreamDelta::Completed);
    result
}
pub fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
pub fn value(record: &ToolCallRecord) -> Value {
    let [ContentBlock::Text { text }] = &record.result.as_ref().expect("tool result").content[..]
    else {
        panic!("text JSON")
    };
    serde_json::from_str(text).unwrap()
}
pub async fn records(store: &dyn Store, session: &SessionId) -> Vec<ToolCallRecord> {
    let mut continuation = None;
    let mut records = Vec::new();
    loop {
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: session.clone(),
                continuation,
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 1_000_000,
                    encoded_bytes: 2_000_000,
                },
            })
            .await
            .unwrap()
        else {
            panic!("snapshot")
        };
        records.extend(page.tool_calls);
        continuation = page.continuation;
        if continuation.is_none() {
            return records;
        }
    }
}
pub fn ask() -> crabber_extensions::ask_user::AskUser {
    use crabber_extensions::ask_user::{AskUser, Limits, Options};
    AskUser::new(Options {
        authority: crabber::core::AuthorityRequirement::new("operator", "1").unwrap(),
        limits: Limits::default(),
    })
    .unwrap()
}
pub fn question() -> Value {
    json!({"question":"Select a synthetic answer", "options":[{"label":"one"},{"label":"two"}]})
}

pub async fn answer_question(
    agent: &crabber::Agent,
    session: &SessionId,
    run: &crabber::core::RunId,
) {
    use crabber::{
        ContinueTool, ToolContinuation,
        core::ControlId,
        session::{ApprovalResolverClaim, ToolInteractionAnswer, ToolInteractionReply},
    };
    let snapshot = agent
        .tool_continuation_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    let interaction = &snapshot.interactions[0];
    let answer = ToolInteractionAnswer {
        id: ControlId::new(),
        expected_revision: snapshot.revision,
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation.clone(),
        interaction_id: interaction.id.clone(),
        resolver: ApprovalResolverClaim {
            resolver_id: "consumer-host".into(),
            authority: interaction.description.authority().clone(),
            authority_claim: "synthetic authenticated operator".into(),
        },
        reply: ToolInteractionReply::Answer {
            value: json!({"selected":2}),
        },
    };
    let accepted = agent.answer_tool_interaction(answer.clone()).await.unwrap();
    assert_eq!(
        agent.answer_tool_interaction(answer).await.unwrap(),
        accepted
    );
    let request = ContinueTool {
        id: ControlId::new(),
        session_id: session.clone(),
        run_id: run.clone(),
        continuation: snapshot.continuation,
        expected_revision: accepted.revision,
    };
    let ToolContinuation::Started { receipt, handle } =
        agent.continue_tool(request.clone()).await.unwrap()
    else {
        panic!("new continuation")
    };
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber::core::RunStatus::Completed
    );
    assert!(
        matches!(agent.continue_tool(request).await.unwrap(),ToolContinuation::Replayed(r) if r==receipt)
    );
}
pub async fn deny_approval(
    agent: &crabber::Agent,
    session: &SessionId,
    run: &crabber::core::RunId,
) {
    use crabber::{
        Continuation, ContinueApproval,
        core::ControlId,
        session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval},
    };
    let snapshot = agent
        .approval_snapshot(session, run)
        .await
        .unwrap()
        .unwrap();
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("durable permission Ask")
    };
    let accepted = agent
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "consumer-host".into(),
                authority: description.authority().clone(),
                authority_claim: "synthetic authenticated operator".into(),
            },
            reply: ApprovalReply::Deny,
        })
        .await
        .unwrap();
    let Continuation::Started { handle, .. } = agent
        .continue_approval(ContinueApproval {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
            expected_revision: accepted.revision,
        })
        .await
        .unwrap()
    else {
        panic!("approval continuation")
    };
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber::core::RunStatus::Completed
    );
}
