//! Supplied closed pairs remain distinct from observed native execution.
use super::*;
use crabber::core::{
    AdmissionKind, InputContent, InputMessage, InputPart, InputState, MessageContent, Role,
};

fn supplied_pair() -> InputEnvelope {
    let entries = [
        (
            Role::User,
            InputContent::Content {
                content: MessageContent::Text {
                    text: "supplied history".into(),
                },
            },
        ),
        (
            Role::Assistant,
            InputContent::ToolCall {
                call_id: "supplied:原本/call".into(),
                name: "identity".into(),
                arguments: json!({"supplied":true}),
            },
        ),
        (
            Role::Tool,
            InputContent::ToolResult {
                call_id: "supplied:原本/call".into(),
                content: vec![MessageContent::Text {
                    text: "supplied result".into(),
                }],
                is_error: true,
            },
        ),
    ];
    InputEnvelope::new(
        entries
            .into_iter()
            .enumerate()
            .map(|(index, (role, content))| InputMessage {
                id: Some(format!("supplied:原本/message-{index}")),
                parent_id: None,
                role,
                parts: vec![InputPart {
                    id: Some(format!("supplied:原本/part-{index}")),
                    content,
                }],
            })
            .collect(),
        vec![],
        vec![],
        InputState::Absent,
        SessionRevision::INITIAL,
        AdmissionKind::Ordinary,
    )
    .unwrap()
}
#[tokio::test]
async fn memory_fork_preserves_supplied_pairs_without_claiming_native_execution() {
    let store = Arc::new(MemoryStore::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![done()]);
    let agent = worker(store.clone(), provider.clone(), effects.clone());
    let source = SessionId::new();
    let execution = context(&source.0, "fork-workspace", "/fork-workspace", "scripted");
    let Admission::Started { handle, .. } = agent
        .prompt_input_keyed_in(
            &execution,
            supplied_pair(),
            AdmissionOptions {
                key: AdmissionKey::new("supplied-pair").unwrap(),
                fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("source admission")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let messages = store.list_all_messages(&source).await.unwrap();
    let mut partial = fork_request(&agent, store.as_ref(), &source).await;
    partial.boundary = ForkBoundary::AfterMessage {
        message_id: messages[1].id.clone(),
    };
    assert!(
        matches!(agent.fork_session(partial).await.unwrap(), ForkOutcome::UnsafeBoundary { nearest_earlier: ForkBoundary::AfterMessage { message_id } } if message_id == messages[0].id)
    );
    let request = fork_request(&agent, store.as_ref(), &source).await;
    let mut limited = request.clone();
    limited.max_proof_bytes = 0;
    assert!(matches!(
        agent.fork_session(limited).await.unwrap(),
        ForkOutcome::ProofLimited
    ));
    agent.close_extensions().await.unwrap();
    assert!(
        matches!(agent.fork_session(request.clone()).await.unwrap(), ForkOutcome::Applied(receipt) if receipt.imported_calls == 1)
    );
    snapshot::check(
        store.as_ref(),
        &request.target_session_id,
        crabber::core::ImportedCallSource::AdmittedInput,
        messages.len(),
    )
    .await;
    let second = fork_request(&agent, store.as_ref(), &request.target_session_id).await;
    assert!(
        matches!(agent.fork_session(second).await.unwrap(), ForkOutcome::Applied(receipt) if receipt.imported_calls == 1)
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 1);
}
