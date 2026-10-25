use super::{
    AdmitRequest, ContentBlock, ContextEpoch, Duration, EpochId, EventKind, MessageId,
    OffsetDateTime, PartKind, Role, RunStatus, SessionId, Store, ToolCallId, Usage, event, message,
};

async fn projected(
    store: &dyn Store,
    session: &SessionId,
    epoch: &EpochId,
    expected: &[MessageId],
) {
    for selected in [None, Some(epoch.clone())] {
        let messages = store.list_messages(session, selected).await.unwrap();
        assert_eq!(
            messages
                .into_iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[allow(clippy::too_many_lines)] // One history scenario crosses compaction and fresh admission.
pub(super) async fn run(store: &dyn Store, now: OffsetDateTime) {
    let session = SessionId::new();
    let user = message(
        &session,
        None,
        Role::User,
        PartKind::UserInputText,
        "summarized input",
        now,
    );
    let request = AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: None,
        session_id: None,
        workspace_id: "workspace".into(),
        directory: "/tmp".into(),
        title: "compacted followup".into(),
        user_message: user.clone(),
        config_hash: "config".into(),
        plan_fingerprint: "plan".into(),
        owner: "projection-worker".into(),
        lease: Duration::from_secs(30),
    };
    let admitted = store.admit_run(request.clone()).await.unwrap();
    let execution = store.execution(admitted.fence).await.unwrap();
    let epoch = EpochId::new();
    execution
        .start_epoch(ContextEpoch {
            id: epoch.clone(),
            session_id: session.clone(),
            run_id: admitted.run.id.clone(),
            parent: Some(admitted.epoch),
            summarized_range: Some((user.id.clone(), user.id.clone())),
            summary_message_id: None,
            tail_start_message_id: None,
            provider_id: "fake".into(),
            model_id: "test".into(),
            reason: "compact all input".into(),
            next_policy: None,
        })
        .await
        .unwrap();
    let summary = message(
        &session,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::CompactionSummary,
        "summary",
        now,
    );
    execution
        .finish_epoch(&epoch, summary.clone())
        .await
        .unwrap();
    projected(store, &session, &epoch, std::slice::from_ref(&summary.id)).await;
    let assistant = message(
        &session,
        Some(admitted.run.id.clone()),
        Role::Assistant,
        PartKind::AssistantText,
        "after summary",
        now,
    );
    let mut tool = message(
        &session,
        Some(admitted.run.id.clone()),
        Role::Tool,
        PartKind::FunctionToolResult,
        "tool result",
        now,
    );
    tool.parts[0].content = ContentBlock::ToolResult {
        call_id: ToolCallId::new(),
        content: vec![ContentBlock::Text {
            text: "tool result".into(),
        }],
        is_error: false,
    };
    execution.append_message(assistant.clone()).await.unwrap();
    execution.append_message(tool.clone()).await.unwrap();
    let mut expected = vec![summary.id, assistant.id, tool.id];
    projected(store, &session, &epoch, &expected).await;
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            Usage::default(),
            event(&session, &admitted.run.id, EventKind::RunSettled, now),
        )
        .await
        .unwrap();
    let nudge = message(
        &session,
        None,
        Role::User,
        PartKind::UserInputText,
        "continue with a visible answer",
        now,
    );
    let mut followup = request;
    followup.session_id = Some(session.clone());
    followup.user_message = nudge.clone();
    let admitted = store.admit_run(followup).await.unwrap();
    assert_eq!(
        admitted
            .prior_history
            .into_iter()
            .map(|message| message.id)
            .collect::<Vec<_>>(),
        expected
    );
    expected.push(nudge.id);
    projected(store, &session, &admitted.epoch, &expected).await;
}
