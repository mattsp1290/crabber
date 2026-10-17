//! Actual native resolution produces closed output before its cleanup is joined.
use crate::{
    ForkBoundary, ForkOutcome, ForkSession, PhaseCommand, PhaseQuiescence, PhaseQuiescenceResult,
    PhaseResolverClaim, PhaseTarget, SessionSummary, SnapshotLimits, Store,
};
use crabber_core::{
    ContentBlock, ControlId, EventKind, EventRecord, Message, MessageId, Part, PartId, PartKind,
    Role, RunId, SessionId, ToolCallRecord, ToolOutput,
};
use time::OffsetDateTime;
pub(crate) struct Fixture {
    pub request: ForkSession,
    pub target: PhaseTarget,
    pub run: RunId,
    pub output: ToolOutput,
}
pub(crate) fn request(source: &SessionSummary) -> ForkSession {
    ForkSession {
        id: ControlId::new(),
        source_session_id: source.session_id.clone(),
        workspace_id: source.workspace_id.clone(),
        directory: source.directory.clone(),
        expected_source_revision: source.revision,
        boundary: ForkBoundary::Empty,
        target_session_id: SessionId::new(),
        title: "copy".into(),
        defaults: source.defaults.clone(),
        limits: SnapshotLimits {
            messages: 64,
            tool_calls: 64,
            parts: 256,
            text_bytes: 4_194_304,
            encoded_bytes: 4_194_304,
        },
        max_proof_bytes: 32 * 1024 * 1024,
    }
}
fn claim() -> PhaseResolverClaim {
    PhaseResolverClaim {
        resolver_id: "fork-resolver".into(),
        authority_claim: "host observed source worker stopped".into(),
    }
}
#[allow(clippy::too_many_lines)] // One real admitted/committed/claimed/resolved source journal.
pub(crate) async fn fixture(store: &impl Store) -> Fixture {
    let session = SessionId::new();
    let now = OffsetDateTime::UNIX_EPOCH;
    let request = crate::execution_binding_contract::fixture(&session, now, "fork-pending", 'b');
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_bound_keyed_run(request).await.unwrap()
    else {
        panic!("admission")
    };
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    let (admitted, execution, owner, phase_claim) =
        crate::phase_contract::fixture_for_admission(store, *admitted).await;
    let id = MessageId::new();
    let message = Message {
        id: id.clone(),
        session_id: session.clone(),
        run_id: Some(admitted.run.id.clone()),
        role: Role::Assistant,
        parent_id: None,
        created_at: now,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id.clone(),
            ordinal: 0,
            kind: PartKind::FunctionToolCall,
            content: ContentBlock::ToolCall {
                call_id: phase_claim.command.target().call_id.clone(),
                name: "phase-owned".into(),
                arguments: serde_json::json!({}),
            },
        }],
    };
    execution.append_message(message).await.unwrap();
    execution
        .append_event(EventRecord {
            cursor: None,
            session_id: session.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: None,
            kind: EventKind::MessageCommitted,
            payload: serde_json::json!({"message_id":id,"role":Role::Assistant}),
            correlation: None,
            live_only: false,
            created_at: now,
        })
        .await
        .unwrap();
    let mut phase_claim = phase_claim;
    phase_claim.expected_revision = store.session_revision(&session).await.unwrap();
    let (claimed, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                phase_claim,
                Some(crate::PhaseWriteAuthority::Attempt(owner)),
            )
            .await
            .unwrap(),
    );
    let (uncertain, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    claimed.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: claimed.phase.target,
                        claim: claim(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let output = ToolOutput::from_json(&serde_json::json!({"protected":"original"})).unwrap();
    let resolution = crate::PhaseResolution {
        session_id: session.clone(),
        run_id: admitted.run.id.clone(),
        message_id: MessageId::new(),
        part_id: PartId::new(),
        resolution: crate::phase_contract::mutation(
            uncertain.revision,
            PhaseCommand::ResolveUncertain {
                target: uncertain.phase.target.clone(),
                claim: claim(),
                choice: crate::UncertainPhaseChoice::Success {
                    output: output.clone(),
                },
            },
        ),
        turn_id: None,
        created_at: now,
        duration_ms: 1,
    };
    let crate::PhaseResolutionOutcome::Applied(receipt) =
        store.resolve_uncertain_phase(resolution).await.unwrap()
    else {
        panic!("resolved source")
    };
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Pending);
    let summary = store.session_summary(&session).await.unwrap();
    let messages = store.list_all_messages(&session).await.unwrap();
    let mut request = self::request(&summary);
    request.boundary = ForkBoundary::AfterMessage {
        message_id: messages.last().unwrap().id.clone(),
    };
    assert_unsafe(store, &request, &messages[0].id).await;
    let quiescence = crate::phase_contract::mutation(
        receipt.revision,
        PhaseCommand::RecordQuiescence {
            target: receipt.phase.target.clone(),
            result: PhaseQuiescenceResult::Incomplete,
            host_claim: Some(claim()),
        },
    );
    let crate::PhaseControlOutcome::Applied(incomplete) = store
        .record_phase_cleanup(crate::PhaseCleanup {
            session_id: session.clone(),
            run_id: admitted.run.id.clone(),
            id: quiescence.id,
            expected_revision: quiescence.expected_revision,
            target: receipt.phase.target.clone(),
            result: PhaseQuiescenceResult::Incomplete,
            claim: claim(),
        })
        .await
        .unwrap()
    else {
        panic!("incomplete")
    };
    assert_eq!(incomplete.phase.quiescence, PhaseQuiescence::Incomplete);
    request.expected_source_revision = incomplete.revision;
    assert_unsafe(store, &request, &messages[0].id).await;
    Fixture {
        request,
        target: receipt.phase.target,
        run: admitted.run.id,
        output,
    }
}
async fn assert_unsafe(store: &impl Store, request: &ForkSession, earlier: &MessageId) {
    assert!(
        matches!(store.fork_session(request.clone()).await.unwrap(),ForkOutcome::UnsafeBoundary{nearest_earlier:ForkBoundary::AfterMessage{message_id}} if message_id==*earlier)
    );
    assert!(
        store
            .get_session(&request.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
}
pub(crate) async fn forge(
    store: &impl Store,
    fixture: &Fixture,
) -> (ToolCallRecord, Message, ToolOutput) {
    let output = ToolOutput::from_json(&serde_json::json!({"protected":"forged"})).unwrap();
    assert_ne!(output, fixture.output);
    let mut call = {
        let crate::SnapshotOutcome::Page(page) = store
            .snapshot(crate::SnapshotRequest {
                session_id: fixture.request.source_session_id.clone(),
                limits: fixture.request.limits,
                continuation: None,
            })
            .await
            .unwrap()
        else {
            panic!("fixture snapshot")
        };
        page.tool_calls
    }
    .into_iter()
    .find(|call| call.id == fixture.target.call_id)
    .unwrap();
    call.result.as_mut().unwrap().content = output.to_content_blocks();
    let mut message=store.list_all_messages(&fixture.request.source_session_id).await.unwrap().into_iter().find(|message|message.parts.iter().any(|part|matches!(&part.content,ContentBlock::ToolResult{call_id,..} if call_id==&fixture.target.call_id))).unwrap();
    for part in &mut message.parts {
        if let ContentBlock::ToolResult { content, .. } = &mut part.content {
            *content = output.to_content_blocks();
        }
    }
    (call, message, output)
}

#[tokio::test]
async fn memory_retained_cleanup_is_unsafe_and_forgery_is_unknown() {
    let store = crate::MemoryStore::with_clock(std::sync::Arc::new(
        crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH),
    ));
    let fixture = fixture(&store).await;
    assert_eq!(
        store
            .get_run(&fixture.run)
            .await
            .unwrap()
            .unwrap()
            .session_id,
        fixture.request.source_session_id
    );
    let (call, message, output) = forge(&store, &fixture).await;
    store.corrupt_fork_call_for_test(&fixture.target, call, message, output);
    assert_eq!(
        store.fork_session(fixture.request).await.unwrap_err(),
        crate::ForkError::UnknownStoreOutcome
    );
}
