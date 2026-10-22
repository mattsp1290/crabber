use super::*;
use crabber::core::{Clock, ManualClock, SystemClock};

#[tokio::test]
async fn store_rejects_stale_authority_schema_and_foreign_targets_without_resuming() {
    let store = Arc::new(MemoryStore::new());
    let provider = FakeProvider::scripted(vec![call(), done()]);
    let (tool, count) = counted().await;
    let agent = builder(store.clone(), provider.clone(), tool)
        .build()
        .unwrap();
    let (_, s) = pause(&agent).await;
    let valid = answer(
        &s,
        0,
        ToolInteractionReply::Answer {
            value: json!({"selected":2}),
        },
    );
    let mut foreign = valid.clone();
    foreign.session_id = "other".into();
    tool_error(
        agent.answer_tool_interaction(foreign).await.unwrap_err(),
        ToolContinuationError::NotFound,
    );
    let mut foreign = valid.clone();
    foreign.interaction_id = crabber::core::InteractionId::new();
    tool_error(
        agent.answer_tool_interaction(foreign).await.unwrap_err(),
        ToolContinuationError::NotFound,
    );
    let mut unauthorized = valid.clone();
    unauthorized.resolver.authority = AuthorityRequirement::new("other", "1").unwrap();
    tool_error(
        agent
            .answer_tool_interaction(unauthorized)
            .await
            .unwrap_err(),
        ToolContinuationError::AuthorityMismatch,
    );
    let invalid = answer(
        &s,
        0,
        ToolInteractionReply::Answer {
            value: json!({"selected":9}),
        },
    );
    tool_error(
        agent.answer_tool_interaction(invalid).await.unwrap_err(),
        ToolContinuationError::SchemaMismatch,
    );
    let accepted = agent.answer_tool_interaction(valid.clone()).await.unwrap();
    assert_eq!(
        agent.answer_tool_interaction(valid.clone()).await.unwrap(),
        accepted
    );
    let stale = answer(&s, 0, ToolInteractionReply::Deny);
    tool_error(
        agent.answer_tool_interaction(stale).await.unwrap_err(),
        ToolContinuationError::RevisionConflict,
    );
    let mut changed = valid;
    changed.reply = ToolInteractionReply::Answer {
        value: json!({"selected":1}),
    };
    tool_error(
        agent.answer_tool_interaction(changed).await.unwrap_err(),
        ToolContinuationError::SemanticConflict,
    );
    assert_eq!(count.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider.requests().len(),
        1,
        "scripted answer text cannot consume the pause"
    );
    assert!(records(&store).await[0].result.is_none());
    assert_eq!(
        agent
            .tool_continuation_snapshot(&s.session_id, &s.run_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        crabber::session::ToolContinuationState::Waiting
    );
    complete(&agent, continuation(&s, accepted.revision)).await;
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn expiry_needs_explicit_host_operation_after_store_deadline() {
    let clock = Arc::new(ManualClock::new(SystemClock.now()));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let mut o = options();
    o.limits.max_wait = Duration::from_secs(1);
    let (tool, count) = counted_with(AskUser::new(o).unwrap(), None).await;
    let agent = builder(
        store.clone(),
        FakeProvider::scripted(vec![call(), done()]),
        tool,
    )
    .build()
    .unwrap();
    let (_, s) = pause(&agent).await;
    tool_error(
        agent
            .answer_tool_interaction(answer(&s, 0, ToolInteractionReply::Expire))
            .await
            .unwrap_err(),
        ToolContinuationError::InvalidRequest,
    );
    clock.set(clock.now() + Duration::from_secs(2));
    assert_eq!(count.resumes.load(Ordering::SeqCst), 0);
    assert!(records(&store).await[0].result.is_none());
    let receipt = agent
        .answer_tool_interaction(answer(&s, 0, ToolInteractionReply::Expire))
        .await
        .unwrap();
    complete(&agent, continuation(&s, receipt.revision)).await;
    assert_eq!(
        value(&records(&store).await[0]),
        json!({"status":"timed_out"})
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn malformed_inputs_checkpoints_and_responses_fail_closed() {
    let tool = definition(extension()).await;
    let r = tool.executor.resumable().unwrap();
    for a in [
        json!({}),
        json!({"question":"q","options":[]}),
        json!({"question":"q","options":[{"label":"same"},{"label":"same"}]}),
        json!({"question":"q","options":[{"label":"\u{1}"},{"label":"second"}]}),
        json!({"question":"q","options":[{"label":"first","description":"\u{1}"},{"label":"second"}]}),
        json!({"question":"\u{1}","options":[{"label":"first"},{"label":"second"}]}),
    ] {
        assert!(r.start(context(), a).await.is_err());
    }
    let cancelled = context();
    cancelled.cancel.cancel();
    assert!(r.start(cancelled.clone(), arguments()).await.is_err());
    let ToolPhaseOutcome::Suspended(s) = r.start(context(), arguments()).await.unwrap() else {
        panic!("suspended")
    };
    let checkpoint = s.checkpoint().clone();
    for (state, v) in [
        (InteractionState::Pending, None),
        (InteractionState::Answered, Some(json!({"custom":" "}))),
        (InteractionState::Answered, Some(json!({"custom":"\0"}))),
        (
            InteractionState::Answered,
            Some(json!({"custom":"é".repeat(3000)})),
        ),
        (InteractionState::Answered, Some(json!({"selected":0}))),
        (
            InteractionState::Answered,
            Some(json!({"selected":1,"custom":"x"})),
        ),
        (InteractionState::Answered, None),
    ] {
        let response = AcceptedToolResponse {
            name: "answer".into(),
            state,
            value: v,
        };
        assert!(
            r.resume(context(), json!({}), checkpoint.clone(), vec![response])
                .await
                .is_err()
        );
    }
    let good = AcceptedToolResponse {
        name: "answer".into(),
        state: InteractionState::Answered,
        value: Some(json!({"selected":2})),
    };
    assert!(
        r.resume(
            context(),
            arguments(),
            ToolCheckpoint::new("wrong", "1", checkpoint.value().clone()).unwrap(),
            vec![good.clone()]
        )
        .await
        .is_err()
    );
    assert!(
        r.resume(
            context(),
            arguments(),
            ToolCheckpoint::new("crabber-extensions/ask-user", "1", json!({})).unwrap(),
            vec![good.clone()]
        )
        .await
        .is_err()
    );
    assert!(
        r.resume(context(), arguments(), checkpoint.clone(), vec![])
            .await
            .is_err()
    );
    assert!(
        r.resume(
            context(),
            arguments(),
            checkpoint.clone(),
            vec![good.clone(), good.clone()]
        )
        .await
        .is_err()
    );
    assert!(
        r.resume(
            cancelled.clone(),
            arguments(),
            checkpoint.clone(),
            vec![good.clone()]
        )
        .await
        .is_err()
    );
    let ToolPhaseOutcome::Completed(output) = r
        .resume(
            context(),
            json!({"tampered":"ignored"}),
            checkpoint,
            vec![good],
        )
        .await
        .unwrap()
    else {
        panic!("completed")
    };
    assert_eq!(output.json_value().unwrap()["answer"], "second");
}
