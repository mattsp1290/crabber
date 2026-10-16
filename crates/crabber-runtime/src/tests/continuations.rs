//! Real policy Ask ends its worker before dispatching any prepared call.
use super::*;
use crabber_core::ToolCallStatus;
use crabber_session::{ApprovalContinuationState, AttemptCleanup, AttemptState, CallApproval};

struct MixedPolicy(Arc<AtomicUsize>);
impl PermissionPolicy for MixedPolicy {
    fn decide(&self, _: &crabber_core::ToolInfo, arguments: &Value) -> PermissionDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        match arguments["text"].as_str().unwrap() {
            "ask" => PermissionDecision::Ask,
            "deny" => PermissionDecision::Deny,
            _ => PermissionDecision::Allow,
        }
    }
}
#[tokio::test]
async fn mixed_batch_is_frozen_and_worker_returns_before_any_executor_starts() {
    for execution_mode in [
        ExecutionMode::Sequential,
        ExecutionMode::Parallel { max: 3 },
    ] {
        let store = Arc::new(MemoryStore::new());
        let effects = Arc::new(AtomicUsize::new(0));
        let mut script = Vec::new();
        for text in ["allow", "ask", "deny"] {
            let mut call = call_script(
                ToolCallId::new(),
                &serde_json::to_string(&json!({"text":text})).unwrap(),
            );
            call.pop();
            script.extend(call);
        }
        script.push(StreamDelta::Completed);
        let provider = FakeProvider::scripted(vec![script]);
        let runtime = Orchestrator::builder()
            .behavior_version("durable-approval-test.v1")
            .store(store.clone())
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(effects.clone())))],
                vec![],
            )))
            .policy(Arc::new(MixedPolicy(Arc::new(AtomicUsize::new(0)))))
            .execution_mode(execution_mode)
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), handle.done())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.status, RunStatus::Paused);
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(provider.requests().len(), 1);
        let pending = store.list_unfinished_tool_calls(&run).await.unwrap();
        assert_eq!(pending.len(), 3);
        assert!(
            pending
                .iter()
                .all(|call| call.status == ToolCallStatus::Pending && call.result.is_none())
        );
        let snapshot = store
            .approval_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.state, ApprovalContinuationState::Waiting);
        assert_eq!(snapshot.calls.len(), 3);
        assert_eq!(snapshot.interactions.len(), 1);
        assert!(
            snapshot
                .calls
                .iter()
                .any(|call| matches!(call.decision, CallApproval::Allow))
        );
        assert!(
            snapshot
                .calls
                .iter()
                .any(|call| matches!(call.decision, CallApproval::Deny))
        );
        for call in &snapshot.calls {
            assert_eq!(
                store.tool_phase(&run, &call.target.id).await.unwrap_err(),
                crabber_session::PhaseError::NotFound
            );
        }
        let attempt = store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt;
        assert_eq!(attempt.state, AttemptState::Suspended);
        assert_eq!(attempt.cleanup, AttemptCleanup::Complete);
        let events = store.list_events(&session, None, 1000).await.unwrap();
        assert!(
            !events
                .iter()
                .any(|event| event.kind == EventKind::ToolCallRunning
                    || event.kind == EventKind::ToolCallSettled)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event.kind == EventKind::RunPaused)
                .count(),
            1
        );
        assert!(matches!(
            runtime.resume(&run).await,
            Err(RuntimeError::DurableContinuationRequired { .. })
        ));
        assert_eq!(effects.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn answer_and_continue_replay_only_the_frozen_batch_once() {
    use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim};
    for reply in [ApprovalReply::Grant, ApprovalReply::Deny] {
        for execution_mode in [
            ExecutionMode::Sequential,
            ExecutionMode::Parallel { max: 3 },
        ] {
            let store = Arc::new(MemoryStore::new());
            let effects = Arc::new(AtomicUsize::new(0));
            let decisions = Arc::new(AtomicUsize::new(0));
            let mut script = Vec::new();
            for text in ["allow", "ask", "deny"] {
                let mut call = call_script(
                    ToolCallId::new(),
                    &serde_json::to_string(&json!({"text":text})).unwrap(),
                );
                call.pop();
                script.extend(call);
            }
            script.push(StreamDelta::Completed);
            let provider =
                FakeProvider::scripted(vec![script, text_script("after approved batch")]);
            let runtime = Orchestrator::builder()
                .behavior_version("durable-approval-test.v1")
                .store(store.clone())
                .resolver(Arc::new(provider.clone()))
                .plan_provider(Arc::new(StaticPlanProvider::new(
                    vec![tool(Arc::new(EchoTool(effects.clone())))],
                    vec![],
                )))
                .policy(Arc::new(MixedPolicy(decisions.clone())))
                .execution_mode(execution_mode)
                .build()
                .unwrap();
            let handle = runtime.start(request()).await.unwrap();
            let session = handle.session_id().clone();
            let run = handle.run_id().clone();
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
            let snapshot = store
                .approval_snapshot(&session, &run)
                .await
                .unwrap()
                .unwrap();
            let (question, authority) = snapshot
                .calls
                .iter()
                .find_map(|call| match &call.decision {
                    CallApproval::Ask { id, description } => {
                        Some((id.clone(), description.authority().clone()))
                    }
                    _ => None,
                })
                .unwrap();
            let answer = ApprovalAnswer {
                id: crabber_core::ControlId::new(),
                expected_revision: snapshot.revision,
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: snapshot.continuation.clone(),
                interaction_id: question,
                resolver: ApprovalResolverClaim {
                    resolver_id: "test-host".into(),
                    authority,
                    authority_claim: "host verified test decision".into(),
                },
                reply,
            };
            let answered = store.answer_approval(answer.clone()).await.unwrap();
            let request = crate::ContinueApproval {
                id: crabber_core::ControlId::new(),
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: snapshot.continuation,
                expected_revision: answered.revision,
            };
            let crate::Continuation::Started { receipt, handle } =
                runtime.continue_approval(request.clone()).await.unwrap()
            else {
                panic!("one fresh continuation handle");
            };
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
            assert_eq!(
                effects.load(Ordering::SeqCst),
                if reply == ApprovalReply::Grant { 2 } else { 1 }
            );
            assert_eq!(decisions.load(Ordering::SeqCst), 3);
            assert_eq!(provider.requests().len(), 2);
            let crate::Continuation::Replayed(replayed) =
                runtime.continue_approval(request).await.unwrap()
            else {
                panic!("receipt replay cannot return another handle");
            };
            assert_eq!(replayed, receipt);
            assert_eq!(store.answer_approval(answer).await.unwrap(), answered);
            assert_eq!(provider.requests().len(), 2);
            assert_eq!(decisions.load(Ordering::SeqCst), 3);
            let snapshot = store
                .approval_snapshot(&session, &run)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(snapshot.state, ApprovalContinuationState::Released);
            assert!(
                snapshot
                    .interactions
                    .iter()
                    .all(|question| question.consumed)
            );
            let attempt = store
                .latest_execution_attempt(&run)
                .await
                .unwrap()
                .unwrap()
                .attempt;
            assert_eq!(attempt.target.generation.get(), 2);
            assert_eq!(attempt.state, AttemptState::Completed);
        }
    }
}
