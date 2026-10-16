//! Original frontend metadata remains readable without materializing a mutable Run.
use super::*;
use crabber_core::{
    Clock, ControlId, FrontendResultStatus, FrontendToolResult, InputEnvelope, InputFingerprint,
    SessionRevision, ToolOutput,
};
use crabber_session::{
    FrontendContinuationClaim, FrontendResolverClaim, ToolContinuationClaim,
    ToolContinuationClaimOutcome,
};

#[tokio::test]
#[allow(clippy::too_many_lines)] // Complete original-claim/recovery reconciliation journey.
async fn frontend_claim_and_recovery_replay_never_read_current_run() {
    for recover in [false, true] {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::now_utc()));
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let provider = FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), "{}"),
            text_script("done"),
        ]);
        let make = |selected: Arc<dyn Store>| {
            Orchestrator::builder()
                .behavior_version("frontend-replay.v1")
                .clock(clock.clone())
                .store(selected)
                .resolver(Arc::new(provider.clone()))
                .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
                .build()
                .unwrap()
        };
        let runtime = make(store.clone());
        let mut admission = request();
        admission.text.clear();
        let mut input = serde_json::to_value(
            InputEnvelope::text("external", SessionRevision::INITIAL).unwrap(),
        )
        .unwrap();
        input["frontend_tools"] = json!([{"name":"echo", "description":"external", "input_parameters":{"type":"object"}, "result_contract":null}]);
        admission.input_envelope = Some(serde_json::from_value(input).unwrap());
        let handle = runtime.start(admission).await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let waiting = store
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        let original = crate::ContinueFrontend {
            continuation: crate::ContinueTool {
                id: ControlId::new(),
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: waiting.continuation,
                expected_revision: waiting.revision,
            },
            resolver: FrontendResolverClaim {
                resolver_id: "host".into(),
                authority_claim: "verified".into(),
            },
            results: vec![
                FrontendToolResult::new(
                    waiting.frontend_calls[0].call_id.clone(),
                    FrontendResultStatus::Completed,
                    ToolOutput::from_json(&json!("external result")).unwrap(),
                )
                .unwrap(),
            ],
        };
        let recovery = if recover {
            let stored = store.get_run(&run).await.unwrap().unwrap();
            let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
                .claim_frontend_continuation(FrontendContinuationClaim {
                    claim: ToolContinuationClaim {
                        id: original.continuation.id.clone(),
                        session_id: session.clone(),
                        run_id: run.clone(),
                        continuation: original.continuation.continuation.clone(),
                        expected_revision: original.continuation.expected_revision,
                        expected_plan: InputFingerprint::new(stored.plan_fingerprint).unwrap(),
                        owner: "lost".into(),
                        lease_ms: 1,
                    },
                    resolver: original.resolver.clone(),
                    results: original.results.clone(),
                })
                .await
                .unwrap()
            else {
                panic!("fresh frontend claim")
            };
            clock.set(clock.now() + time::Duration::milliseconds(2));
            Some(crate::RecoverFrontend {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                original: original.clone(),
            })
        } else {
            None
        };
        let started = if let Some(recovery) = &recovery {
            runtime
                .recover_frontend_continuation(recovery.clone())
                .await
                .unwrap()
        } else {
            runtime.continue_frontend(original.clone()).await.unwrap()
        };
        let crate::ToolContinuation::Started { receipt, handle } = started else {
            panic!("frontend worker")
        };
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        let gate = Arc::new(SettlementGate::default());
        gate.reject_run_reads.store(true, Ordering::SeqCst);
        let cold = make(Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate,
        }));
        let before = store.list_events(&session, None, 1000).await.unwrap();
        let replay = if let Some(recovery) = &recovery {
            cold.recover_frontend_continuation(recovery.clone())
                .await
                .unwrap()
        } else {
            cold.continue_frontend(original.clone()).await.unwrap()
        };
        assert!(matches!(replay, crate::ToolContinuation::Replayed(saved) if saved == receipt));
        // Both metadata APIs validate the original complete set and captured R.
        let mut changed = original.clone();
        changed.continuation.expected_revision =
            changed.continuation.expected_revision.next().unwrap();
        assert!(matches!(
            cold.lookup_frontend_continuation(&changed).await,
            Err(RuntimeError::ToolContinuation(
                crabber_session::ToolContinuationError::SemanticConflict
            ))
        ));
        changed = original.clone();
        changed.resolver.authority_claim.push_str(" changed");
        assert!(matches!(
            cold.lookup_frontend_continuation(&changed).await,
            Err(RuntimeError::ToolContinuation(
                crabber_session::ToolContinuationError::SemanticConflict
            ))
        ));
        if let Some(mut changed) = recovery {
            changed.expected_revision = changed.expected_revision.next().unwrap();
            assert!(matches!(
                cold.lookup_frontend_recovery(&changed).await,
                Err(RuntimeError::ToolContinuation(
                    crabber_session::ToolContinuationError::SemanticConflict
                ))
            ));
        }
        assert_eq!(
            store.list_events(&session, None, 1000).await.unwrap(),
            before
        );
        assert_eq!(provider.requests().len(), 2);
    }
}
