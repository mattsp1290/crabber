use super::*;
use crabber_providers::{FakeProvider, Selection, StreamDelta};
use crabber_session::{MemoryStore, Store};
use std::sync::Arc;

#[tokio::test]
async fn rejected_approval_continuation_records_failed_status() {
    let store = Arc::new(MemoryStore::new());
    let runtime = Orchestrator::builder()
        .behavior_version("approval-error-telemetry.v1")
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("complete".into()),
            StreamDelta::Completed,
        ]])))
        .plan_provider(Arc::new(crabber_extension::StaticPlanProvider::new(
            vec![],
            vec![],
        )))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    handle.done().await.unwrap();
    let run = store.get_run(&run_id).await.unwrap().unwrap();
    let claim = ContinueApproval {
        id: ControlId::new(),
        session_id: run.session_id.clone(),
        run_id,
        continuation: ContinuationTarget {
            id: crabber_core::ContinuationId::new(),
            generation: crabber_core::ContinuationGeneration::INITIAL,
        },
        expected_revision: SessionRevision::INITIAL,
    }
    .claim(&run)
    .unwrap();
    let (sender, _receiver) = oneshot::channel();
    crate::tests::tracing_spans::assert_failed_approval_span(
        "crabber.approval.continue",
        runtime.acquire_resume(
            &run,
            super::super::recovery::ResumeRequest::Continuation(Box::new(
                ApprovalResume::Approval {
                    prepared: None,
                    control: ApprovalControl::Claim(claim),
                    sender,
                },
            )),
        ),
    )
    .await;
}

fn request() -> crate::Request {
    crate::Request {
        selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
        captured_revision: None,
        input_envelope: None,
        session_id: None,
        workspace_id: "test".into(),
        directory: "/tmp".into(),
        title: "test".into(),
        text: "test".into(),
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
        system_prompt: None,
        max_output_tokens: None,
        model_controls: crabber_core::ModelControls::default(),
    }
}

#[tokio::test]
async fn invalid_prepared_authority_does_not_record_approved_outcome() {
    use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};
    let store = Arc::new(MemoryStore::new());
    let runtime = approval_runtime(store.clone());
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let run = store.get_run(&run_id).await.unwrap().unwrap();
    let snapshot = runtime
        .approval_snapshot(&run.session_id, &run.id)
        .await
        .unwrap()
        .unwrap();
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("expected approval");
    };
    let receipt = runtime
        .answer_approval(ApprovalAnswer {
            id: ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "test".into(),
                authority: description.authority().clone(),
                authority_claim: "test".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let claim = ContinueApproval {
        id: ControlId::new(),
        session_id: run.session_id.clone(),
        run_id,
        continuation: snapshot.continuation,
        expected_revision: receipt.revision,
    }
    .claim(&run)
    .unwrap();
    let mut outcome = store
        .claim_approval_continuation(claim.clone())
        .await
        .unwrap();
    let ApprovalClaimOutcome::Applied { grant, .. } = &mut outcome else {
        panic!("fresh claim required");
    };
    grant.usage.output_tokens += 1; // Preserve the actual authority; corrupt only its binding metadata.
    let (sender, _receiver) = oneshot::channel();
    crate::tests::tracing_spans::assert_failed_approval_span("crabber.approval.continue", async {
        let result = runtime.acquire_resume(&run, super::super::recovery::ResumeRequest::Continuation(Box::new(
            ApprovalResume::Approval { prepared: Some(crabber_session::session_owner::preparation::PreparationCompletion::Approval(outcome)),
                control: ApprovalControl::Claim(claim), sender },
        ))).await;
        assert!(matches!(result, Err(RuntimeError::Approval(ApprovalError::UnknownStoreOutcome))));
        result
    }).await;
}

fn approval_runtime(store: Arc<MemoryStore>) -> Orchestrator {
    struct Unused;
    #[async_trait::async_trait]
    impl crabber_extension::JsonToolExecutor for Unused {
        async fn execute(
            &self,
            _: serde_json::Value,
        ) -> Result<serde_json::Value, crabber_extension::ExtensionError> {
            panic!("approval test must not dispatch tool effects");
        }
    }
    let call_id = crabber_core::ToolCallId::new();
    Orchestrator::builder()
        .behavior_version("approval-authority-telemetry.v1")
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: "ask".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id },
            StreamDelta::Completed,
        ]])))
        .plan_provider(Arc::new(crabber_extension::StaticPlanProvider::new(
            vec![Arc::new(crabber_extension::ToolDefinition {
                info: crabber_core::ToolInfo {
                    name: "ask".into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type":"object"}),
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(Unused),
            })],
            vec![],
        )))
        .policy(Arc::new(crate::StaticPolicy::new(
            crate::PermissionDecision::Ask,
        )))
        .build()
        .unwrap()
}
