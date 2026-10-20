use super::*;
use crabber::session::{
    AdmitRequest, AttemptCleanup, AttemptCommand, AttemptMutation, AttemptMutationOutcome,
    AttemptState, AttemptTarget,
};
use crabber::{
    AttemptControlOutcome, AttemptResolverClaim, AttemptRetirement, AttemptRetirementOutcome,
    AttemptUncertainty, AttemptUncertaintyCause, PendingCallDisposition,
    core::{AttemptGeneration, AttemptId, ControlId, Message, MessageId, Role},
};
#[tokio::test]
async fn closed_agent_parent_controls_initialize_no_factories_or_execution() {
    let store = Arc::new(MemoryStore::new());
    let factories = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![]);
    let agent = Agent::builder()
        .behavior_version("parent-controls.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .extension_factory(Arc::new(super::ForbiddenFactory(factories.clone())))
        .build()
        .unwrap();
    let (admitted, target) = started_fixture(&store).await;
    let session = admitted.session.id.clone();
    agent.close_extensions().await.unwrap();
    let claim = AttemptResolverClaim {
        resolver_id: "test-host".into(),
        authority_claim: "exact fixture owner has no retained worker resources".into(),
    };
    let mark = AttemptUncertainty {
        session_id: session.clone(),
        run_id: admitted.run.id.clone(),
        id: ControlId::new(),
        expected_revision: store.session_revision(&session).await.unwrap(),
        target: target.clone(),
        cause: AttemptUncertaintyCause::HostStopped {
            claim: claim.clone(),
        },
    };
    let AttemptControlOutcome::Applied(marked) =
        agent.mark_attempt_uncertain(mark.clone()).await.unwrap()
    else {
        panic!("mark");
    };
    assert_eq!(marked.attempt.state, AttemptState::OutcomeUnknown);
    let request = AttemptRetirement {
        session_id: session.clone(),
        run_id: admitted.run.id.clone(),
        id: ControlId::new(),
        expected_revision: marked.revision,
        target,
        outcome: AttemptRetirementOutcome::Failed,
        joined: claim,
        pending: PendingCallDisposition::RequireNone,
    };
    let AttemptControlOutcome::Applied(retired) =
        agent.retire_attempt(request.clone()).await.unwrap()
    else {
        panic!("retire");
    };
    assert_eq!(retired.attempt.cleanup, AttemptCleanup::Complete);
    assert_eq!(
        agent.retire_attempt(request).await.unwrap(),
        AttemptControlOutcome::Replayed(retired)
    );
    assert_eq!(
        agent.mark_attempt_uncertain(mark).await.unwrap(),
        AttemptControlOutcome::Replayed(marked)
    );
    assert_eq!(
        store
            .get_run(&admitted.run.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Failed
    );
    assert_eq!(factories.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 0);
}

async fn started_fixture(store: &MemoryStore) -> (crabber::session::AdmitOutcome, AttemptTarget) {
    let session = SessionId::new();
    let now = time::OffsetDateTime::now_utc();
    let admitted = store
        .admit_run(AdmitRequest {
            expected_revision: None,
            additional_messages: Vec::new(),
            input_envelope: None,
            session_id: None,
            workspace_id: "parent-controls".into(),
            directory: "/parent-controls".into(),
            title: "fixture".into(),
            user_message: Message {
                id: MessageId::new(),
                session_id: session.clone(),
                run_id: None,
                role: Role::User,
                parent_id: None,
                created_at: now,
                parts: Vec::new(),
            },
            config_hash: "fixture".into(),
            plan_fingerprint: "fixture".into(),
            owner: "joined-test-worker".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let target = AttemptTarget {
        id: AttemptId::new(),
        generation: AttemptGeneration::INITIAL,
    };
    let AttemptMutationOutcome::Applied { receipt, authority } = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(&session).await.unwrap(),
                command: AttemptCommand::Reserve {
                    target: target.clone(),
                },
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reserve");
    };
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: target.clone(),
                },
            },
            authority,
        )
        .await
        .unwrap();
    (admitted, target)
}
