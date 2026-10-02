use super::*;
use std::collections::BTreeMap;
struct ResponseLoss(Arc<dyn Store>);
#[allow(unused_variables)]
#[async_trait]
impl crabber::session::Store for ResponseLoss {
    async fn abandon_run(&self, request: AbandonRequest) -> Result<AbandonOutcome, AbandonError> {
        self.0.abandon_run(request).await?;
        Err(StoreError::Validation("committed response lost".into()).into())
    }
    async fn admit_run(
        &self,
        request: crabber::session::AdmitRequest,
    ) -> Result<crabber::session::AdmitOutcome, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn execution(
        &self,
        fence: RunFence,
    ) -> Result<Box<dyn crabber::session::ExecutionStore>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: crabber::session::InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        panic!("abandonment must only invoke abandon_run")
    }
}

#[tokio::test]
async fn memory_committed_response_loss_exact_request_reconciles() {
    verify_loss(Arc::new(MemoryStore::new())).await;
}
pub(super) async fn verify_loss(store: Arc<dyn Store>) -> (AbandonRequest, AbandonOutcome) {
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            ..admit_request(&SessionId::new())
        })
        .await
        .unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    for running in [false, true] {
        let call = ToolCallRecord {
            id: ToolCallId::new(),
            run_id: admitted.run.id.clone(),
            name: "unavailable".into(),
            arguments: serde_json::json!({}),
            status: ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        execution
            .create_tool_call(
                call.clone(),
                event(&admitted.run, EventKind::ToolCallPending),
            )
            .await
            .unwrap();
        if running {
            execution
                .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                .await
                .unwrap();
        }
    }
    let request = AbandonRequest {
        expected: admitted.fence,
        expected_owner: admitted.run.owner,
        authority: AbandonAuthority::HostStoppedOwner,
    };
    let counters = Arc::new(AtomicUsize::new(0));
    assert!(matches!(
        agent(Arc::new(ResponseLoss(store.clone())), counters.clone())
            .abandon(request.clone())
            .await,
        Err(AbandonError::Store(_))
    ));
    let messages = store
        .list_all_messages(&admitted.run.session_id)
        .await
        .unwrap();
    let events = store
        .list_events(&admitted.run.session_id, None, 100)
        .await
        .unwrap();
    let outcome = agent(store.clone(), counters.clone())
        .abandon(request.clone())
        .await
        .unwrap();
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    assert_eq!(outcome.interrupted_tools.len(), 2);
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap(),
        [] as [crabber_core::ToolCallRecord; 0]
    );
    assert_eq!(
        store
            .list_all_messages(&admitted.run.session_id)
            .await
            .unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&admitted.run.session_id, None, 100)
            .await
            .unwrap(),
        events
    );
    assert_eq!(counters.load(Ordering::SeqCst), 0);
    (request, outcome)
}
