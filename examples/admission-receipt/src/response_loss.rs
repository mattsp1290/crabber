//! Demo-only transport fault. Forwards the production transaction contract.
use async_trait::async_trait;
use crabber::core::{
    AdmissionKey, AdmissionReceipt, EpochId, EventCursor, EventRecord, Message, Run, RunFence,
    RunId, Session, SessionId, ToolCallRecord,
};
use crabber::session::{
    AdmissionExecutionError, AdmitOutcome, AdmitRequest, ClaimUnstartedAdmissionRequest,
    ClaimedAdmission, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest, Store,
    StoreError,
};
use std::{collections::BTreeMap, sync::Arc};
pub(super) struct LostAdmissionReply {
    pub(super) inner: Arc<dyn Store>,
}
#[async_trait]
impl Store for LostAdmissionReply {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        mut request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        request.request.lease = std::time::Duration::from_secs(2);
        self.inner.admit_keyed_run(request).await?;
        // The actual transaction commits; its response never reaches the runtime.
        Err(StoreError::Validation(
            "injected unknown admission response".into(),
        ))
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber::core::AdmissionKey,
    ) -> Result<
        Option<crabber::session::AdmissionExecutionRecord>,
        crabber::session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, AdmissionExecutionError> {
        self.inner.claim_unstarted_admission(request).await
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber::session::AdmissionExecutionState>, StoreError> {
        self.inner.admission_execution_state(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
}
