//! Example-owned first-thread admission. The runtime's ordinary Some(session)
//! path requires an existing session. This wrapper allows first POST creation
//! using the already validated host-selected user-message session ID, atomically
//! in `MemoryStore`'s existing admission transaction. It adds no retry receipt.
use async_trait::async_trait;
use crabber::{
    core::{
        EpochId, EventCursor, EventRecord, Message, Run, RunFence, RunId, Session, SessionId,
        ToolCallRecord,
    },
    session::{
        AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, MemoryStore, Store, StoreError,
    },
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct ThreadStore(pub Arc<MemoryStore>);
#[async_trait]
impl Store for ThreadStore {
    async fn admit_run(&self, mut request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        if request.session_id.as_ref() != Some(&request.user_message.session_id) {
            return Err(StoreError::Validation("thread identity mismatch".into()));
        }
        request.session_id = None;
        self.0.admit_run(request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.0.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.0.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.0.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.0.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.0.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.0.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.0.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.0.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.0.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.0.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.0.enqueue_inbox(session, kind, message).await
    }
}
