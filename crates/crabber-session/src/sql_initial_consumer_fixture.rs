//! Test-only forwarding of SAME opaque calls; observation never constructs permission.
use crate::native_enrollment::initial_consumer::CallKind;
use crate::{
    Store,
    session_owner::{
        owned_scope::{
            PendingInitialConsumerNativeCall, PendingInitialConsumerNativeReply,
            PendingInitialNativeCall, PendingInitialNativeReply,
        },
        preparation::PreparationCommitError,
    },
};
use crabber_core::RunId;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
pub(super) struct Forward {
    pub inner: Arc<dyn Store>,
    pub kind: CallKind,
    pub armed: Arc<AtomicBool>,
    pub calls: std::sync::Mutex<Vec<CallKind>>,
    pub fail_first_open: AtomicBool,
}
#[async_trait::async_trait]
impl Store for Forward {
    async fn admit_run(
        &self,
        request: crate::AdmitRequest,
    ) -> Result<crate::AdmitOutcome, crate::StoreError> {
        self.inner.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crate::ExecutionStore>, crate::StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crate::StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<crabber_core::Run>, crate::StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crate::StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<crabber_core::Run>, crate::StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crate::StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::StoreError> {
        self.inner.get_extension_state(extension, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crate::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crate::StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }

    async fn pending_initial_native(
        &self,
        call: PendingInitialNativeCall<'_>,
    ) -> Result<PendingInitialNativeReply, PreparationCommitError> {
        if self.fail_first_open.swap(false, Ordering::SeqCst) {
            return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into());
        }
        self.inner.pending_initial_native(call).await
    }
    async fn pending_initial_consumer_native(
        &self,
        call: PendingInitialConsumerNativeCall<'_>,
    ) -> Result<PendingInitialConsumerNativeReply, PreparationCommitError> {
        let kind = call.operation_kind();
        self.calls.lock().unwrap().push(kind);
        if kind == self.kind {
            self.armed.store(true, Ordering::SeqCst);
        }
        self.inner.pending_initial_consumer_native(call).await
    }
}
