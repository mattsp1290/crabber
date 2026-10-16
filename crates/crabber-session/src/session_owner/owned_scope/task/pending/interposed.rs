use super::*;
use crate::*;
use crabber_core::*;
use std::collections::BTreeMap;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    Complete,
    Register,
    Activate,
}
pub(super) struct Interposed {
    pub inner: Arc<MemoryStore>,
    pub stage: Stage,
    pub arrived: Notify,
    pub release: Notify,
    pub fault: bool,
    pub wrong_permit: std::sync::Mutex<Option<OwnedDispatchPermit>>,
}
impl Interposed {
    pub(super) fn new(inner: Arc<MemoryStore>, stage: Stage, fault: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            stage,
            arrived: Notify::new(),
            release: Notify::new(),
            fault,
            wrong_permit: std::sync::Mutex::default(),
        })
    }
    async fn pause(&self, stage: Stage) -> bool {
        if self.stage != stage {
            return false;
        }
        self.arrived.notify_one();
        self.release.notified().await;
        self.fault
    }
}
#[async_trait::async_trait]
impl Store for Interposed {
    async fn complete_preparation(
        &self,
        original: JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let result = self.inner.complete_preparation(original).await;
        if self.pause(Stage::Complete).await {
            return Err(Error::UnknownStoreOutcome.into());
        }
        result
    }
    async fn register_successor_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<crate::session_owner::driver::SuccessorExecutionDriverOutcome, Error> {
        let result = self
            .inner
            .register_successor_execution_driver(request)
            .await;
        if self.pause(Stage::Register).await {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
    async fn activate_owned_scope(
        &self,
        activation: super::super::super::OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::owned_scope::OwnedScopeActivationOutcome, Error> {
        let result = self
            .inner
            .activate_owned_scope(activation, authority, parent)
            .await;
        if self.pause(Stage::Activate).await {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Ok(crate::session_owner::owned_scope::OwnedScopeActivationOutcome::Applied {
            receipt,
            ..
        }) = &result
            && let Some(permit) = self.wrong_permit.lock().unwrap().take()
        {
            return Ok(
                crate::session_owner::owned_scope::OwnedScopeActivationOutcome::Applied {
                    receipt: receipt.clone(),
                    permit: Box::new(permit),
                },
            );
        }
        result
    }
    async fn admit_run(&self, r: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(r).await
    }
    async fn execution(&self, f: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.inner.execution(f).await
    }
    async fn get_session(&self, s: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(s).await
    }
    async fn get_run(&self, r: &RunId) -> Result<Option<Run>, StoreError> {
        self.inner.get_run(r).await
    }
    async fn list_messages(
        &self,
        s: &SessionId,
        e: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(s, e).await
    }
    async fn list_all_messages(&self, s: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(s).await
    }
    async fn list_events(
        &self,
        s: &SessionId,
        c: Option<EventCursor>,
        n: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(s, c, n).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        r: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(r).await
    }
    async fn claim_expired_run(&self, r: &RunId, o: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(r, o).await
    }
    async fn get_extension_state(
        &self,
        e: &str,
        s: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(e, s).await
    }
    async fn enqueue_inbox(
        &self,
        s: &SessionId,
        k: InboxKind,
        m: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(s, k, m).await
    }
}
