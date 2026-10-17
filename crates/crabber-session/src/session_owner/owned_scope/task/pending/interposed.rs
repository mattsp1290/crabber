use super::*;
use crate::session_owner::preparation::{
    InspectPreparedApprovalDriver, PreparedApprovalDriverMetadata, PreparedDriverCompletion,
};
use crate::*;
use crabber_core::*;
use std::collections::BTreeMap;
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Complete,
    Inspect,
    Register,
    Activate,
}
pub(crate) struct Interposed {
    pub inner: Arc<dyn Store>,
    pub stage: Stage,
    pub arrived: Notify,
    pub release: Notify,
    pub fault: bool,
    pub corrupt_receipt: std::sync::atomic::AtomicBool,
    pub corrupt_operation: std::sync::atomic::AtomicBool,
    pub corrupt_replay_run: std::sync::atomic::AtomicBool,
    pub corrupt_version: std::sync::atomic::AtomicBool,
    pub replacement: std::sync::Mutex<Option<PreparationCompletion>>,
    pub approval_complete_calls: AtomicUsize,
    pub approval_inspect_calls: AtomicUsize,
    pub register_calls: AtomicUsize,
    pub corrupt_inspection: std::sync::atomic::AtomicBool,
    pub wrong_permit: std::sync::Mutex<Option<OwnedDispatchPermit>>,
}
impl Interposed {
    pub(crate) fn new(inner: Arc<dyn Store>, stage: Stage, fault: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            stage,
            arrived: Notify::new(),
            release: Notify::new(),
            fault,
            corrupt_receipt: std::sync::atomic::AtomicBool::new(false),
            corrupt_operation: std::sync::atomic::AtomicBool::new(false),
            corrupt_replay_run: std::sync::atomic::AtomicBool::new(false),
            corrupt_version: std::sync::atomic::AtomicBool::new(false),
            replacement: std::sync::Mutex::default(),
            wrong_permit: std::sync::Mutex::default(),
            approval_complete_calls: AtomicUsize::new(0),
            approval_inspect_calls: AtomicUsize::new(0),
            register_calls: AtomicUsize::new(0),
            corrupt_inspection: std::sync::atomic::AtomicBool::new(false),
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
    fn supports_initial_preparation_admission_inspection(&self) -> bool {
        true
    }
    async fn inspect_initial_preparation_admission(
        &self,
        original: &JoinedPreparation,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationAdmissionReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let mut result = self
            .inner
            .inspect_initial_preparation_admission(original)
            .await;
        if let Ok(Some(receipt)) = &mut result {
            if self
                .corrupt_operation
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                receipt.operation_id = ControlId::new();
            }
            if self
                .corrupt_replay_run
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                receipt.run_id = RunId("bad\nrun".into());
            }
        }
        if self.pause(Stage::Inspect).await {
            return Err(Error::UnknownStoreOutcome.into());
        }
        result
    }
    async fn register_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<crate::session_owner::driver::ExecutionDriverOutcome, Error> {
        let result = self.inner.register_execution_driver(request).await;
        if self.pause(Stage::Register).await {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }

    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        let mut receipt = self.inner.lookup_admission(session, key).await?;
        if self
            .corrupt_version
            .load(std::sync::atomic::Ordering::SeqCst)
            && let Some(receipt) = &mut receipt
        {
            receipt.semantic_digest_version = 777;
        }
        Ok(receipt)
    }

    async fn complete_preparation(
        &self,
        original: JoinedPreparation,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let replacement = self.replacement.lock().unwrap().take();
        let mut result = match replacement {
            Some(replacement) => Ok(replacement),
            None => self.inner.complete_preparation(original).await,
        };
        if self
            .corrupt_replay_run
            .load(std::sync::atomic::Ordering::SeqCst)
            && let Ok(PreparationCompletion::AdmissionReplayed(receipt)) = &mut result
        {
            receipt.run_id = RunId("bad\nrun".into());
        }
        if self
            .corrupt_receipt
            .load(std::sync::atomic::Ordering::SeqCst)
            && let Ok(PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                receipt,
                ..
            })) = &mut result
        {
            receipt.user_message_id = MessageId::new();
        }
        if self
            .corrupt_receipt
            .load(std::sync::atomic::Ordering::SeqCst)
            && let Ok(PreparationCompletion::AdmissionRecovery(
                crate::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(value),
            )) = &mut result
        {
            value.record.receipt.user_message_id = MessageId::new();
        }
        if self
            .corrupt_version
            .load(std::sync::atomic::Ordering::SeqCst)
            && let Ok(PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started {
                receipt,
                ..
            })) = &mut result
        {
            receipt.semantic_digest_version = 777;
        }
        if self.pause(Stage::Complete).await {
            return Err(Error::UnknownStoreOutcome.into());
        }
        result
    }
    async fn register_successor_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<crate::session_owner::driver::SuccessorExecutionDriverOutcome, Error> {
        self.register_calls.fetch_add(1, Ordering::SeqCst);
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
    fn supports_pending_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        self.inner.supports_pending_execution_driver_origin(origin)
    }
    async fn complete_preparation_with_driver_source(
        &self,
        original: JoinedPreparation,
    ) -> Result<PreparedDriverCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        self.approval_complete_calls.fetch_add(1, Ordering::SeqCst);
        let mut result = self
            .inner
            .complete_preparation_with_driver_source(original)
            .await;
        if let Ok(value) = &mut result {
            if self.corrupt_version.load(Ordering::SeqCst)
                && let Some(claim) = &mut value.claim
            {
                claim.issuance += 1;
            }
            if self.corrupt_operation.load(Ordering::SeqCst)
                && let Some(claim) = &mut value.claim
            {
                claim.root_join_id = crabber_core::SessionOwnerOperationId::new();
            }
            if self.corrupt_receipt.load(Ordering::SeqCst)
                && let PreparationCompletion::Approval(ApprovalClaimOutcome::Applied {
                    receipt,
                    ..
                }) = &mut value.completion
            {
                receipt.revision = receipt.revision.next().unwrap();
            }
        }
        if self.pause(Stage::Complete).await {
            return Err(Error::UnknownStoreOutcome.into());
        }
        result
    }
    async fn inspect_prepared_approval_driver(
        &self,
        request: InspectPreparedApprovalDriver,
    ) -> Result<PreparedApprovalDriverMetadata, Error> {
        self.approval_inspect_calls.fetch_add(1, Ordering::SeqCst);
        let mut result = self.inner.inspect_prepared_approval_driver(request).await;
        if self.corrupt_inspection.load(Ordering::SeqCst)
            && let Ok(metadata) = &mut result
        {
            metadata.receipt.revision = metadata.receipt.revision.next().unwrap();
        }
        if self.pause(Stage::Inspect).await {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
    async fn lookup_approval(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ApprovalReceipt>, ApprovalError> {
        self.inner.lookup_approval(session, id).await
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
