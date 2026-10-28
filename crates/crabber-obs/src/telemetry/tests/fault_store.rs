pub(super) struct FaultStore {
    pub inner: crabber_session::MemoryStore,
    pub limit_on_completion: bool,
    pub lose_cleanup_ack: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crabber_session::Store for FaultStore {
    fn supports_session_owners(&self) -> bool {
        self.inner.supports_session_owners()
    }
    fn supports_preparation_protocol(&self) -> bool {
        self.inner.supports_preparation_protocol()
    }
    async fn reserve_preparation(
        &self,
        request: crabber_session::session_owner::preparation::ReservePreparation,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.reserve_preparation(request).await
    }
    async fn lookup_preparation(
        &self,
        session: &crabber_core::SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_preparation(session, key).await
    }
    async fn complete_preparation(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        if self.limit_on_completion {
            return Err(crabber_session::StoreError::AdmissionLimit(
                crabber_core::AdmissionLimit::proof_bytes(Some(33_554_432)),
            )
            .into());
        }
        self.inner.complete_preparation(joined).await
    }
    async fn mutate_session_owner(
        &self,
        request: crabber_session::session_owner::MutateSessionOwner,
        authority: Option<crabber_session::session_owner::SessionOwnerAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let lose_ack = matches!(
            &request.command,
            crabber_session::session_owner::SessionOwnerCommand::Join {
                termination: crabber_session::session_owner::SessionOwnerTermination::Cancelled
            }
        );
        let result = self.inner.mutate_session_owner(request, authority).await?;
        if lose_ack
            && self
                .lose_cleanup_ack
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn get_session_owner(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::SessionOwnerId,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerSnapshot,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.get_session_owner(session, id).await
    }
    async fn lookup_session_owner(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crabber_session::session_owner::SessionOwnerReceipt>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_session_owner(session, id).await
    }
    fn supports_owned_attempts(&self) -> bool {
        self.inner.supports_owned_attempts()
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.inner.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.inner.supports_pre_effect_approvals()
    }
    async fn session_revision(
        &self,
        session: &crabber_core::SessionId,
    ) -> Result<crabber_core::SessionRevision, crabber_session::StoreError> {
        self.inner.session_revision(session).await
    }
    async fn admit_run(
        &self,
        request: crabber_session::AdmitRequest,
    ) -> Result<crabber_session::AdmitOutcome, crabber_session::StoreError> {
        self.inner.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crabber_session::ExecutionStore>, crabber_session::StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crabber_session::StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(
        &self,
        id: &crabber_core::RunId,
    ) -> Result<Option<crabber_core::Run>, crabber_session::StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crabber_session::StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crabber_session::StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crabber_session::StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(
        &self,
    ) -> Result<Vec<crabber_core::Run>, crabber_session::StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &crabber_core::RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crabber_session::StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &crabber_core::RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crabber_session::StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crabber_session::StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crabber_session::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crabber_session::StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
}
