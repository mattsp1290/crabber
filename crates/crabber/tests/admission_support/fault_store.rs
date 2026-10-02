use super::*;

pub(super) struct FaultStore {
    pub(super) inner: Arc<PostgresStore>,
    pub(super) mode: String,
    pub(super) dir: PathBuf,
}
#[async_trait]
impl Store for FaultStore {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        mut request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        if self.mode == "precommit" {
            return Err(StoreError::Validation("injected before commit".into()));
        }
        if self.mode == "delayed" {
            fs::write(self.dir.join("waiting"), "ready").unwrap();
            wait(&self.dir.join("release")).await;
        }
        if self.mode == "commit-loss" && !self.dir.join("text-only").exists() {
            // The host never sees the successful Store outcome or its fence.
            request.request.lease = Duration::from_millis(20);
        }
        if self.dir.join("short-admission-lease").exists() {
            request.request.lease = Duration::from_secs(1);
        }
        let outcome = self.inner.admit_keyed_run(request).await?;
        let receipt = match &outcome {
            KeyedAdmitOutcome::Started { receipt, .. } | KeyedAdmitOutcome::Replayed(receipt) => {
                receipt
            }
        };
        if let KeyedAdmitOutcome::Started { .. } = &outcome {
            fs::write(
                self.dir.join("committed.json"),
                serde_json::to_vec(receipt).unwrap(),
            )
            .unwrap();
        }
        if self.mode == "commit-loss" {
            assert_eq!(
                self.inner
                    .lookup_admission(&receipt.session_id, &options().key)
                    .await?,
                Some(receipt.clone())
            );
            assert!(self.inner.get_run(&receipt.run_id).await?.is_some());
            let record = self
                .inner
                .load_admission_execution(&receipt.session_id, &options().key)
                .await
                .expect("execution evidence must commit atomically")
                .unwrap();
            assert_eq!(record.receipt, *receipt);
            assert_eq!(
                record.state,
                crabber_session::AdmissionExecutionState::Unstarted
            );
            // Let this abandoned owner's positive lease expire before fresh recovery.
            if !self.dir.join("text-only").exists() {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
            // Same sanitized error category as a real PostgreSQL commit acknowledgement loss.
            return Err(StoreError::Validation("PostgreSQL operation failed".into()));
        }
        Ok(outcome)
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn claim_unstarted_admission(
        &self,
        mut request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<crabber_session::ClaimedAdmission, crabber_session::AdmissionExecutionError> {
        if self.mode == "unstarted-claim-loss"
            || self.mode == "unstarted-before-begin"
            || self.mode == "unstarted-begin-fail"
            || self.mode == "unstarted-begin-loss"
            || self.mode == "unstarted-after-begin"
        {
            request.lease = Duration::from_secs(3);
        }
        let outcome = self.inner.claim_unstarted_admission(request).await?;
        if self.mode == "unstarted-claim-loss" {
            return Err(crabber_session::AdmissionExecutionError::UnknownStoreFailure);
        }
        if self.mode == "unstarted-before-begin" {
            fs::write(self.dir.join("claimed"), "ready").unwrap();
            wait(&self.dir.join("claim-release")).await;
        }
        Ok(outcome)
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        Ok(Box::new(FaultExecution {
            inner: self.inner.execution(fence).await?,
            mode: self.mode.clone(),
            dir: self.dir.clone(),
        }))
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
    ) -> Result<Option<crabber_session::AdmissionExecutionState>, StoreError> {
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

struct FaultExecution {
    inner: Box<dyn ExecutionStore>,
    mode: String,
    dir: PathBuf,
}
#[async_trait]
impl ExecutionStore for FaultExecution {
    async fn renew_lease(&self, until: time::OffsetDateTime) -> Result<(), StoreError> {
        self.inner.renew_lease(until).await
    }
    async fn begin_admission_execution(
        &self,
    ) -> Result<(), crabber_session::AdmissionExecutionError> {
        use crabber_session::AdmissionExecutionError;
        if self.mode == "unstarted-begin-fail" {
            return Err(AdmissionExecutionError::UnknownStoreFailure);
        }
        self.inner.begin_admission_execution().await?;
        if self.mode == "unstarted-begin-loss" {
            return Err(AdmissionExecutionError::UnknownStoreFailure);
        }
        if self.mode.starts_with("unstarted-race-") || self.mode == "unstarted-after-begin" {
            fs::write(self.dir.join("begun"), "ready").unwrap();
            wait(&self.dir.join("begin-release")).await;
        }
        Ok(())
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        self.inner.append_event(event).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.create_tool_call(call, pending_event).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner
            .settle_tool_call(id, result, result_message, terminal_event)
            .await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.inner.start_epoch(epoch).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.inner.finish_epoch(id, summary).await
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.pause_run(checkpoint, event).await
    }
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.settle_run(status, error, usage, event).await
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        self.inner.put_extension_state(extension_id, entries).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox(kind).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox_into_history(kind).await
    }
}
