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
        if self.mode == "commit-loss" {
            // The host never sees the successful Store outcome or its fence.
            request.request.lease = Duration::from_millis(20);
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
            // Let this abandoned owner's positive lease expire before fresh recovery.
            tokio::time::sleep(Duration::from_millis(30)).await;
            // Same sanitized error category as a real PostgreSQL commit acknowledgement loss.
            return Err(StoreError::Validation("PostgreSQL operation failed".into()));
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
