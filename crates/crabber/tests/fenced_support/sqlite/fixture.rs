use super::*;
use sqlx::{Column, Row, TypeInfo, ValueRef};
use std::collections::BTreeMap;

pub(super) struct SqliteFixture {
    pub store: SqliteStore,
    pub raw: SqlitePool,
}

impl SqliteFixture {
    pub async fn connect(path: &Path) -> Self {
        Self {
            store: SqliteStore::connect(path).await.unwrap(),
            raw: SqlitePool::connect_with(
                SqliteConnectOptions::new()
                    .filename(path)
                    .foreign_keys(true),
            )
            .await
            .unwrap(),
        }
    }
}
#[async_trait]
impl Store for SqliteFixture {
    fn supports_preparation_protocol(&self) -> bool {
        self.store.supports_preparation_protocol()
    }
    async fn reserve_preparation(
        &self,
        request: crabber_session::session_owner::preparation::ReservePreparation,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.store.reserve_preparation(request).await
    }
    async fn mutate_session_owner(
        &self,
        request: crabber_session::session_owner::MutateSessionOwner,
        authority: Option<crabber_session::session_owner::SessionOwnerAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.store.mutate_session_owner(request, authority).await
    }
    async fn lookup_preparation(
        &self,
        session: &crabber_core::SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.store.lookup_preparation(session, key).await
    }
    async fn complete_preparation(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        let completed = self.store.complete_preparation(joined).await?;

        Ok(completed)
    }

    fn supports_owned_attempts(&self) -> bool {
        self.store.supports_owned_attempts()
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.store.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.store.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber::core::RunId,
        id: &crabber::core::PhaseId,
    ) -> Result<crabber::session::VersionedPhase, crabber::session::PhaseError> {
        self.store.tool_phase(run, id).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber::session::PhaseResolution,
    ) -> Result<crabber::session::PhaseResolutionOutcome, crabber::session::PhaseError> {
        self.store.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::PhaseReceipt>, crabber::session::PhaseError> {
        self.store.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber::core::SessionId,
        run: &crabber::core::RunId,
    ) -> Result<Option<crabber::session::ApprovalSnapshot>, crabber::session::ApprovalError> {
        self.store.approval_snapshot(session, run).await
    }
    async fn original_approval_plan(
        &self,
        selected: &crabber::core::SessionId,
        run: &crabber::core::RunId,
        target: &crabber::session::ContinuationTarget,
    ) -> Result<crabber::core::InputFingerprint, crabber::session::ApprovalError> {
        self.store
            .original_approval_plan(selected, run, target)
            .await
    }
    async fn lookup_approval(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::ApprovalReceipt>, crabber::session::ApprovalError> {
        self.store.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber::session::ApprovalAnswer,
    ) -> Result<crabber::session::ApprovalReceipt, crabber::session::ApprovalError> {
        self.store.answer_approval(answer).await
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crabber_session::ApprovalContinuationRecovery,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        self.store.reclaim_approval_continuation(request).await
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber::session::ApprovalContinuationClaim,
    ) -> Result<crabber::session::ApprovalClaimOutcome, crabber::session::ApprovalError> {
        self.store.claim_approval_continuation(claim).await
    }

    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::VersionedAttempt>, crabber_session::AttemptError> {
        self.store.latest_execution_attempt(run).await
    }

    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.store.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.store.admit_keyed_run(request).await
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.store.lookup_admission(session, key).await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<
        Option<crabber::session::AdmissionExecutionRecord>,
        crabber::session::AdmissionExecutionError,
    > {
        self.store.load_admission_execution(session, key).await
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber::session::AdmissionExecutionState>, StoreError> {
        self.store.admission_execution_state(run).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crabber::session::ClaimUnstartedAdmissionRequest,
    ) -> Result<crabber::session::ClaimedAdmission, crabber::session::AdmissionExecutionError> {
        self.store.claim_unstarted_admission(request).await
    }
    async fn abandon_run(&self, request: AbandonRequest) -> Result<AbandonOutcome, AbandonError> {
        self.store.abandon_run(request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.store.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.store.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.store.get_run(id).await
    }
    async fn snapshot(
        &self,
        request: crabber::session::SnapshotRequest,
    ) -> Result<crabber::session::SnapshotOutcome, StoreError> {
        self.store.snapshot(request).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.store.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.store.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.store.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.store.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.store.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.store.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.store.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.store.enqueue_inbox(session, kind, message).await
    }
}
#[async_trait]
impl abandonment_contract::FixtureStore for SqliteFixture {
    async fn seed_run(&self, run: Run) {
        let mut tx = self.raw.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let previous: String = sqlx::query_scalar("SELECT data FROM runs WHERE id=$1")
            .bind(&run.id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let previous: Run = serde_json::from_str(&previous).unwrap();
        let latest:Option<String>=sqlx::query_scalar("SELECT data_json FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 ORDER BY sequence DESC LIMIT 1").bind(&run.session_id.0).bind(&run.id.0).fetch_optional(&mut *tx).await.unwrap();
        if let Some(issuance) = crate::run_journal::advance(&previous, &run, latest.as_deref()) {
            sqlx::query("INSERT INTO native_run_issuances(session_id,run_id,sequence,data_json) VALUES($1,$2,$3,$4)").bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(issuance.sequence).unwrap()).bind(serde_json::to_string(&issuance).unwrap()).execute(&mut *tx).await.unwrap();
        }
        sqlx::query("UPDATE runs SET status=$2,data=$3,lease_until=$4,claim_token=$5 WHERE id=$1")
            .bind(&run.id.0)
            .bind(match run.status {
                RunStatus::Pending => "pending",
                RunStatus::Running => "running",
                RunStatus::Paused => "paused",
                _ => panic!(),
            })
            .bind(serde_json::to_string(&run).unwrap())
            .bind(i64::try_from(run.lease_until.unix_timestamp_nanos() / 1_000).unwrap())
            .bind(&run.claim_token)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord> {
        sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id")
            .bind(&run.0)
            .fetch_all(&self.raw)
            .await
            .unwrap()
            .into_iter()
            .map(|row| serde_json::from_str(row.get("data")).unwrap())
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL",
        )
        .bind(&session.0)
        .fetch_one(&self.raw)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    }
}

// Select * deliberately: rollback evidence includes accounting and fencing columns,
// not merely the canonical domain record. Integer and text types cover this schema.
fn dump_row(row: &sqlx::sqlite::SqliteRow) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for column in row.columns() {
        let raw = row.try_get_raw(column.ordinal()).unwrap();
        let value = if raw.is_null() {
            serde_json::Value::Null
        } else {
            match raw.type_info().name() {
                "INTEGER" => serde_json::Value::from(row.get::<i64, _>(column.ordinal())),
                "TEXT" => serde_json::Value::from(row.get::<String, _>(column.ordinal())),
                kind => panic!("unexpected persisted column type: {kind}"),
            }
        };
        object.insert(column.name().into(), value);
    }
    serde_json::Value::Object(object)
}
pub(super) async fn snapshot(pool: &SqlitePool, run: &Run) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for (table, filter, key) in [
        ("runs", "id", "seq"),
        ("tool_calls", "run_id", "seq"),
        ("messages", "run_id", "seq"),
        ("events", "run_id", "seq"),
        ("abandonment_commits", "run_id", "run_id"),
    ] {
        let rows = sqlx::query(&format!(
            "SELECT * FROM {table} WHERE {filter}=$1 ORDER BY {key}"
        ))
        .bind(&run.id.0)
        .fetch_all(pool)
        .await
        .unwrap();
        result.insert(
            table.into(),
            serde_json::Value::Array(rows.iter().map(dump_row).collect()),
        );
    }
    let rows=sqlx::query("SELECT p.* FROM parts p JOIN messages m ON m.id=p.message_id WHERE m.run_id=$1 ORDER BY p.id")
        .bind(&run.id.0).fetch_all(pool).await.unwrap();
    result.insert(
        "parts".into(),
        serde_json::Value::Array(rows.iter().map(dump_row).collect()),
    );
    serde_json::Value::Object(result)
}
