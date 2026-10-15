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
    fn supports_owned_attempts(&self) -> bool {
        self.store.supports_owned_attempts()
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
        sqlx::query("UPDATE runs SET status=$2,data=$3,lease_until=$4 WHERE id=$1")
            .bind(&run.id.0)
            .bind(match run.status {
                RunStatus::Pending => "pending",
                RunStatus::Running => "running",
                RunStatus::Paused => "paused",
                _ => panic!(),
            })
            .bind(serde_json::to_string(&run).unwrap())
            .bind(i64::try_from(run.lease_until.unix_timestamp_nanos() / 1_000).unwrap())
            .execute(&self.raw)
            .await
            .unwrap();
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
