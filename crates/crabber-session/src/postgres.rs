//! PostgreSQL 14+ store. JSONB preserves the domain records while relational keys,
//! ordering columns, and the active-run index enforce ownership across processes.
use crate::{AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, Store, StoreError};
use async_trait::async_trait;
use crabber_core::{
    ByteLimits, Clock, ContextEpoch, EpochId, EventCursor, EventRecord, Message, MessageId, Part,
    PartKind, Run, RunFence, RunId, RunStatus, Session, SessionId, SystemClock, ToolCallId,
    ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus, Usage,
};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions, types::Json};
use std::{collections::BTreeMap, sync::Arc};
use time::OffsetDateTime;

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    clock: Arc<dyn Clock>,
    limits: ByteLimits,
}
struct PostgresExecution {
    store: PostgresStore,
    fence: RunFence,
}

#[allow(clippy::needless_pass_by_value)] // Required by map_err.
fn db(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(ref detail) = error {
        if detail.code().as_deref() == Some("23505") {
            return StoreError::Conflict;
        }
        if detail.code().as_deref() == Some("23503") {
            return StoreError::NotFound;
        }
    }
    // Connection URLs and database details may contain credentials.
    StoreError::Validation("PostgreSQL operation failed".into())
}
fn json<T: Serialize>(value: &T) -> Result<Json<serde_json::Value>, StoreError> {
    serde_json::to_value(value)
        .map(Json)
        .map_err(|_| StoreError::Validation("record encoding failed".into()))
}
fn decode<T: DeserializeOwned>(value: Json<serde_json::Value>) -> Result<T, StoreError> {
    serde_json::from_value(value.0)
        .map_err(|_| StoreError::Validation("stored record is invalid".into()))
}
fn micros(time: OffsetDateTime) -> i64 {
    i64::try_from(time.unix_timestamp_nanos() / 1_000).unwrap_or(i64::MAX)
}
fn inbox_kind(kind: InboxKind) -> &'static str {
    match kind {
        InboxKind::Steer => "steer",
        InboxKind::FollowUp => "follow_up",
    }
}
fn status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Running => "running",
        RunStatus::Paused => "paused",
        RunStatus::Interrupted => "interrupted",
        RunStatus::Failed => "failed",
        RunStatus::Completed => "completed",
    }
}
fn call_status(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::Running => "running",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        ToolCallStatus::Interrupted => "interrupted",
    }
}
async fn save_run(tx: &mut Transaction<'_, Postgres>, run: &Run) -> Result<(), StoreError> {
    sqlx::query("UPDATE runs SET status=$2, claim_token=$3, lease_until=$4, data=$5 WHERE id=$1")
        .bind(&run.id.0)
        .bind(status(run.status))
        .bind(&run.claim_token)
        .bind(micros(run.lease_until))
        .bind(json(run)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
async fn load_run(
    tx: &mut Transaction<'_, Postgres>,
    id: &RunId,
    lock: bool,
) -> Result<Run, StoreError> {
    let query = if lock {
        "SELECT data FROM runs WHERE id=$1 FOR UPDATE"
    } else {
        "SELECT data FROM runs WHERE id=$1"
    };
    let row = sqlx::query(query)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
    decode(row.get("data"))
}
async fn insert_message(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    message: &Message,
) -> Result<(), StoreError> {
    if message.session_id != run.session_id || message.run_id.as_ref() != Some(&run.id) {
        return Err(StoreError::Validation(
            "message belongs to another run".into(),
        ));
    }
    if message
        .parts
        .iter()
        .any(|part| part.message_id != message.id)
    {
        return Err(StoreError::Validation(
            "part belongs to another message".into(),
        ));
    }
    sqlx::query("INSERT INTO messages(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
        .bind(&message.id.0)
        .bind(&message.session_id.0)
        .bind(&run.id.0)
        .bind(json(message)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    for part in &message.parts {
        insert_part(tx, part).await?;
    }
    Ok(())
}
async fn insert_part(tx: &mut Transaction<'_, Postgres>, part: &Part) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO parts(id,message_id,ordinal,data) VALUES($1,$2,$3,$4)")
        .bind(&part.id.0)
        .bind(&part.message_id.0)
        .bind(i64::from(part.ordinal))
        .bind(json(part)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    event: &EventRecord,
) -> Result<(), StoreError> {
    if event.live_only || event.kind.is_live_only() {
        return Err(StoreError::Validation(
            "live-only events cannot be persisted".into(),
        ));
    }
    if event.session_id != run.session_id || event.run_id != run.id {
        return Err(StoreError::Validation(
            "event belongs to another run".into(),
        ));
    }
    sqlx::query("INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3)")
        .bind(&run.session_id.0)
        .bind(&run.id.0)
        .bind(json(event)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
async fn messages(pool: &PgPool, id: &SessionId) -> Result<Vec<Message>, StoreError> {
    let rows = sqlx::query("SELECT data FROM messages WHERE session_id=$1 ORDER BY seq")
        .bind(&id.0)
        .fetch_all(pool)
        .await
        .map_err(db)?;
    rows.into_iter()
        .map(|row| decode(row.get("data")))
        .collect()
}
async fn project(
    pool: &PgPool,
    id: &SessionId,
    epoch: Option<EpochId>,
) -> Result<Vec<Message>, StoreError> {
    let exists = sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
        .bind(&id.0)
        .fetch_optional(pool)
        .await
        .map_err(db)?;
    if exists.is_none() {
        return Err(StoreError::NotFound);
    }
    let epoch_id = if let Some(epoch) = epoch {
        Some(epoch)
    } else {
        let row =
            sqlx::query("SELECT data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1")
                .bind(&id.0)
                .fetch_optional(pool)
                .await
                .map_err(db)?;
        row.map(|row| decode::<Run>(row.get("data")).map(|run| run.epoch_id))
            .transpose()?
    };
    let selected = if let Some(epoch_id) = epoch_id {
        let row = sqlx::query("SELECT data FROM epochs WHERE id=$1")
            .bind(&epoch_id.0)
            .fetch_optional(pool)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let value: ContextEpoch = decode(row.get("data"))?;
        if value.session_id != *id {
            return Err(StoreError::Validation(
                "epoch belongs to another session".into(),
            ));
        }
        Some(value)
    } else {
        None
    };
    let all = messages(pool, id).await?;
    let selected = if let Some(epoch) = selected.filter(|epoch| epoch.summary_message_id.is_some())
    {
        let summary_id = epoch.summary_message_id.as_ref().expect("checked");
        let summary = all
            .iter()
            .find(|message| &message.id == summary_id)
            .ok_or(StoreError::NotFound)?
            .clone();
        let mut selected = vec![summary];
        if let Some(tail) = epoch.tail_start_message_id {
            let index = all
                .iter()
                .position(|message| message.id == tail)
                .ok_or(StoreError::NotFound)?;
            selected.extend(
                all[index..]
                    .iter()
                    .filter(|message| &message.id != summary_id)
                    .cloned(),
            );
        }
        selected
    } else {
        all
    };
    Ok(selected
        .into_iter()
        .filter_map(|mut message| {
            message
                .parts
                .retain(|part| !matches!(part.kind, PartKind::Custom { .. }));
            (!message.parts.is_empty()).then_some(message)
        })
        .collect())
}

impl PostgresStore {
    /// Opens an already migrated dedicated database. The URL is never included in errors.
    /// # Errors
    /// Returns an error when connection or schema verification fails.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(db)?;
        let version: i32 =
            sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version DESC LIMIT 1")
                .fetch_optional(&pool)
                .await
                .map_err(db)?
                .ok_or(StoreError::Validation(
                    "unsupported PostgreSQL schema version".into(),
                ))?;
        if version != 1 {
            return Err(StoreError::Validation(
                "unsupported PostgreSQL schema version".into(),
            ));
        }
        Ok(Self {
            pool,
            clock: Arc::new(SystemClock),
            limits: ByteLimits::default(),
        })
    }
    /// Applies the bundled migrations to a dedicated PostgreSQL 14+ database.
    /// # Errors
    /// Returns a sanitized error on connection or migration failure.
    pub async fn migrate(url: &str) -> Result<(), StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .map_err(db)?;
        let mut tx = pool.begin().await.map_err(db)?;
        sqlx::query("SELECT pg_advisory_xact_lock(751302919)")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        for statement in include_str!("../migrations/0001_initial.sql")
            .split(';')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            sqlx::query(statement).execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }
    /// Replaces the clock, primarily for deterministic lease tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    #[must_use]
    pub fn with_limits(mut self, limits: ByteLimits) -> Self {
        self.limits = limits;
        self
    }
    async fn fenced(
        &self,
        fence: &RunFence,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let run = load_run(&mut tx, &fence.run_id, true).await?;
        if run.claim_token != fence.claim_token
            || run.status.is_terminal()
            || run.lease_until <= self.clock.now()
        {
            return Err(StoreError::Conflict);
        }
        Ok((tx, run))
    }
}

#[allow(clippy::too_many_lines)] // Transactional trait methods stay together.
#[async_trait]
impl Store for PostgresStore {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        let now = self.clock.now();
        let lease = time::Duration::try_from(request.lease)
            .map_err(|_| StoreError::Validation("lease is too large".into()))?;
        if lease <= time::Duration::ZERO {
            return Err(StoreError::Validation("lease must be positive".into()));
        }
        let session_id = request
            .session_id
            .clone()
            .unwrap_or_else(|| request.user_message.session_id.clone());
        if request.user_message.session_id != session_id {
            return Err(StoreError::Validation(
                "user message has wrong session".into(),
            ));
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        // Serialize admission for one session, including first admission. Other sessions proceed independently.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(&session_id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let existing = sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&session_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let had_session = existing.is_some();
        let session: Session = if let Some(row) = existing {
            decode(row.get("data"))?
        } else if request.session_id.is_some() {
            return Err(StoreError::NotFound);
        } else {
            Session {
                id: session_id.clone(),
                workspace_id: request.workspace_id.clone(),
                directory: request.directory.clone(),
                title: request.title.clone(),
                created_at: now,
                updated_at: now,
            }
        };
        let busy = sqlx::query(
            "SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused')",
        )
        .bind(&session_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .is_some();
        if busy {
            return Err(StoreError::Busy);
        }
        let prior_history = if had_session {
            project(&self.pool, &session_id, None).await?
        } else {
            Vec::new()
        };
        let previous = sqlx::query("SELECT e.data FROM runs r JOIN epochs e ON e.id = r.data->>'epoch_id' WHERE r.session_id=$1 ORDER BY r.seq DESC LIMIT 1")
            .bind(&session_id.0).fetch_optional(&mut *tx).await.map_err(db)?;
        let previous: Option<ContextEpoch> =
            previous.map(|row| decode(row.get("data"))).transpose()?;
        let run_id = RunId::new();
        let epoch_id = EpochId::new();
        let token = uuid::Uuid::new_v4().to_string();
        let run = Run {
            id: run_id.clone(),
            session_id: session_id.clone(),
            status: RunStatus::Running,
            owner: request.owner,
            claim_token: token.clone(),
            lease_until: now + lease,
            epoch_id: epoch_id.clone(),
            config_hash: request.config_hash,
            plan_fingerprint: request.plan_fingerprint,
            checkpoint: None,
            error: None,
            usage: Usage::default(),
            created_at: now,
            updated_at: now,
        };
        let epoch = ContextEpoch {
            id: epoch_id.clone(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent: previous.as_ref().map(|value| value.id.clone()),
            summarized_range: previous
                .as_ref()
                .and_then(|value| value.summarized_range.clone()),
            summary_message_id: previous
                .as_ref()
                .and_then(|value| value.summary_message_id.clone()),
            tail_start_message_id: previous
                .as_ref()
                .and_then(|value| value.tail_start_message_id.clone()),
            provider_id: previous
                .as_ref()
                .map_or_else(String::new, |value| value.provider_id.clone()),
            model_id: previous
                .as_ref()
                .map_or_else(String::new, |value| value.model_id.clone()),
            reason: "initial".into(),
            next_policy: previous.and_then(|value| value.next_policy),
        };
        if !had_session {
            sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
                .bind(&session.id.0)
                .bind(json(&session)?)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(&run.id.0).bind(&session_id.0).bind(status(run.status)).bind(&token).bind(micros(run.lease_until)).bind(json(&run)?).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&epoch.id.0)
            .bind(&session_id.0)
            .bind(&run.id.0)
            .bind(json(&epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let mut user = request.user_message;
        user.run_id = Some(run_id.clone());
        insert_message(&mut tx, &run, &user).await?;
        tx.commit().await.map_err(db)?;
        Ok(AdmitOutcome {
            session,
            run,
            fence: RunFence {
                run_id,
                claim_token: token,
            },
            assistant_placeholder: MessageId::new(),
            epoch: epoch_id,
            prior_history,
        })
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        let (tx, _) = self.fenced(&fence).await?;
        tx.rollback().await.map_err(db)?;
        Ok(Box::new(PostgresExecution {
            store: self.clone(),
            fence,
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        sqlx::query("SELECT data FROM sessions WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .map(|row| decode(row.get("data")))
            .transpose()
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        sqlx::query("SELECT data FROM runs WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?
            .map(|row| decode(row.get("data")))
            .transpose()
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        project(&self.pool, id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        messages(&self.pool, id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let rows = sqlx::query(
            "SELECT seq,data FROM events WHERE session_id=$1 AND seq>$2 ORDER BY seq LIMIT $3",
        )
        .bind(&id.0)
        .bind(after.map_or(0, |value| i64::try_from(value.0).unwrap_or(i64::MAX)))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let mut event: EventRecord = decode(row.get("data"))?;
                event.cursor = Some(EventCursor(
                    u64::try_from(row.get::<i64, _>("seq"))
                        .map_err(|_| StoreError::Validation("invalid event cursor".into()))?,
                ));
                Ok(event)
            })
            .collect()
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        sqlx::query(
            "SELECT data FROM runs WHERE status IN ('pending','running','paused') ORDER BY seq",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db)?
        .into_iter()
        .map(|row| decode(row.get("data")))
        .collect()
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running') ORDER BY id")
            .bind(&run.0).fetch_all(&self.pool).await.map_err(db)?.into_iter().map(|row| decode(row.get("data"))).collect()
    }
    async fn claim_expired_run(&self, id: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        let mut run = load_run(&mut tx, id, true).await?;
        let now = self.clock.now();
        if run.status.is_terminal() || run.lease_until > now {
            return Err(StoreError::Conflict);
        }
        run.owner = owner.into();
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now + time::Duration::seconds(30);
        run.updated_at = now;
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)?;
        Ok(RunFence {
            run_id: id.clone(),
            claim_token: run.claim_token,
        })
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let rows = sqlx::query(
            "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
        )
        .bind(&session.0)
        .bind(extension_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("key"), row.get("value")))
            .collect())
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        if message.session_id != *session {
            return Err(StoreError::Validation(
                "inbox message has wrong session".into(),
            ));
        }
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query("SELECT 1 FROM sessions WHERE id=$1 FOR UPDATE")
            .bind(&session.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        sqlx::query("INSERT INTO inbox(session_id,kind,data) VALUES($1,$2,$3)")
            .bind(&session.0)
            .bind(inbox_kind(kind))
            .bind(json(&message)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
}

#[async_trait]
impl ExecutionStore for PostgresExecution {
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        let now = self.store.clock.now();
        if until <= now {
            return Err(StoreError::Validation(
                "lease must extend into the future".into(),
            ));
        }
        run.lease_until = until;
        run.updated_at = now;
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        insert_message(&mut tx, &run, &message).await?;
        tx.commit().await.map_err(db)
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query("SELECT data FROM messages WHERE id=$1 FOR UPDATE")
            .bind(&part.message_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let mut message: Message = decode(row.get("data"))?;
        if message.session_id != run.session_id || message.run_id.as_ref() != Some(&run.id) {
            return Err(StoreError::Validation("part belongs to another run".into()));
        }
        if message
            .parts
            .iter()
            .any(|old| old.id == part.id || old.ordinal == part.ordinal)
        {
            return Err(StoreError::Conflict);
        }
        insert_part(&mut tx, &part).await?;
        message.parts.push(part);
        message.parts.sort_by_key(|part| part.ordinal);
        sqlx::query("UPDATE messages SET data=$2 WHERE id=$1")
            .bind(&message.id.0)
            .bind(json(&message)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        insert_event(&mut tx, &run, &event).await?;
        tx.commit().await.map_err(db)
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        if call.run_id != run.id || call.status != ToolCallStatus::Pending {
            return Err(StoreError::Validation("invalid new tool call".into()));
        }
        insert_event(&mut tx, &run, &pending_event).await?;
        sqlx::query("INSERT INTO tool_calls(id,run_id,status,data) VALUES($1,$2,$3,$4)")
            .bind(&call.id.0)
            .bind(&run.id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query("SELECT data FROM tool_calls WHERE id=$1 FOR UPDATE")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let mut call: ToolCallRecord = decode(row.get("data"))?;
        if call.run_id != run.id || call.status != ToolCallStatus::Pending {
            return Err(StoreError::Conflict);
        }
        insert_event(&mut tx, &run, &running_event).await?;
        call.status = ToolCallStatus::Running;
        sqlx::query("UPDATE tool_calls SET status=$2,data=$3 WHERE id=$1")
            .bind(&id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query("SELECT data FROM tool_calls WHERE id=$1 FOR UPDATE")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let mut call: ToolCallRecord = decode(row.get("data"))?;
        if call.run_id != run.id || call.status != ToolCallStatus::Running {
            return Err(StoreError::Conflict);
        }
        if !result_message.parts.iter().any(|part| matches!(&part.content, crabber_core::ContentBlock::ToolResult { call_id, .. } if call_id == id)) { return Err(StoreError::Validation("tool result message lacks matching result part".into())); }
        insert_message(&mut tx, &run, &result_message).await?;
        insert_event(&mut tx, &run, &terminal_event).await?;
        call.status = match result.status {
            ToolResultStatus::Completed => ToolCallStatus::Completed,
            ToolResultStatus::Failed => ToolCallStatus::Failed,
            ToolResultStatus::Interrupted => ToolCallStatus::Interrupted,
        };
        call.result = Some(result);
        sqlx::query("UPDATE tool_calls SET status=$2,data=$3 WHERE id=$1")
            .bind(&id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        if epoch.run_id != run.id
            || epoch.session_id != run.session_id
            || epoch.parent.as_ref() != Some(&run.epoch_id)
        {
            return Err(StoreError::Validation("invalid context epoch".into()));
        }
        sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&epoch.id.0)
            .bind(&epoch.session_id.0)
            .bind(&run.id.0)
            .bind(json(&epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        run.epoch_id = epoch.id;
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let row = sqlx::query("SELECT data FROM epochs WHERE id=$1 FOR UPDATE")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let mut epoch: ContextEpoch = decode(row.get("data"))?;
        if epoch.run_id != run.id || epoch.summary_message_id.is_some() || run.epoch_id != *id {
            return Err(StoreError::Conflict);
        }
        if !summary
            .parts
            .iter()
            .any(|part| part.kind == PartKind::CompactionSummary)
        {
            return Err(StoreError::Validation(
                "summary message lacks compaction part".into(),
            ));
        }
        insert_message(&mut tx, &run, &summary).await?;
        epoch.summary_message_id = Some(summary.id);
        sqlx::query("UPDATE epochs SET data=$2 WHERE id=$1")
            .bind(&id.0)
            .bind(json(&epoch)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        insert_event(&mut tx, &run, &event).await?;
        let now = self.store.clock.now();
        run.status = RunStatus::Paused;
        run.checkpoint = Some(checkpoint);
        run.lease_until = now;
        run.updated_at = now;
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn settle_run(
        &self,
        status_value: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let (mut tx, mut run) = self.store.fenced(&self.fence).await?;
        if !status_value.is_terminal() {
            return Err(StoreError::Validation(
                "run settlement must be terminal".into(),
            ));
        }
        if status_value == RunStatus::Completed {
            sqlx::query("SELECT 1 FROM sessions WHERE id=$1 FOR UPDATE")
                .bind(&run.session_id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            let pending = sqlx::query(
                "SELECT 1 FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL LIMIT 1",
            )
            .bind(&run.session_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .is_some();
            if pending {
                return Err(StoreError::PendingInput);
            }
        }
        insert_event(&mut tx, &run, &event).await?;
        run.status = status_value;
        run.error = error;
        run.usage = usage;
        run.updated_at = self.store.clock.now();
        save_run(&mut tx, &run).await?;
        tx.commit().await.map_err(db)
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        for (key, value) in entries {
            if let Some(value) = value {
                sqlx::query("INSERT INTO extension_state(session_id,extension_id,key,value) VALUES($1,$2,$3,$4) ON CONFLICT(session_id,extension_id,key) DO UPDATE SET value=excluded.value")
                    .bind(&run.session_id.0).bind(extension_id).bind(&key).bind(&value).execute(&mut *tx).await.map_err(db)?;
            } else {
                sqlx::query("DELETE FROM extension_state WHERE session_id=$1 AND extension_id=$2 AND key=$3")
                    .bind(&run.session_id.0).bind(extension_id).bind(&key).execute(&mut *tx).await.map_err(db)?;
            }
        }
        let rows = sqlx::query(
            "SELECT key,value FROM extension_state WHERE session_id=$1 AND extension_id=$2",
        )
        .bind(&run.session_id.0)
        .bind(extension_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        if rows.len() > self.store.limits.max_state_entries
            || rows
                .iter()
                .map(|row| row.get::<String, _>("key").len() + row.get::<String, _>("value").len())
                .sum::<usize>()
                > self.store.limits.max_state_bytes
        {
            return Err(StoreError::Limit("extension state".into()));
        }
        tx.commit().await.map_err(db)
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, false).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.claim(kind, true).await
    }
}
impl PostgresExecution {
    async fn claim(&self, kind: InboxKind, into_history: bool) -> Result<Vec<Message>, StoreError> {
        let (mut tx, run) = self.store.fenced(&self.fence).await?;
        let rows = sqlx::query("SELECT seq,data FROM inbox WHERE session_id=$1 AND kind=$2 AND consumed_by_run IS NULL ORDER BY seq FOR UPDATE")
            .bind(&run.session_id.0).bind(inbox_kind(kind)).fetch_all(&mut *tx).await.map_err(db)?;
        let mut claimed = Vec::with_capacity(rows.len());
        for row in rows {
            let mut message: Message = decode(row.get("data"))?;
            if into_history {
                message.run_id = Some(run.id.clone());
                insert_message(&mut tx, &run, &message).await?;
            }
            sqlx::query("UPDATE inbox SET consumed_by_run=$2 WHERE seq=$1")
                .bind(row.get::<i64, _>("seq"))
                .bind(&run.id.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            claimed.push(message);
        }
        tx.commit().await.map_err(db)?;
        Ok(claimed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_core::{ContentBlock, ManualClock, PartId, Role};
    use std::time::Duration;
    static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn test_url() -> Option<String> {
        match std::env::var("CRABBER_TEST_POSTGRES_URL") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent)
                if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1") =>
            {
                panic!("CRABBER_TEST_POSTGRES_URL is required")
            }
            Err(_error) => {
                eprintln!("skipping PostgreSQL tests: CRABBER_TEST_POSTGRES_URL is unset");
                None
            }
        }
    }
    fn input(session_id: &SessionId, text: &str) -> Message {
        let id = MessageId::new();
        Message {
            id: id.clone(),
            session_id: session_id.clone(),
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: id,
                ordinal: 0,
                kind: PartKind::UserInputText,
                content: ContentBlock::Text { text: text.into() },
            }],
            created_at: OffsetDateTime::now_utc(),
        }
    }
    fn request(session: &SessionId) -> AdmitRequest {
        AdmitRequest {
            session_id: None,
            workspace_id: "test".into(),
            directory: "/tmp".into(),
            title: "test".into(),
            user_message: input(session, "hello"),
            config_hash: "config".into(),
            plan_fingerprint: "plan".into(),
            owner: "test".into(),
            lease: Duration::from_secs(30),
        }
    }
    #[tokio::test]
    async fn postgres_contract() {
        let Some(url) = test_url() else { return };
        let _guard = TEST_LOCK.lock().await;
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        sqlx::query("TRUNCATE sessions CASCADE")
            .execute(&store.pool)
            .await
            .unwrap();
        crate::storetest::run_contract(|clock: Arc<ManualClock>| store.clone().with_clock(clock))
            .await;
    }
    #[tokio::test]
    async fn migrate_reopen_and_concurrent_sessions() {
        let Some(url) = test_url() else { return };
        let _guard = TEST_LOCK.lock().await;
        PostgresStore::migrate(&url).await.unwrap();
        let store = PostgresStore::connect(&url).await.unwrap();
        let before: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
                .fetch_all(&store.pool)
                .await
                .unwrap();
        PostgresStore::migrate(&url).await.unwrap();
        let after: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
                .fetch_all(&store.pool)
                .await
                .unwrap();
        assert_eq!(before, after);
        let first = SessionId::new();
        let second = SessionId::new();
        let (a, b) = tokio::join!(
            store.admit_run(request(&first)),
            store.admit_run(request(&second))
        );
        let a = a.unwrap();
        let b = b.unwrap();
        let (write_a, write_b) = tokio::join!(
            async {
                store
                    .execution(a.fence.clone())
                    .await
                    .unwrap()
                    .append_message({
                        let mut value = input(&first, "a");
                        value.run_id = Some(a.run.id.clone());
                        value
                    })
                    .await
            },
            async {
                store
                    .execution(b.fence.clone())
                    .await
                    .unwrap()
                    .append_message({
                        let mut value = input(&second, "b");
                        value.run_id = Some(b.run.id.clone());
                        value
                    })
                    .await
            }
        );
        write_a.unwrap();
        write_b.unwrap();
        let mut busy_request = request(&first);
        busy_request.session_id = Some(first.clone());
        assert_eq!(
            store.admit_run(busy_request).await.unwrap_err(),
            StoreError::Busy
        );
        let reopened = PostgresStore::connect(&url).await.unwrap();
        assert_eq!(
            store.list_messages(&first, None).await.unwrap(),
            reopened.list_messages(&first, None).await.unwrap()
        );
        assert_eq!(reopened.list_all_messages(&first).await.unwrap().len(), 2);
    }
}
