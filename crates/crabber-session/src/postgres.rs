//! PostgreSQL 14+ store. JSONB preserves the domain records while relational keys,
//! ordering columns, and the active-run index enforce ownership across processes.
//! Snapshots return messages in append order, then tool calls in creation order.
//! Legacy calls migrated from v1/v2 use pending-event order when recorded, then
//! deterministic ID order for calls whose creation order was never persisted.
//! Migration backfills canonical records one at a time under exclusive locks;
//! connect remains read-only and requires explicit migration through
//! `crate::POSTGRES_SCHEMA_VERSION`.
#[path = "postgres/admission_reads.rs"]
mod admission_reads;
#[path = "postgres/continuation_handoff.rs"]
mod continuation_handoff;
#[path = "postgres/continuation_input.rs"]
mod continuation_input;
#[path = "postgres/credential_access.rs"]
mod credential_access;
#[path = "postgres/expired.rs"]
mod expired;
#[path = "postgres/read_proof.rs"]
mod read_proof;
#[path = "postgres/run_facts.rs"]
mod run_facts;

mod abandon;
mod abandon_history;
mod admission_execution;
mod admission_journal;
mod approval;
mod attempt;
#[cfg(test)]
mod begin_cancellation_tests;
mod connection;
mod deletion;
mod execution;
mod execution_binding;
mod extension_state;
mod inbox;
mod legacy_execution;
#[cfg(test)]
mod lock_order_tests;
mod management;
mod migration;
mod native_enrollment;
mod native_parent;
mod ownership_origin;
mod phase;
mod public_continuation_source;
mod public_header;
mod public_run;
mod public_state;
mod session_owner;
mod store_impl;
mod store_reads;
mod tool_result;
mod transaction;

mod fork;
mod frontend;
mod queue;
mod run_control;
mod tool_continuation;
#[cfg(test)]
use crate::Store;
use crate::postgres::transaction::Transaction;
use crate::{AdmitOutcome, AdmitRequest, InboxKind, StoreError};
use crabber_core::{
    AdmissionKey, AdmissionReceipt, ByteLimits, Clock, ContextEpoch, EpochId, EventCursor,
    EventRecord, Message, MessageId, Part, Run, RunFence, RunId, RunStatus, Session, SessionId,
    ToolCallRecord, ToolCallStatus, Usage,
};
#[cfg(test)]
use crabber_core::{PartKind, ToolCallId, ToolResult, ToolResultStatus};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{PgConnection, PgPool, Postgres, Row, types::Json};
use std::sync::Arc;
use time::OffsetDateTime;

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    clock: Arc<dyn Clock>,
    limits: ByteLimits,
    #[cfg(test)]
    abandon_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    public_state_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    management_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    attempt_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    native_raw_probe: Arc<crate::native_enrollment::caller::Probe>,
    #[cfg(test)]
    phase_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    approval_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    binding_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    queue_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    deletion_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    tool_continuation_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    control_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    session_owner_fault: Arc<std::sync::atomic::AtomicU8>,
}
struct PostgresExecution {
    store: PostgresStore,
    fence: RunFence,
}

#[allow(clippy::needless_pass_by_value)] // Required by map_err.
fn db(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(ref detail) = error {
        if detail.message() == "crabber_session_deleted" {
            return StoreError::SessionDeleted;
        }
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
    crate::storable::storable_value(value).map(Json)
}
fn decode<T: DeserializeOwned>(value: Json<serde_json::Value>) -> Result<T, StoreError> {
    serde_json::from_value(value.0)
        .map_err(|_| StoreError::Validation("stored record is invalid".into()))
}
fn decode_record<T: DeserializeOwned>(value: &str) -> Result<T, StoreError> {
    serde_json::from_str(value)
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
        ToolCallStatus::Waiting => "waiting",
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::Running => "running",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        ToolCallStatus::Interrupted => "interrupted",
    }
}
// Mutation order: session advisory gate -> run rows -> dependent rows ->
// events/revision. Every existing and new write path must enter through this
// gate, including lease renewal and keyed replay. No external I/O under locks.
async fn lock_session(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), StoreError> {
    lock_session_scope(tx, session).await?;
    deletion::ensure_live(tx, session).await
}
async fn lock_session_scope(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), StoreError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&session.0)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
async fn save_run(tx: &mut Transaction<'_, Postgres>, run: &Run) -> Result<(), StoreError> {
    save_run_with_completion(tx, run, None).await
}
async fn save_run_with_completion(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
) -> Result<(), StoreError> {
    save_run_with_proof(
        tx,
        run,
        source,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await
}
async fn save_run_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    native_parent::record_run_with_proof(tx, run, false, source, proof).await?;
    update_run_row(tx, run).await
}
async fn save_run_captured_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<u64>, StoreError> {
    save_run_publication_with_proof(
        tx,
        run,
        source,
        proof,
        crate::native_enrollment::caller::initial::Capture::None,
    )
    .await
    .map(crate::native_enrollment::caller::initial::RunPublication::into_sequence)
}
async fn save_run_publication_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
    capture: crate::native_enrollment::caller::initial::Capture,
) -> Result<crate::native_enrollment::caller::initial::RunPublication, StoreError> {
    let captured =
        native_parent::record_run_publication_with_proof(tx, run, false, source, proof, capture)
            .await?;
    update_run_row(tx, run).await?;
    Ok(captured)
}

async fn update_run_row(tx: &mut Transaction<'_, Postgres>, run: &Run) -> Result<(), StoreError> {
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
async fn load_run_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    id: &RunId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Run, StoreError> {
    read_proof::charge(
        tx,
        sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM runs WHERE id=$1")
            .bind(&id.0),
        proof,
    )
    .await
    .map_err(|_| StoreError::Validation("native run proof allowance".into()))?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(data::text)<=33554432 THEN data END AS data FROM runs WHERE id=$1 FOR UPDATE")
        .bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?.ok_or(StoreError::NotFound)?;
    decode(
        row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("data")
            .map_err(db)?
            .ok_or_else(|| StoreError::Validation("native run proof carrier".into()))?,
    )
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
async fn bump_public_revision(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<(), StoreError> {
    let changed = sqlx::query("UPDATE sessions SET public_revision=public_revision+1 WHERE id=$1 AND public_revision<9223372036854775807")
        .bind(&session.0).execute(&mut **tx).await.map_err(db)?.rows_affected();
    if changed != 1 {
        return Err(StoreError::Validation(
            "public session revision exhausted".into(),
        ));
    }
    Ok(())
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
    let accounting = snapshot::message_record(message)?;
    sqlx::query("INSERT INTO messages(id,session_id,run_id,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(&message.id.0)
        .bind(&message.session_id.0)
        .bind(&run.id.0)
        .bind(json(message)?)
        .bind(&accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    for part in &message.parts {
        insert_part(tx, part).await?;
    }
    bump_public_revision(tx, &run.session_id).await?;
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
    insert_event_captured(tx, run, event).await.map(|_| ())
}
async fn insert_event_captured(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    event: &EventRecord,
) -> Result<crabber_core::EventCursor, StoreError> {
    crate::event_persistence::validate(run, event)?;
    // Lock before identity allocation: cursor order follows session commit order.
    sqlx::query("SELECT 1 FROM sessions WHERE id=$1 FOR UPDATE")
        .bind(&run.session_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(json(event)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    bump_public_revision(tx, &run.session_id).await?;
    Ok(crabber_core::EventCursor(u64::try_from(seq).map_err(
        |_| StoreError::Validation("invalid event cursor".into()),
    )?))
}
async fn messages(
    connection: &mut PgConnection,
    id: &SessionId,
) -> Result<Vec<Message>, StoreError> {
    let rows = sqlx::query("SELECT snapshot_record FROM messages WHERE session_id=$1 ORDER BY seq")
        .bind(&id.0)
        .fetch_all(&mut *connection)
        .await
        .map_err(db)?;
    rows.into_iter()
        .map(|row| decode_record(&row.get::<String, _>("snapshot_record")))
        .collect()
}
async fn project(
    connection: &mut PgConnection,
    id: &SessionId,
    epoch: Option<EpochId>,
) -> Result<Vec<Message>, StoreError> {
    let exists = sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
        .bind(&id.0)
        .fetch_optional(&mut *connection)
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
                .fetch_optional(&mut *connection)
                .await
                .map_err(db)?;
        row.map(|row| decode::<Run>(row.get("data")).map(|run| run.epoch_id))
            .transpose()?
    };
    let selected = if let Some(epoch_id) = epoch_id {
        let row = sqlx::query("SELECT data FROM epochs WHERE id=$1")
            .bind(&epoch_id.0)
            .fetch_optional(&mut *connection)
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
    let all = messages(connection, id).await?;
    crate::context_projection::project(all, selected)
}

pub(super) async fn lookup_receipt_with_proof(
    tx: &mut transaction::Transaction<'_, Postgres>,
    session: &SessionId,
    key: &AdmissionKey,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<AdmissionReceipt>, StoreError> {
    read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2").bind(&session.0).bind(key.as_str()), proof).await.map_err(|_| StoreError::Conflict)?;
    lookup_receipt(tx, session, key).await
}
async fn lookup_receipt(
    connection: &mut PgConnection,
    session: &SessionId,
    key: &AdmissionKey,
) -> Result<Option<AdmissionReceipt>, StoreError> {
    sqlx::query("SELECT data FROM admission_receipts WHERE session_id=$1 AND admission_key=$2")
        .bind(&session.0)
        .bind(key.as_str())
        .fetch_optional(connection)
        .await
        .map_err(db)?
        .map(|row| decode(row.get("data")))
        .transpose()
}

impl PostgresStore {
    async fn session_transaction(
        &self,
        session: &SessionId,
    ) -> Result<Transaction<'_, Postgres>, StoreError> {
        self.session_transaction_scoped(crate::transaction::StoreScope::Session, session)
            .await
    }
    async fn session_transaction_scoped(
        &self,
        scope: crate::transaction::StoreScope,
        session: &SessionId,
    ) -> Result<Transaction<'_, Postgres>, StoreError> {
        let mut tx = crate::transaction::begin_scoped(&self.pool, scope)
            .await
            .map_err(db)?;
        lock_session(&mut tx, session).await?;
        Ok(tx)
    }
    // Discover immutable routing without a row lock, then take the session
    // mutation gate before any run/call/event lock. Verify the relational and
    // serialized identity agree before granting write authority.
    async fn run_transaction(
        &self,
        id: &RunId,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        self.run_transaction_scoped(crate::transaction::StoreScope::Run, id)
            .await
    }
    async fn run_transaction_scoped(
        &self,
        scope: crate::transaction::StoreScope,
        id: &RunId,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        let mut tx = crate::transaction::begin_scoped(&self.pool, scope)
            .await
            .map_err(db)?;
        let session: String = sqlx::query_scalar("SELECT session_id FROM runs WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        lock_session(&mut tx, &SessionId(session.clone())).await?;
        let run = load_run(&mut tx, id, true).await?;
        if run.id != *id || run.session_id.0 != session {
            return Err(StoreError::Validation(
                "stored run identity is invalid".into(),
            ));
        }
        Ok((tx, run))
    }
    async fn run_transaction_with_proof(
        &self,
        id: &RunId,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        self.run_transaction_with_proof_scoped(crate::transaction::StoreScope::Run, id, proof)
            .await
    }
    async fn run_transaction_with_proof_scoped(
        &self,
        scope: crate::transaction::StoreScope,
        id: &RunId,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        let mut tx = crate::transaction::begin_scoped(&self.pool, scope)
            .await
            .map_err(db)?;
        read_proof::charge(
            &mut tx,
            sqlx::query_scalar(
                "SELECT CAST(octet_length(session_id) AS BIGINT) FROM runs WHERE id=$1",
            )
            .bind(&id.0),
            proof,
        )
        .await
        .map_err(|_| StoreError::Validation("native routing proof allowance".into()))?;
        let session: Option<String> = sqlx::query_scalar("SELECT CASE WHEN octet_length(session_id)<=512 THEN session_id END FROM runs WHERE id=$1")
            .bind(&id.0).fetch_optional(&mut *tx).await.map_err(db)?.ok_or(StoreError::NotFound)?;
        let session =
            session.ok_or_else(|| StoreError::Validation("invalid native routing".into()))?;
        lock_session(&mut tx, &SessionId(session.clone())).await?;
        let run = load_run_with_proof(&mut tx, id, proof).await?;
        if run.id != *id || run.session_id.0 != session {
            return Err(StoreError::Validation(
                "stored run identity is invalid".into(),
            ));
        }
        Ok((tx, run))
    }
    pub(super) async fn admit_transaction_guarded_with_proof(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &AdmitRequest,
        allow_create: bool,
        guard: crate::queue::AdmissionGuard<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<AdmitOutcome, StoreError> {
        let captured = self
            .admit_transaction_captured(
                tx,
                request,
                allow_create,
                guard,
                crate::native_enrollment::caller::initial::Dispatch::raw(
                    crate::native_enrollment::caller::Caller::Raw,
                ),
                read_proof,
            )
            .await?;
        self.finish_initial(
            tx,
            crate::native_enrollment::caller::initial::Input::Ordinary(request),
            captured,
            read_proof,
        )
        .await
    }
    #[allow(clippy::too_many_lines)] // Existing atomic admission body, shared with verified queue admission.
    pub(super) async fn admit_transaction_captured(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        request: &AdmitRequest,
        allow_create: bool,
        guard: crate::queue::AdmissionGuard<'_>,
        caller: crate::native_enrollment::caller::initial::Dispatch<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::native_enrollment::caller::initial::CapturedAdmission, StoreError> {
        let birth = caller.birth();
        caller
            .input_request(request, read_proof)
            .map_err(|_| StoreError::Conflict)?;
        guard.verify(request)?;
        if request.input_envelope.is_some() || !request.additional_messages.is_empty() {
            request.validate_messages()?;
        }
        let now = self.clock.now();
        let (owner, requested_lease) = guard.owner_and_lease(request);
        let lease = time::Duration::try_from(requested_lease)
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
        session_owner::context_with_proof(
            tx,
            &session_id,
            &request.workspace_id,
            &request.directory,
            read_proof,
        )
        .await
        .map_err(|error| {
            if error == crate::session_owner::SessionOwnerError::IdentityMismatch {
                StoreError::SessionIdentityMismatch
            } else {
                StoreError::Validation("session ownership proof unavailable".into())
            }
        })?;
        let existing = admission_reads::session(tx, &session_id, read_proof).await?;
        let had_session = existing.is_some();
        let current_revision = existing.as_ref().map_or(0, |(_, revision)| *revision);
        let session: Session = if let Some((existing, _)) = existing {
            existing.ensure_identity(&request.workspace_id, &request.directory)?;
            existing
        } else if request.session_id.is_some() && !allow_create {
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
        if guard.is_ordinary()
            && let Some(expected) = guard.expected_revision(request.expected_revision)
        {
            let current = u64::try_from(current_revision)
                .map_err(|_| StoreError::Validation("invalid public session revision".into()))?;
            if expected.get() != current {
                return Err(StoreError::AdmissionRevisionConflict);
            }
        }
        if guard.is_ordinary() && sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND state='Claimed' AND prepared=true)").bind(&session_id.0).fetch_one(&mut **tx).await.map_err(db)? { return Err(StoreError::Busy); }
        if management::admission_archived_with_proof(tx, &session_id, read_proof).await? {
            return Err(StoreError::Conflict);
        }
        let barrier = admission_reads::pending(tx, &session_id, read_proof).await?;
        if barrier {
            return Err(StoreError::Busy);
        }
        let busy = sqlx::query(
            "SELECT 1 FROM runs WHERE session_id=$1 AND status IN ('pending','running','paused')",
        )
        .bind(&session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .is_some();
        if busy {
            return Err(StoreError::Busy);
        }
        request.validate_messages()?;
        let prior_history = if had_session {
            admission_reads::project(tx, &session_id, read_proof).await?
        } else {
            Vec::new()
        };
        let previous = admission_reads::previous_epoch(tx, &session_id, read_proof).await?;
        let run_id = RunId::new();
        let epoch_id = EpochId::new();
        let token = uuid::Uuid::new_v4().to_string();
        let run = Run {
            id: run_id.clone(),
            session_id: session_id.clone(),
            status: RunStatus::Running,
            owner: owner.to_owned(),
            claim_token: token.clone(),
            lease_until: now + lease,
            epoch_id: epoch_id.clone(),
            config_hash: request.config_hash.clone(),
            plan_fingerprint: request.plan_fingerprint.clone(),
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
            sqlx::query("INSERT INTO sessions(id,data,ownership_protocol,owner_scope_protocol,native_run_protocol) VALUES($1,$2,$3,COALESCE((SELECT scope_protocol FROM session_owner_contexts WHERE session_id=$1),0),COALESCE((SELECT native_run_protocol FROM session_owner_contexts WHERE session_id=$1),1))")
                .bind(&session.id.0)
                .bind(json(&session)?)
                .bind(i32::from(guard.has_owned_resources()))
                .execute(&mut **tx)
                .await
                .map_err(db)?;
        }
        sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data,native_source_protocol,execution_enrollment_protocol,execution_enrollment_origin_id) VALUES($1,$2,$3,$4,$5,$6,1,1,$1)")
            .bind(&run.id.0).bind(&session_id.0).bind(status(run.status)).bind(&token).bind(micros(run.lease_until)).bind(json(&run)?).execute(&mut **tx).await.map_err(db)?;
        if !guard.has_owned_resources()
            && matches!(
                &birth,
                crate::native_enrollment::pending_initial::BirthDecision::PendingInitial(_)
            )
        {
            return Err(StoreError::Conflict);
        }
        let raw_first = if guard.has_owned_resources() {
            None
        } else {
            Some(native_parent::record_initial_run_with_proof(tx, &run, read_proof).await?)
        };
        sqlx::query("INSERT INTO epochs(id,session_id,run_id,data) VALUES($1,$2,$3,$4)")
            .bind(&epoch.id.0)
            .bind(&session_id.0)
            .bind(&run.id.0)
            .bind(json(&epoch)?)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        let mut user = request.user_message.clone();
        user.run_id = Some(run_id.clone());
        insert_message(tx, &run, &user).await?;
        for original in &request.additional_messages {
            let mut message = original.clone();
            message.run_id = Some(run_id.clone());
            insert_message(tx, &run, &message).await?;
        }
        let owned_first = ownership_origin::record_admission(
            tx,
            &session,
            &run,
            !had_session,
            guard,
            &birth,
            read_proof,
        )
        .await?;
        let ((Some(first), None) | (None, Some(first))) = (raw_first, owned_first) else {
            return Err(StoreError::Conflict);
        };
        Ok(
            crate::native_enrollment::caller::initial::CapturedAdmission {
                first,
                admitted: AdmitOutcome {
                    session,
                    run,
                    fence: RunFence {
                        run_id,
                        claim_token: token,
                    },
                    assistant_placeholder: MessageId::new(),
                    epoch: epoch_id,
                    prior_history,
                },
            },
        )
    }
    async fn ownership_fenced_scoped(
        &self,
        fence: &RunFence,
        scope: crate::transaction::StoreScope,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        proof
            .measure(fence)
            .map_err(crate::native_enrollment::access::failure)?;
        let (mut tx, run) = self
            .run_transaction_with_proof_scoped(scope, &fence.run_id, &mut proof)
            .await?;
        if run.claim_token != fence.claim_token
            || run.status.is_terminal()
            || run.lease_until <= self.clock.now()
        {
            return Err(StoreError::Conflict);
        }
        credential_access::check_current(&mut tx, &run, &mut proof).await?;
        Ok((tx, run))
    }
    async fn fenced(
        &self,
        fence: &RunFence,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        self.fenced_scoped(fence, crate::transaction::StoreScope::Run)
            .await
    }
    async fn fenced_scoped(
        &self,
        fence: &RunFence,
        scope: crate::transaction::StoreScope,
    ) -> Result<(Transaction<'_, Postgres>, Run), StoreError> {
        let (mut tx, run) = self.ownership_fenced_scoped(fence, scope).await?;
        // Full decoding deliberately fails closed on malformed retained evidence.
        // Compact state reads can be optimized separately with equivalent validation.
        if admission_execution::load_record(&mut tx, &run.id)
            .await?
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired);
        }
        Ok((tx, run))
    }
}

#[cfg(test)]
pub(crate) fn test_lock() -> &'static tokio::sync::Mutex<()> {
    &tests::TEST_LOCK
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;

mod snapshot;

#[cfg(test)]
mod abandon_tests;
#[cfg(test)]
mod snapshot_tests;

#[cfg(test)]
pub(crate) async fn pending_initial_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    tests::TEST_LOCK.lock().await
}

#[cfg(test)]
mod pending_initial_fixture;
