//! Record persistence and context projection; callers own transaction boundaries.
use super::{Sqlite, SqliteConnection, StoreError, Transaction, db, decode, text};
use crate::InboxKind;
use crate::sql_snapshot as snapshot;
use crabber_core::{
    AdmissionKey, AdmissionReceipt, ContextEpoch, EpochId, EventRecord, Message, Part, Run, RunId,
    RunStatus, SessionId, ToolCallId, ToolCallRecord, ToolCallStatus,
};
use serde::de::DeserializeOwned;
use sqlx::{Row, sqlite::SqliteRow};
use time::OffsetDateTime;

pub(super) fn decode_row<T: DeserializeOwned>(
    row: &SqliteRow,
    column: &str,
) -> Result<T, StoreError> {
    let data: &str = row
        .try_get(column)
        .map_err(|_| StoreError::Validation("stored record is invalid".into()))?;
    decode(data)
}

pub(super) fn micros(time: OffsetDateTime) -> i64 {
    i64::try_from(time.unix_timestamp_nanos() / 1_000).unwrap_or(i64::MAX)
}
pub(super) fn inbox_kind(kind: InboxKind) -> &'static str {
    match kind {
        InboxKind::Steer => "steer",
        InboxKind::FollowUp => "follow_up",
    }
}
pub(super) fn status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Pending => "pending",
        RunStatus::Running => "running",
        RunStatus::Paused => "paused",
        RunStatus::Interrupted => "interrupted",
        RunStatus::Failed => "failed",
        RunStatus::Completed => "completed",
    }
}
pub(super) fn call_status(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Waiting => "waiting",
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::Running => "running",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        ToolCallStatus::Interrupted => "interrupted",
    }
}
pub(super) async fn save_run(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), StoreError> {
    save_run_with_completion(tx, run, None).await
}
pub(super) async fn save_run_with_completion(
    tx: &mut Transaction<'_, Sqlite>,
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
pub(super) async fn save_run_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), StoreError> {
    super::native_parent::record_run_with_proof(tx, run, false, source, proof).await?;
    update_run_row(tx, run).await
}
pub(super) async fn save_run_captured_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
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
pub(super) async fn save_run_publication_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    source: Option<&crate::native_parent::CompletionSource>,
    proof: &mut crate::public_session::v2::proof::Proof,
    capture: crate::native_enrollment::caller::initial::Capture,
) -> Result<crate::native_enrollment::caller::initial::RunPublication, StoreError> {
    let captured = super::native_parent::record_run_publication_with_proof(
        tx, run, false, source, proof, capture,
    )
    .await?;
    update_run_row(tx, run).await?;
    Ok(captured)
}

async fn update_run_row(tx: &mut Transaction<'_, Sqlite>, run: &Run) -> Result<(), StoreError> {
    sqlx::query(
        "UPDATE runs SET status=$2, claim_token=$3, lease_until=$4, data=$5 WHERE \
            id=$1",
    )
    .bind(&run.id.0)
    .bind(status(run.status))
    .bind(&run.claim_token)
    .bind(micros(run.lease_until))
    .bind(text(run)?)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}
pub(super) async fn save_call(
    tx: &mut Transaction<'_, Sqlite>,
    id: &ToolCallId,
    call: &ToolCallRecord,
) -> Result<(), StoreError> {
    text(call)?;
    let accounting = snapshot::call_record(call)?;
    sqlx::query("UPDATE tool_calls SET status=$2,data=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1")
        .bind(&id.0)
        .bind(call_status(call.status))
        .bind(&accounting.record)
        .bind(accounting.parts)
        .bind(accounting.text)
        .bind(accounting.bytes)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}

pub(super) async fn load_run_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    id: &RunId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Run, StoreError> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar("SELECT length(CAST(data AS BLOB)) FROM runs WHERE id=$1").bind(&id.0),
        proof,
    )
    .await
    .map_err(|_| StoreError::Validation("native run proof allowance".into()))?;
    let row = sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM runs WHERE id=$1")
        .bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?.ok_or(StoreError::NotFound)?;
    super::records::decode_row(&row, "data")
}
pub(super) async fn load_run(
    tx: &mut Transaction<'_, Sqlite>,
    id: &RunId,
) -> Result<Run, StoreError> {
    let query = "SELECT data FROM runs WHERE id=$1";
    let row = sqlx::query(query)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .ok_or(StoreError::NotFound)?;
    decode_row(&row, "data")
}
pub(super) async fn bump_public_revision(
    tx: &mut Transaction<'_, Sqlite>,
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

pub(super) async fn insert_message(
    tx: &mut Transaction<'_, Sqlite>,
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
    text(message)?;
    let accounting = snapshot::message_record(message)?;
    sqlx::query(
        "INSERT INTO \
            messages(id,session_id,run_id,data,snapshot_parts,snapshot_text,snapshot_bytes) \
            VALUES($1,$2,$3,$4,$5,$6,$7)",
    )
    .bind(&message.id.0)
    .bind(&message.session_id.0)
    .bind(&run.id.0)
    .bind(&accounting.record)
    .bind(accounting.parts)
    .bind(accounting.text)
    .bind(accounting.bytes)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    for part in &message.parts {
        insert_part(tx, part).await?;
    }
    bump_public_revision(tx, &run.session_id).await?;
    Ok(())
}
pub(super) async fn insert_part(
    tx: &mut Transaction<'_, Sqlite>,
    part: &Part,
) -> Result<(), StoreError> {
    sqlx::query("INSERT INTO parts(id,message_id,ordinal,data) VALUES($1,$2,$3,$4)")
        .bind(&part.id.0)
        .bind(&part.message_id.0)
        .bind(i64::from(part.ordinal))
        .bind(text(part)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok(())
}
pub(super) async fn insert_event(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    event: &EventRecord,
) -> Result<(), StoreError> {
    insert_event_captured(tx, run, event).await.map(|_| ())
}
pub(super) async fn insert_event_captured(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    event: &EventRecord,
) -> Result<crabber_core::EventCursor, StoreError> {
    crate::event_persistence::validate(run, event)?;
    // BEGIN IMMEDIATE serializes allocation with commit order.
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(text(event)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    bump_public_revision(tx, &run.session_id).await?;
    Ok(crabber_core::EventCursor(u64::try_from(seq).map_err(
        |_| StoreError::Validation("invalid event cursor".into()),
    )?))
}
pub(super) async fn messages(
    connection: &mut SqliteConnection,
    id: &SessionId,
) -> Result<Vec<Message>, StoreError> {
    let rows = sqlx::query("SELECT data FROM messages WHERE session_id=$1 ORDER BY seq")
        .bind(&id.0)
        .fetch_all(&mut *connection)
        .await
        .map_err(db)?;
    rows.into_iter()
        .map(|row| decode_row(&row, "data"))
        .collect()
}
pub(super) async fn project(
    connection: &mut SqliteConnection,
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
        row.map(|row| decode_row::<Run>(&row, "data").map(|run| run.epoch_id))
            .transpose()?
    };
    let selected = if let Some(epoch_id) = epoch_id {
        let row = sqlx::query("SELECT data FROM epochs WHERE id=$1")
            .bind(&epoch_id.0)
            .fetch_optional(&mut *connection)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let value: ContextEpoch = decode_row(&row, "data")?;
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    key: &AdmissionKey,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<AdmissionReceipt>, StoreError> {
    super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2").bind(&session.0).bind(key.as_str()), proof).await.map_err(|error| proof.admission_failure(&error, StoreError::Conflict))?;
    lookup_receipt(tx, session, key).await
}
pub(super) async fn lookup_receipt(
    connection: &mut SqliteConnection,
    session: &SessionId,
    key: &AdmissionKey,
) -> Result<Option<AdmissionReceipt>, StoreError> {
    sqlx::query(
        "SELECT data FROM admission_receipts WHERE session_id=$1 AND \
            admission_key=$2",
    )
    .bind(&session.0)
    .bind(key.as_str())
    .fetch_optional(connection)
    .await
    .map_err(db)?
    .map(|row| decode_row(&row, "data"))
    .transpose()
}
