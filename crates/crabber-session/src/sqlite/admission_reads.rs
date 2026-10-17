//! Bounded original admission history; scalar preflight and CASE carriers share the caller Proof.
use super::{db, decode as decode_record};
use crate::{StoreError, public_session::v2::proof::Proof};
use crabber_core::{ContextEpoch, EpochId, Message, PartKind, Run, Session, SessionId};
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn project(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Vec<Message>, StoreError> {
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE((SELECT SUM(length(CAST(data AS BLOB))) FROM (SELECT data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1) last_run),0)+COALESCE((SELECT SUM(length(CAST(e.data AS BLOB))) FROM epochs e WHERE e.id=(SELECT json_extract(data,'$.epoch_id') FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1)),0)+COALESCE((SELECT SUM(length(CAST(data AS BLOB))) FROM messages WHERE session_id=$1),0) AS BIGINT)").bind(&id.0),proof).await.map_err(|_|StoreError::Conflict)?;
    let epoch: Option<EpochId> = None;
    let exists = sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
    if exists.is_none() {
        return Err(StoreError::NotFound);
    }
    let epoch_id = if let Some(epoch) = epoch {
        Some(epoch)
    } else {
        let row =
            sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1")
                .bind(&id.0)
                .fetch_optional(&mut **tx)
                .await
                .map_err(db)?;
        row.map(|row| read::<Run>(&row).map(|run| run.epoch_id))
            .transpose()?
    };
    let selected = if let Some(epoch_id) = epoch_id {
        let row = sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM epochs WHERE id=$1")
            .bind(&epoch_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let value: ContextEpoch = read(&row)?;
        if value.session_id != *id {
            return Err(StoreError::Validation(
                "epoch belongs to another session".into(),
            ));
        }
        Some(value)
    } else {
        None
    };
    let all = bounded_messages(tx, id).await?;
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

async fn bounded_messages(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
) -> Result<Vec<Message>, StoreError> {
    let rows=sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS record FROM messages WHERE session_id=$1 ORDER BY seq")
        .bind(&id.0).fetch_all(&mut **tx).await.map_err(db)?;
    rows.into_iter()
        .map(|row| {
            decode_record(
                &row.try_get::<Option<String>, _>("record")
                    .map_err(db)?
                    .ok_or(StoreError::Conflict)?,
            )
        })
        .collect()
}
pub(super) async fn session(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Option<(Session, i64)>, StoreError> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(
            "SELECT CAST(length(CAST(data AS BLOB))+8 AS BIGINT) FROM sessions WHERE id=$1",
        )
        .bind(&id.0),
        proof,
    )
    .await
    .map_err(|_| StoreError::Conflict)?;
    let row=sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data,public_revision FROM sessions WHERE id=$1").bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    row.map(|row| {
        let text = row
            .try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or(StoreError::Conflict)?;
        let session: Session = super::decode(&text)?;
        Ok((session, row.try_get("public_revision").map_err(db)?))
    })
    .transpose()
}

pub(super) async fn previous_epoch(
    tx: &mut Transaction<'_, Sqlite>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Option<ContextEpoch>, StoreError> {
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1").bind(&id.0),proof).await.map_err(|_|StoreError::Conflict)?;
    let row=sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1").bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    let Some(row) = row else { return Ok(None) };
    let run: Run = read(&row)?;
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(
            "SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM epochs WHERE id=$1",
        )
        .bind(&run.epoch_id.0),
        proof,
    )
    .await
    .map_err(|_| StoreError::Conflict)?;
    let row=sqlx::query("SELECT CASE WHEN length(CAST(data AS BLOB))<=33554432 THEN data END AS data FROM epochs WHERE id=$1").bind(&run.epoch_id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    row.as_ref().map(read).transpose()
}

fn read<T: serde::de::DeserializeOwned>(row: &sqlx::sqlite::SqliteRow) -> Result<T, StoreError> {
    let text = row
        .try_get::<Option<String>, _>("data")
        .map_err(db)?
        .ok_or(StoreError::Conflict)?;
    serde_json::from_str(&text).map_err(|_| StoreError::Conflict)
}

pub(super) async fn pending(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, StoreError> {
    Box::pin(super::approval::verify_session_with_proof(
        tx, session, proof,
    ))
    .await
    .map_err(|_| StoreError::Validation("approval integrity failure".into()))?;
    let pending = Box::pin(super::tool_continuation::pending_in_session_with_proof(
        tx, session, proof,
    ))
    .await
    .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?;
    let approval: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    Ok(approval || pending)
}
