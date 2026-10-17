//! Bounded original parent reservation read, independent of current Run.
use crate::session_owner::{SessionOwnerError as Error, preparation::expired::unstarted::Original};
use crabber_core::{ControlId, SessionId};
use sqlx::{Postgres, Transaction};
#[cfg(test)]
#[path = "unstarted/tests.rs"]
mod tests;
pub(in crate::postgres) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Original, Error> {
    read_with_proof(
        tx,
        session,
        id,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}
pub(in crate::postgres) async fn read_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Original, Error> {
    let receipt = super::super::attempt::records::receipt_with_proof(tx, session, id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(
            "SELECT CAST(octet_length(data::text) AS BIGINT) FROM events WHERE seq=$1",
        )
        .bind(i64::try_from(receipt.event_cursor.0).map_err(unknown)?),
        proof,
    )
    .await
    .map_err(unknown)?;
    let text: Option<String> = sqlx::query_scalar("SELECT CASE WHEN octet_length(data::text)<=65536 THEN data::text END FROM events WHERE seq=$1")
        .bind(i64::try_from(receipt.event_cursor.0).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
    let text = text.ok_or(Error::UnknownStoreOutcome)?;
    let encoded = serde_json::from_str::<serde_json::Value>(text.as_str()).map_err(unknown)?;
    let mut event =
        crate::public_state::decode_event(sqlx::types::Json(encoded)).map_err(unknown)?;
    if event
        .cursor
        .is_some_and(|cursor| cursor != receipt.event_cursor)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    event.cursor = Some(receipt.event_cursor);
    let original = Original { receipt, event };
    original.verify()?;
    Ok(original)
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

pub(super) async fn guard_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &crabber_core::Run,
    binding: &crate::RetainedExecutionBinding,
    now: time::OffsetDateTime,
    initial: (
        crabber_core::InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        crabber_core::InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    proof.charge(8).map_err(unknown)?;
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (SELECT 1 FROM execution_attempts WHERE run_id=$1 LIMIT 2) AS a",
    )
    .bind(&run.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if count == 0 {
        super::super::attempt::pristine_for_expired_with_proof(tx, run, proof)
            .await
            .map_err(|_| Error::Unsupported)?;
        return Ok(initial);
    }
    if count != 1 {
        return Err(Error::Unsupported);
    }
    proof.charge(1).map_err(unknown)?;
    let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if dirty {
        return Err(Error::Unsupported);
    }
    let input = crate::attempt::pristine::input_count(run, binding).map_err(unknown)?;
    proof.charge(8).map_err(unknown)?;
    let observed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS m")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(input + 1).map_err(unknown)?).fetch_one(&mut **tx).await.map_err(unknown)?;
    if observed != i64::try_from(input).map_err(unknown)? {
        return Err(Error::Unsupported);
    }
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(SUM(octet_length(data::text)) AS BIGINT) FROM (SELECT data FROM events WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT 2) selected_events").bind(&run.session_id.0).bind(&run.id.0), proof).await.map_err(unknown)?;
    let events: Vec<Option<String>> = sqlx::query_scalar("SELECT CASE WHEN octet_length(data::text)<=65536 THEN data::text END FROM events WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT 2")
        .bind(&run.session_id.0).bind(&run.id.0).fetch_all(&mut **tx).await.map_err(unknown)?;
    let [text] = events.as_slice() else {
        return Err(Error::Unsupported);
    };
    let text = text.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    let encoded = serde_json::from_str::<serde_json::Value>(text.as_str()).map_err(unknown)?;
    let event: crabber_core::EventRecord =
        crate::public_state::decode_event(sqlx::types::Json(encoded)).map_err(unknown)?;
    let id = ControlId(event.correlation.ok_or(Error::UnknownStoreOutcome)?);
    crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
    let original = read_with_proof(tx, &run.session_id, &id, proof).await?;
    let (record, _) =
        super::super::attempt::records::load_locked_with_proof(tx, &run.id, None, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
    original.verify_record(run, &record, now)?;
    Ok((original.digest(&initial.0)?, original.origin(initial.1)?))
}
