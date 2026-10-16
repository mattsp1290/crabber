//! Bounded original parent reservation read, independent of current Run.
use crate::session_owner::{SessionOwnerError as Error, preparation::expired::unstarted::Original};
use crabber_core::{ControlId, SessionId};
use sqlx::{Sqlite, Transaction};
#[cfg(test)]
#[path = "unstarted/tests.rs"]
mod tests;
pub(in crate::sqlite) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Original, Error> {
    let receipt = super::super::attempt::records::receipt(tx, session, id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let text: Option<String> = sqlx::query_scalar(
        "SELECT CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END FROM events WHERE seq=$1",
    )
    .bind(i64::try_from(receipt.event_cursor.0).map_err(unknown)?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)?;
    let text = text.ok_or(Error::UnknownStoreOutcome)?;
    let mut event: crabber_core::EventRecord = serde_json::from_str(&text).map_err(unknown)?;
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

pub(super) async fn guard(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    binding: &crate::RetainedExecutionBinding,
    now: time::OffsetDateTime,
    initial: (
        crabber_core::InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
) -> Result<
    (
        crabber_core::InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM (SELECT 1 FROM execution_attempts WHERE run_id=$1 LIMIT 2) AS a",
    )
    .bind(&run.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if count == 0 {
        super::super::attempt::pristine_for_expired(tx, run)
            .await
            .map_err(|_| Error::Unsupported)?;
        return Ok(initial);
    }
    if count != 1 {
        return Err(Error::Unsupported);
    }
    let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if dirty {
        return Err(Error::Unsupported);
    }
    let input = crate::attempt::pristine::input_count(run, binding).map_err(unknown)?;
    let observed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS m")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(input + 1).map_err(unknown)?).fetch_one(&mut **tx).await.map_err(unknown)?;
    if observed != i64::try_from(input).map_err(unknown)? {
        return Err(Error::Unsupported);
    }
    let events: Vec<Option<String>> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END FROM events WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT 2")
        .bind(&run.session_id.0).bind(&run.id.0).fetch_all(&mut **tx).await.map_err(unknown)?;
    let [text] = events.as_slice() else {
        return Err(Error::Unsupported);
    };
    let text = text.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    let event: crabber_core::EventRecord = serde_json::from_str(text.as_str()).map_err(unknown)?;
    let id = ControlId(event.correlation.ok_or(Error::UnknownStoreOutcome)?);
    crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
    let original = read(tx, &run.session_id, &id).await?;
    let (record, _) = super::super::attempt::records::load(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    original.verify_record(run, &record, now)?;
    Ok((original.digest(&initial.0)?, original.origin(initial.1)?))
}
