//! Bounded exact current/adjacent issuer and original sources in the native Tx.
use crate::native_parent::{RunIssuance, completion_history as history};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{PreparationCompletion, PreparationGrant},
};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    grant: &PreparationGrant,
    completed: &PreparationCompletion,
) -> Result<(), Error> {
    let Some((session, run, cursor)) = history::metadata(completed) else {
        return Ok(());
    };
    let ids = crate::session_owner::preparation::identity::ids(
        &grant.original_reservation().key,
        session,
    )?;
    let rows = sqlx::query(SOURCES)
        .bind(&session.0)
        .bind(&run.0)
        .bind(i64::try_from(cursor.0).map_err(unknown)?)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    let mut selected = None;
    for row in rows {
        let current: RunIssuance =
            serde_json::from_str(&carrier(&row, "current")?).map_err(unknown)?;
        current.validate().map_err(unknown)?;
        if i64::try_from(current.sequence).map_err(unknown)?
            != row.try_get::<i64, _>("sequence").map_err(unknown)?
            || current.state.session_id != *session
            || current.state.run_id != *run
        {
            return Err(Error::UnknownStoreOutcome);
        }
        if current
            .completion_source
            .as_ref()
            .is_some_and(|source| source.root_join_id == ids.join)
        {
            selected = Some((row, current));
            break;
        }
    }
    let (row, current) = selected.ok_or(Error::UnknownStoreOutcome)?;
    let previous: RunIssuance =
        serde_json::from_str(&carrier(&row, "previous")?).map_err(unknown)?;
    let text = carrier(&row, "event")?;
    let mut event: crabber_core::EventRecord = crate::public_state::decode_event(
        sqlx::types::Json(serde_json::from_str(&text).map_err(unknown)?),
    )
    .map_err(unknown)?;
    event.cursor = Some(cursor);
    let mut audits = Vec::with_capacity(3);
    for id in [&ids.reserve, &ids.begin, &ids.join] {
        audits.push(
            crate::postgres::session_owner::source_audit(tx, session, id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    let control = history::control(completed, &event).map_err(unknown)?;
    if !history::verify_control(
        &previous, &current, &audits[0], &audits[1], &audits[2], &control,
    )
    .map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
fn carrier(row: &sqlx::postgres::PgRow, name: &str) -> Result<String, Error> {
    row.try_get::<Option<String>, _>(name)
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)
}
const SOURCES: &str = "SELECT n.sequence,CASE WHEN octet_length(n.data_json)<=2048 THEN n.data_json END AS current,CASE WHEN octet_length(p.data_json)<=2048 THEN p.data_json END AS previous,CASE WHEN octet_length(e.data::text)<=65536 THEN e.data::text END AS event FROM native_run_issuances n LEFT JOIN native_run_issuances p ON p.session_id=n.session_id AND p.run_id=n.run_id AND p.sequence=n.sequence-1 LEFT JOIN events e ON e.session_id=n.session_id AND e.run_id=n.run_id AND e.seq=$3 WHERE n.session_id=$1 AND n.run_id=$2 ORDER BY n.sequence DESC LIMIT 2";
