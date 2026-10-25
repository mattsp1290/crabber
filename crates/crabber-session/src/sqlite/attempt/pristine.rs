//! Called under the session gate and fenced run transaction before reservation.
use super::super::{db, execution_binding};
use crate::transaction::Transaction;
use crate::{AttemptError as Error, attempt};
use crabber_core::Run;
use sqlx::Sqlite;

pub(super) async fn check(tx: &mut Transaction<'_, Sqlite>, run: &Run) -> Result<(), Error> {
    check_with_proof(
        tx,
        run,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await
}
pub(super) async fn check_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let binding = execution_binding::read_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::InvalidTransition)?;
    let count = attempt::pristine::input_count(run, &binding)?;
    let observed: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS input_prefix")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(count + 1).map_err(|_| Error::InvalidRequest)?)
        .fetch_one(&mut **tx).await.map_err(db).map_err(attempt::sql::unknown)?;
    let controls = super::super::run_control::accepted_event_count(tx, run, proof).await?;
    proof.charge(8).map_err(|_| Error::UnknownStoreOutcome)?;
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM events WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS initial_events")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(controls + 1).map_err(|_| Error::InvalidRequest)?)
        .fetch_one(&mut **tx).await.map_err(db).map_err(attempt::sql::unknown)?;
    let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$2) OR EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(db).map_err(attempt::sql::unknown)?;
    if observed != i64::try_from(count).map_err(|_| Error::InvalidRequest)?
        || events != i64::try_from(controls).map_err(|_| Error::InvalidRequest)?
        || dirty
    {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}
