//! Called under the session gate and fenced run transaction before reservation.
use super::super::{db, execution_binding};
use crate::{AttemptError as Error, attempt};
use crabber_core::Run;
use sqlx::{Postgres, Transaction};

pub(super) async fn check(tx: &mut Transaction<'_, Postgres>, run: &Run) -> Result<(), Error> {
    let binding = execution_binding::read(tx, &run.session_id, &run.id)
        .await
        .map_err(attempt::sql::unknown)?
        .ok_or(Error::InvalidTransition)?;
    let count = attempt::pristine::input_count(run, &binding)?;
    let observed: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS input_prefix")
        .bind(&run.session_id.0).bind(&run.id.0).bind(i64::try_from(count + 1).map_err(|_| Error::InvalidRequest)?)
        .fetch_one(&mut **tx).await.map_err(db).map_err(attempt::sql::unknown)?;
    let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE session_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$2) OR EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(db).map_err(attempt::sql::unknown)?;
    if observed != i64::try_from(count).map_err(|_| Error::InvalidRequest)? || dirty {
        return Err(Error::InvalidTransition);
    }
    Ok(())
}
