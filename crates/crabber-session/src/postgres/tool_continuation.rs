//! Explicit tool checkpoint transactions; session gate precedes run/native rows.
mod answer;
mod claim;
mod original_plan;
pub(super) use claim::waiting_context;
mod cancel;
pub(super) mod current;
pub(super) use cancel::cancel_waiting;
pub(super) mod scan;
pub(super) use scan::pending as pending_in_session;
mod evidence;
mod guards;
mod phases;
pub(super) mod read;
mod recover;
mod suspend;
pub(super) mod write;
use super::{PostgresExecution, PostgresStore};
use crate::tool_continuation::{self as domain, ToolContinuationRecord};
use crate::{
    ToolContinuationClaim, ToolContinuationClaimOutcome, ToolContinuationError as Error,
    ToolContinuationOutcome, ToolContinuationReceipt, ToolContinuationSnapshot,
    ToolContinuationState, ToolContinuationSuspension, ToolInteractionAnswer,
    ToolSuspensionAuthority,
};
use crabber_core::{ControlId, InputFingerprint, Run, RunId, SessionId, SessionRevision};
pub(super) use guards::{guard_attempt, release_terminal};
use sqlx::{Postgres, Row, Transaction};
use time::OffsetDateTime;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T, Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            tx.commit().await.map_err(unknown)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await.map_err(unknown)?;
            Err(error)
        }
    }
}
pub(super) async fn revision(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<SessionRevision, Error> {
    let value: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    SessionRevision::new(u64::try_from(value).map_err(unknown)?).map_err(unknown)
}
impl PostgresStore {
    async fn tool_transaction(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<(Transaction<'_, Postgres>, Run), Error> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM runs WHERE session_id=$1 AND id=$2)")
                .bind(&session.0)
                .bind(&run.0)
                .fetch_one(&self.pool)
                .await
                .map_err(unknown)?;
        if !exists {
            return Err(Error::NotFound);
        }
        let (tx, saved) = self.run_transaction(run).await.map_err(unknown)?;
        if saved.session_id != *session || saved.id != *run {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok((tx, saved))
    }
    pub(super) async fn read_tools(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ToolContinuationSnapshot>, Error> {
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = async {
            read::origin(&mut tx, session, run, None).await?;
            let record = scan::scan(&mut tx, session, Some(run)).await?.latest;
            let revision = revision(&mut tx, session).await?;
            Ok(record.map(|record| record.snapshot(revision)))
        }
        .await;
        finish(tx, result).await
    }
    pub(super) async fn tool_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ToolContinuationReceipt>, Error> {
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        let result = read::receipt(&mut tx, session, id)
            .await
            .map(|saved| saved.map(|(receipt, _)| receipt));
        finish(tx, result).await
    }
}
pub(super) fn fault(store: &PostgresStore) -> u8 {
    #[cfg(test)]
    {
        store
            .tool_continuation_fault
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    #[cfg(not(test))]
    {
        let _ = store;
        0
    }
}
pub(super) fn before(fault: u8) -> Result<(), Error> {
    if fault == 1 {
        Err(Error::UnknownStoreOutcome)
    } else {
        Ok(())
    }
}
pub(super) fn after<T>(result: Result<(T, bool), Error>, fault: u8) -> Result<T, Error> {
    match result {
        Ok((_, true)) if fault == 2 => Err(Error::UnknownStoreOutcome),
        Ok((value, _)) => Ok(value),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests;

pub(super) use write::replay_scope as replay_prepared;

pub(super) use claim::preparation_context;

pub(super) use recover::preparation_context as preparation_recovery_context;
#[cfg(test)]
mod aggregate_proof_tests;

pub(super) use claim::preparation_frontend_context;
pub(super) use recover::preparation_frontend_context as preparation_frontend_recovery_context;
