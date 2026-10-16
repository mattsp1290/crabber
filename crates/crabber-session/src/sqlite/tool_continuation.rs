//! Explicit tool checkpoint transactions; session gate precedes run/native rows.
mod answer;
mod claim;
mod original_plan;
pub(super) use claim::waiting_context;
mod cancel;
mod current;
pub(super) use cancel::cancel_waiting;
mod scan;
pub(super) use scan::pending as pending_in_session;
mod evidence;
mod guards;
mod phases;
pub(super) mod read;
mod recover;
mod suspend;
pub(super) mod write;
use super::{SqliteExecution, SqliteStore};
use crate::tool_continuation::{self as domain, ToolContinuationRecord};
use crate::{
    ToolContinuationClaim, ToolContinuationClaimOutcome, ToolContinuationError as Error,
    ToolContinuationOutcome, ToolContinuationReceipt, ToolContinuationSnapshot,
    ToolContinuationState, ToolContinuationSuspension, ToolInteractionAnswer,
    ToolSuspensionAuthority,
};
use crabber_core::{ControlId, InputFingerprint, Run, RunId, SessionId, SessionRevision};
pub(super) use guards::{guard_attempt, release_terminal};
use sqlx::{Row, Sqlite, Transaction};
use time::OffsetDateTime;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<SessionRevision, Error> {
    let value: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    SessionRevision::new(u64::try_from(value).map_err(unknown)?).map_err(unknown)
}
impl SqliteStore {
    async fn tool_transaction(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<(Transaction<'_, Sqlite>, Run), Error> {
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let saved = super::records::load_run(&mut tx, run)
            .await
            .map_err(unknown)?;
        if saved.session_id != *session || saved.id != *run {
            tx.rollback().await.map_err(unknown)?;
            return Err(Error::NotFound);
        }
        Ok((tx, saved))
    }
    pub(super) async fn read_tools(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ToolContinuationSnapshot>, Error> {
        let mut tx = self.readers.begin().await.map_err(unknown)?;
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
        let mut tx = self.readers.begin().await.map_err(unknown)?;
        let result = read::receipt(&mut tx, session, id)
            .await
            .map(|saved| saved.map(|(receipt, _)| receipt));
        finish(tx, result).await
    }
}
pub(super) fn fault(store: &SqliteStore) -> u8 {
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
