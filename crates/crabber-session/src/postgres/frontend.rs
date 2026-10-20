//! Schema14 frontend origins. Schema registration is owned by the frontend slice.
mod continuation;
mod read;
pub(super) use continuation::{
    mark_waiting, pending_call, pending_call_with_proof, recover_call, save_links,
    verify_batch_with_proof,
};
mod stage;
use super::{PostgresExecution, PostgresStore};
use crate::frontend_continuation::FrontendOriginRecord;
use crate::postgres::transaction::Transaction;
use crate::{FrontendCallOrigin, FrontendStagingReceipt, ToolContinuationError as Error};
use crabber_core::{ControlId, Run, RunId, SessionId, ToolCallId};
pub(super) use read::{declared, origin_with_proof};
use sqlx::Postgres;
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
impl PostgresStore {
    pub(super) async fn frontend_transaction(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<(Transaction<'_, Postgres>, Run), Error> {
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        super::lock_session(&mut tx, session)
            .await
            .map_err(unknown)?;
        let run = read::run(&mut tx, session, run, true).await?;
        Ok((tx, run))
    }
    pub(super) async fn read_frontend_origin(
        &self,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallId,
    ) -> Result<Option<FrontendCallOrigin>, Error> {
        crate::tool_continuation::canonical::identity(&session.0, 512)?;
        crate::tool_continuation::canonical::identity(&run.0, 128)?;
        crate::tool_continuation::canonical::identity(&call.0, 512)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = async {
            let run = read::run(&mut tx, session, run, false).await?;
            origin(&mut tx, &run, call)
                .await?
                .map(|record| {
                    let call_binding =
                        crate::phase::call_binding(&record.request.call).map_err(unknown)?;
                    Ok(FrontendCallOrigin {
                        call_id: record.request.call.id,
                        descriptor: record.request.descriptor,
                        descriptor_fingerprint: record.request.descriptor_fingerprint,
                        call_binding,
                    })
                })
                .transpose()
        }
        .await;
        finish(tx, result).await
    }
    pub(super) async fn frontend_staging_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<FrontendStagingReceipt>, Error> {
        crate::tool_continuation::canonical::identity(&session.0, 512)?;
        crate::tool_continuation::canonical::identity(&id.0, 128)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        let result = read::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
pub(super) async fn origin(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    call: &ToolCallId,
) -> Result<Option<FrontendOriginRecord>, Error> {
    read::origin(tx, run, call).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod read_tests;
