//! Actual claim under the exact authenticated preparation Join transaction.
use crate::session_owner::preparation::{
    PreparationCommitError, PreparedAdmissionRecovery, proof::VerifiedPreparation,
};
use crate::{AdmissionExecutionError as Error, ClaimedAdmission};
use sqlx::{Postgres, Transaction};
pub(in crate::postgres) async fn claim_prepared(
    store: &super::PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    recovery: &PreparedAdmissionRecovery,
    proof: &VerifiedPreparation<'_>,
) -> Result<ClaimedAdmission, PreparationCommitError> {
    recovery.validate()?;
    let source = &recovery.source;
    super::validate_source(tx, source, store.clock.now()).await?;
    let revision: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&source.request().session_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    let revision = crabber_core::SessionRevision::new(
        revision.try_into().map_err(|_| Error::SemanticConflict)?,
    )
    .map_err(|_| Error::SemanticConflict)?;
    let completion =
        crate::session_owner::preparation::recovery::completion_source(source, proof, revision)?;
    let run = super::super::load_run(tx, &source.request().expected_fence.run_id, true).await?;
    Ok(
        super::claim_in_transaction(store, tx, run, source.request().clone(), Some(completion))
            .await?,
    )
}
