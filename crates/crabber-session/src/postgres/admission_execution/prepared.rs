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
    read_proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ClaimedAdmission, PreparationCommitError> {
    recovery
        .source
        .measure_original(read_proof)
        .map_err(|_| Error::UnknownStoreFailure)?;
    read_proof
        .measure(&*recovery.binding)
        .map_err(|_| Error::UnknownStoreFailure)?;
    recovery.validate()?;
    let source = &recovery.source;
    super::inspection::validate_source_with_proof(tx, source, store.clock.now(), read_proof)
        .await?;
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
    let run =
        super::super::load_run_with_proof(tx, &source.request().expected_fence.run_id, read_proof)
            .await?;
    Ok(super::claim_in_transaction(
        store,
        tx,
        run,
        source.request().clone(),
        Some(completion),
        read_proof,
    )
    .await?)
}
