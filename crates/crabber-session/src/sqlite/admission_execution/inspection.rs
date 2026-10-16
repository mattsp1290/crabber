//! Coherent pure recovery preflight; no owner, grant or revision publication.
use super::{Error, SqliteStore};
use crate::{
    ClaimUnstartedAdmissionRequest, session_owner::preparation::AdmissionRecoveryPreparation,
};
use sqlx::{Sqlite, Transaction};

pub(in crate::sqlite) async fn inspect(
    store: &SqliteStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<AdmissionRecoveryPreparation, Error> {
    AdmissionRecoveryPreparation::validate_request(&request)?;
    let mut tx = store.begin_write().await.map_err(super::unknown)?;
    let result = context(&mut tx, &request, store.clock.now()).await;
    tx.rollback()
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    let (revision, sequence, binding) = result?;
    let digest = AdmissionRecoveryPreparation::fingerprint_binding(&binding)
        .map_err(|_| Error::SemanticConflict)?;
    AdmissionRecoveryPreparation::from_verified_source(request, revision, sequence, digest)
}
async fn context(
    tx: &mut Transaction<'_, Sqlite>,
    request: &ClaimUnstartedAdmissionRequest,
    now: time::OffsetDateTime,
) -> Result<
    (
        crabber_core::SessionRevision,
        u64,
        crate::ExecutionBindingCapsule,
    ),
    Error,
> {
    let bounded: Option<bool> = sqlx::query_scalar("SELECT length(CAST(data AS BLOB))<=32768 AND (json_type(data,'$.checkpoint') IS NULL OR json_type(data,'$.checkpoint')='null') AND (json_type(data,'$.error') IS NULL OR json_type(data,'$.error')='null') FROM runs WHERE session_id=$1 AND id=$2")
        .bind(&request.session_id.0).bind(&request.expected_fence.run_id.0)
        .fetch_optional(&mut **tx).await.map_err(|_| Error::UnknownStoreFailure)?;
    if bounded != Some(true) {
        return Err(Error::SemanticConflict);
    }
    let run = super::super::session_owner::run::load(
        tx,
        &request.session_id,
        &request.expected_fence.run_id,
    )
    .await
    .map_err(|_| Error::SemanticConflict)?;
    let record = super::load_record(tx, &run.id)
        .await
        .map_err(super::unknown)?
        .ok_or(Error::MissingEvidence)?;
    if super::lookup_receipt(tx, &request.session_id, &request.key)
        .await
        .map_err(super::unknown)?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::SemanticConflict);
    }
    super::validate_binding(tx, &record, &run).await?;
    record.verify_claim(request, &run)?;
    if run.status != crabber_core::RunStatus::Running {
        return Err(Error::SemanticConflict);
    }
    if run.lease_until > now {
        return Err(Error::LiveLease);
    }
    let sequence = super::super::native_parent::current_issuance(tx, &run)
        .await
        .map_err(|_| Error::SemanticConflict)?
        .sequence;
    let revision: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&request.session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?
        .ok_or(Error::MissingEvidence)?;
    let revision = crabber_core::SessionRevision::new(
        revision.try_into().map_err(|_| Error::SemanticConflict)?,
    )
    .map_err(|_| Error::SemanticConflict)?;
    let binding = super::super::execution_binding::read(tx, &request.session_id, &run.id)
        .await
        .map_err(|_| Error::SemanticConflict)?
        .ok_or(Error::MissingEvidence)?;
    Ok((revision, sequence, binding.capsule))
}

pub(in crate::sqlite) async fn validate_source(
    tx: &mut Transaction<'_, Sqlite>,
    source: &AdmissionRecoveryPreparation,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    source.fingerprint()?;
    let (_, sequence, binding) = context(tx, source.request(), now).await?;
    source.verify_binding(&binding)?;
    if sequence != source.expected_issuance() {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
