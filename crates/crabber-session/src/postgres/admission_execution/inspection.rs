//! Coherent pure recovery preflight; no owner, grant or revision publication.
use super::{Error, PostgresStore};
use crate::postgres::transaction::Transaction;
use crate::{
    ClaimUnstartedAdmissionRequest, session_owner::preparation::AdmissionRecoveryPreparation,
};
use sqlx::Postgres;

pub(in crate::postgres) async fn inspect(
    store: &PostgresStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<AdmissionRecoveryPreparation, Error> {
    AdmissionRecoveryPreparation::validate_request(&request)?;
    let mut tx = store
        .session_transaction_scoped(
            crate::transaction::StoreScope::Admission,
            &request.session_id,
        )
        .await
        .map_err(super::unknown)?;
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
    tx: &mut Transaction<'_, Postgres>,
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
    context_with_proof(
        tx,
        request,
        now,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}
async fn context_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    request: &ClaimUnstartedAdmissionRequest,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        crabber_core::SessionRevision,
        u64,
        crate::ExecutionBindingCapsule,
    ),
    Error,
> {
    proof.charge(1).map_err(|_| Error::UnknownStoreFailure)?;
    let bounded: Option<bool> = sqlx::query_scalar("SELECT octet_length(data::text)<=32768 AND (data->'checkpoint' IS NULL OR data->'checkpoint'='null'::jsonb) AND (data->'error' IS NULL OR data->'error'='null'::jsonb) FROM runs WHERE session_id=$1 AND id=$2 FOR UPDATE")
        .bind(&request.session_id.0).bind(&request.expected_fence.run_id.0)
        .fetch_optional(&mut **tx).await.map_err(|_| Error::UnknownStoreFailure)?;
    if bounded != Some(true) {
        return Err(Error::SemanticConflict);
    }
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length((data-'checkpoint'-'error')::text) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2").bind(&request.session_id.0).bind(&request.expected_fence.run_id.0), proof).await.map_err(|_| Error::UnknownStoreFailure)?;
    let run = super::super::session_owner::run::load(
        tx,
        &request.session_id,
        &request.expected_fence.run_id,
    )
    .await
    .map_err(|_| Error::SemanticConflict)?;
    let record = super::load_record_with_proof(tx, &run.id, proof)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?
        .ok_or(Error::MissingEvidence)?;
    super::recovery::receipt_precharge(tx, request, proof).await?;
    if super::lookup_receipt(tx, &request.session_id, &request.key)
        .await
        .map_err(super::unknown)?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::SemanticConflict);
    }
    super::validate_binding_with_proof(tx, &record, &run, proof).await?;
    record.verify_claim(request, &run)?;
    if run.status != crabber_core::RunStatus::Running {
        return Err(Error::SemanticConflict);
    }
    if run.lease_until > now {
        return Err(Error::LiveLease);
    }
    let sequence = super::super::native_parent::current_issuance_with_proof(tx, &run, proof)
        .await
        .map_err(|_| Error::SemanticConflict)?
        .sequence;
    proof.charge(8).map_err(|_| Error::UnknownStoreFailure)?;
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
    let binding =
        super::super::execution_binding::read_with_proof(tx, &request.session_id, &run.id, proof)
            .await
            .map_err(|_| Error::SemanticConflict)?
            .ok_or(Error::MissingEvidence)?;
    Ok((revision, sequence, binding.capsule))
}

pub(in crate::postgres) async fn validate_source_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    source: &AdmissionRecoveryPreparation,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    crate::native_enrollment::caller::measure_recovery_source(source, proof)
        .map_err(|_| Error::UnknownStoreFailure)?;
    source.fingerprint()?;
    let (_, sequence, binding) = context_with_proof(tx, source.request(), now, proof).await?;
    source.verify_binding(&binding)?;
    if sequence != source.expected_issuance() {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
