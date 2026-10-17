//! Actual expired keyed admission and original native publisher output.
use super::{Error, PostgresStore};
use crate::native_enrollment::caller::{
    Caller, Fresh, initial::Capture, recovery::AdmissionRecovery,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::{ClaimUnstartedAdmissionRequest, ClaimedAdmission};
use crabber_core::{Run, RunFence};
use sqlx::Postgres;
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreFailure
}
pub(in crate::postgres) async fn claim(
    store: &PostgresStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<ClaimedAdmission, Error> {
    claim_with_proof(store, request, &mut Proof::bounded(32 << 20)).await
}
pub(in crate::postgres) async fn claim_with_proof(
    store: &PostgresStore,
    request: ClaimUnstartedAdmissionRequest,
    proof: &mut Proof,
) -> Result<ClaimedAdmission, Error> {
    crate::native_enrollment::caller::recovery::measure_request(&request, proof)
        .map_err(|_| Error::UnknownStoreFailure)?;
    let (mut tx, run) = store
        .run_transaction_with_proof(&request.expected_fence.run_id, proof)
        .await
        .map_err(unknown)?;
    // These original guards materialize only three server-side boolean results.
    proof.charge(3).map_err(|_| Error::UnknownStoreFailure)?;
    super::super::session_owner::guard_native_claim(&mut tx, &request.session_id)
        .await
        .map_err(|error| match error {
            crate::session_owner::SessionOwnerError::Busy => Error::PreparationBusy,
            _ => Error::UnknownStoreFailure,
        })?;
    #[cfg(test)]
    let run_id = request.expected_fence.run_id.clone();
    let value = claim_in_transaction(store, &mut tx, run, request, None, proof).await?;
    tx.commit().await.map_err(|_| Error::UnknownStoreFailure)?;
    #[cfg(test)]
    if store.native_raw_probe.after_admission_recovery(&run_id) {
        return Err(Error::UnknownStoreFailure);
    }
    Ok(value)
}
pub(in crate::postgres) async fn claim_in_transaction(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    mut run: Run,
    request: ClaimUnstartedAdmissionRequest,
    completion: Option<crate::native_parent::CompletionSource>,
    proof: &mut Proof,
) -> Result<ClaimedAdmission, Error> {
    crate::native_enrollment::caller::recovery::measure_request(&request, proof)
        .map_err(|_| Error::UnknownStoreFailure)?;
    proof
        .measure(&(&run, &completion))
        .map_err(|_| Error::UnknownStoreFailure)?;
    let previous = run.clone();
    let record = super::load_record_with_proof(tx, &run.id, proof)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?
        .ok_or(Error::MissingEvidence)?;
    receipt_precharge(tx, &request, proof).await?;
    if super::lookup_receipt(tx, &request.session_id, &request.key)
        .await
        .map_err(unknown)?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::SemanticConflict);
    }
    super::validate_binding_with_proof(tx, &record, &run, proof).await?;
    record.verify_claim(&request, &run)?;
    let now = store.clock.now();
    if run.lease_until > now {
        return Err(Error::LiveLease);
    }
    let lease = time::Duration::try_from(request.lease).map_err(|_| Error::SemanticConflict)?;
    if lease <= time::Duration::ZERO {
        return Err(Error::SemanticConflict);
    }
    run.owner.clone_from(&request.owner);
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
    run.updated_at = now;
    let published = super::super::save_run_publication_with_proof(
        tx,
        &run,
        completion.as_ref(),
        proof,
        Capture::Original,
    )
    .await
    .map_err(unknown)?;
    let outcome = ClaimedAdmission {
        record,
        fence: RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        },
        run,
    };
    let publication = published
        .updated
        .as_ref()
        .ok_or(Error::UnknownStoreFailure)?;
    super::super::native_enrollment::record_raw_native(
        tx,
        Caller::Raw,
        &Fresh::AdmissionRecovery(AdmissionRecovery {
            request: &request,
            previous: &previous,
            result: &outcome,
            completion: completion.as_ref(),
            publication,
        }),
        proof,
        #[cfg(test)]
        &store.native_raw_probe,
    )
    .await
    .map_err(|_| Error::UnknownStoreFailure)?;
    Ok(outcome)
}
pub(super) async fn receipt_precharge(
    tx: &mut Transaction<'_, Postgres>,
    request: &ClaimUnstartedAdmissionRequest,
    proof: &mut Proof,
) -> Result<(), Error> {
    super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2")
        .bind(&request.session_id.0).bind(request.key.as_str()), proof).await.map_err(|_| Error::UnknownStoreFailure)
}
