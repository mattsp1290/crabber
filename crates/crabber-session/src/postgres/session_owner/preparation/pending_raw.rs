//! Pure original Raw completion correlation; no replay writer or current Run/head.
use crate::native_enrollment::caller::initial::{FirstPublication, SelectedOriginal};
use crate::native_enrollment::pending_initial::{
    ActualRawMetadata, Correlation, RawInitialReceipt,
};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError, PreparedOperation, inspection},
};
use sqlx::Postgres;
pub(in crate::postgres) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    original: &JoinedPreparation,
    expected: &Correlation,
    selected: &SelectedOriginal<'_>,
    proof: &mut Proof,
) -> Result<Option<ActualRawMetadata>, PreparationCommitError> {
    selected.verify(original, proof).map_err(budget)?;
    let metadata = original.observed_grant().reconciliation()?;
    let cost:Option<i64>=sqlx::query_scalar("SELECT CAST(octet_length(run_id) AS BIGINT) FROM preparation_admission_witnesses WHERE session_id=$1 AND root_join=$2")
  .bind(&metadata.session_id.0).bind(&metadata.join_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    proof.charge(8).map_err(|_| Error::Capacity)?;
    let Some(cost) = cost else {
        let committed:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2)")
   .bind(&metadata.session_id.0).bind(&metadata.join_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        if committed {
            return Err(Error::UnknownStoreOutcome.into());
        }
        if let PreparedOperation::AdmitKeyed(bound) = original.observed_operation() {
            let receipt:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admission_receipts WHERE session_id=$1 AND admission_key=$2)").bind(&metadata.session_id.0).bind(bound.keyed.options.key.as_str()).fetch_one(&mut **tx).await.map_err(unknown)?;
            if receipt {
                return Err(Error::SemanticConflict.into());
            }
        }
        return Ok(None);
    };
    proof
        .charge(usize::try_from(cost).map_err(unknown)?)
        .map_err(|_| Error::Capacity)?;
    let run:Option<String>=sqlx::query_scalar("SELECT CASE WHEN octet_length(run_id)<=128 THEN run_id END FROM preparation_admission_witnesses WHERE session_id=$1 AND root_join=$2")
  .bind(&metadata.session_id.0).bind(&metadata.join_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    let run = crabber_core::RunId(run.ok_or(Error::UnknownStoreOutcome)?);
    let status =
        super::super::super::native_enrollment::original(tx, &metadata.session_id, &run, proof)
            .await
            .map_err(budget)?;
    match status {
        crate::native_enrollment::Status::Pending => return Ok(None),
        crate::native_enrollment::Status::Legacy => return Err(Error::Unsupported.into()),
        crate::native_enrollment::Status::Unclassified => (),
    }
    let source =
        super::super::drivers::source::original_read(tx, &metadata.session_id, &run, proof)
            .await
            .map_err(|error| inspection::read_error(&error))?;
    if source.witness.join_id()? != metadata.join_id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    crate::session_owner::preparation::proof::verify_join_audit(
        original.observed_grant(),
        Some(source.audits().join),
    )?;
    let receipt = match original.observed_operation() {
        PreparedOperation::Admit(bound) => {
            return Box::pin(unkeyed_metadata(
                tx,
                original,
                source,
                &bound.binding,
                expected,
                proof,
            ))
            .await
            .map(Some);
        }
        PreparedOperation::AdmitKeyed(bound) => {
            keyed_receipt(tx, bound, &source, &run, expected, proof).await?
        }
        _ => return Err(Error::Unsupported.into()),
    };
    let journal = super::super::super::native_parent::source_protocol::source(
        tx,
        &metadata.session_id,
        &run,
        1,
        proof,
    )
    .await
    .map_err(|error| inspection::read_error(&error.map_domain(|_| Error::UnknownStoreOutcome)))?
    .ok_or(Error::UnknownStoreOutcome)?;
    ActualRawMetadata::from_original(
        expected,
        &FirstPublication {
            issuer: source.initial,
            source: journal,
        },
        RawInitialReceipt::Keyed(&receipt),
        proof,
    )
    .map(Some)
    .map_err(budget)
}
async fn unkeyed_metadata(
    tx: &mut Transaction<'_, Postgres>,
    original: &JoinedPreparation,
    source: super::super::drivers::source::Original,
    binding: &crate::ExecutionBindingCapsule,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<ActualRawMetadata, PreparationCommitError> {
    let receipt = source.witness.replay(
        original.observed_grant(),
        &source.session,
        &source.initial,
        &source.audits(),
        binding,
    )?;
    let journal = super::super::super::native_parent::source_protocol::source(
        tx,
        &source.initial.state.session_id,
        &source.initial.state.run_id,
        1,
        proof,
    )
    .await
    .map_err(|error| inspection::read_error(&error.map_domain(|_| Error::UnknownStoreOutcome)))?
    .ok_or(Error::UnknownStoreOutcome)?;
    ActualRawMetadata::from_original(
        expected,
        &FirstPublication {
            issuer: source.initial,
            source: journal,
        },
        RawInitialReceipt::Unkeyed(&receipt),
        proof,
    )
    .map_err(budget)
}

async fn keyed_receipt(
    tx: &mut Transaction<'_, Postgres>,
    bound: &crate::BoundKeyedAdmitRequest,
    source: &super::super::drivers::source::Original,
    run: &crabber_core::RunId,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<crabber_core::AdmissionReceipt, PreparationCommitError> {
    bound.binding.validate_keyed(&bound.keyed)?;
    let size:Option<i64>=sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2").bind(&expected.key.session_id.0).bind(bound.keyed.options.key.as_str()).fetch_optional(&mut **tx).await.map_err(unknown)?;
    proof
        .charge(usize::try_from(size.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?)
        .map_err(|_| Error::Capacity)?;
    let raw:Option<String>=sqlx::query_scalar("SELECT CASE WHEN octet_length(data::text)<=8192 THEN data::text END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2").bind(&expected.key.session_id.0).bind(bound.keyed.options.key.as_str()).fetch_one(&mut **tx).await.map_err(unknown)?;
    let receipt: crabber_core::AdmissionReceipt =
        serde_json::from_str(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    source
        .init
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?
        .verify_capsule(
            &receipt,
            &bound.keyed.options.key,
            bound.keyed.execution.as_ref().ok_or(Error::Unsupported)?,
        )?;
    if receipt.run_id != *run
        || receipt.session_id != expected.key.session_id
        || receipt.semantic_digest != bound.keyed.validated_digest()?
    {
        return Err(Error::SemanticConflict.into());
    }
    Ok(receipt)
}

fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
fn budget(error: crate::public_session::v2::proof::Failure) -> PreparationCommitError {
    inspection::read_error(&ReadError::Budget(error))
}
