//! Original compound carriers under the actual enclosing caller Proof.
use super::{Error, sql};
use crate::native_enrollment::caller::{
    Approval, ApprovalCompletion, ApprovalInput, ApprovalResult, Caller, Fresh,
};
use crate::public_session::v2::proof::Proof;
use sqlx::{Sqlite, Transaction};
pub(super) async fn digest(
    tx: &mut Transaction<'_, Sqlite>,
    input: ApprovalInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::InputFingerprint, Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE session_id=$1 AND operation_id=$2)",
    )
    .bind(&input.original().session_id.0)
    .bind(&input.id().0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::db)
    .map_err(sql::unknown)?;
    if !exists {
        input
            .measure(proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    input.fingerprint()
}
pub(super) async fn replay(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    id: &crabber_core::ControlId,
    digest: &crabber_core::InputFingerprint,
    proof: &mut Proof,
) -> Result<Option<crate::ApprovalReceipt>, Error> {
    let saved = super::receipt_with_proof(tx, &run.session_id, id, proof)
        .await
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?;
    if saved
        .as_ref()
        .is_some_and(|s| s.run_id != run.id || s.semantic_digest != *digest)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(saved)
}
pub(super) async fn observe(
    tx: &mut Transaction<'_, Sqlite>,
    caller: Caller,
    completed: ApprovalCompletion<'_>,
    outcome: &crate::ApprovalClaimOutcome,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<(), Error> {
    let crate::ApprovalClaimOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    let current = super::super::records::load_run_with_proof(tx, &completed.previous.id, proof)
        .await
        .map_err(sql::unknown)?;
    let (parent, _) = super::super::attempt::load_host_attempt_with_proof(
        tx,
        &current.id,
        Some(&completed.native_receipt.attempt.target.id),
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::UnknownStoreOutcome)?;
    let native_event = super::super::read_proof::event(
        tx,
        &current.session_id,
        completed.native_receipt.event_cursor,
        false,
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let event = super::super::read_proof::control_event(
        tx,
        &current.session_id,
        receipt.event_cursor,
        proof,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)?;
    super::super::native_enrollment::record_raw_native(
        tx,
        caller,
        &Fresh::Approval(Approval {
            input: completed.input,
            previous: completed.previous,
            activated: completed.activated,
            current: &current,
            record: completed.record,
            parent: &parent,
            native_request: completed.native_request,
            native_receipt: completed.native_receipt,
            native_event: &native_event,
            receipt,
            event: &event,
            grant,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .await
    .map_err(|_| Error::UnknownStoreOutcome)
}

pub(super) async fn issue(
    tx: &mut Transaction<'_, Sqlite>,
    caller: Caller,
    completed: ApprovalCompletion<'_>,
    parts: ApprovalResult,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<crate::ApprovalClaimOutcome, Error> {
    let outcome = completed
        .issue(parts, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    observe(
        tx,
        caller,
        completed,
        &outcome,
        proof,
        #[cfg(test)]
        probe,
    )
    .await?;
    Ok(outcome)
}
