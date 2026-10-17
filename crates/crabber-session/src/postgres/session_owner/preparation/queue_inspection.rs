//! Pure historical Queue association, within one read snapshot and one Proof.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError, inspection, queue_admission},
};
use crate::{QueueReceipt, postgres::PostgresStore};
impl PostgresStore {
    pub(in crate::postgres) async fn inspect_queue_admission(
        &self,
        original: &JoinedPreparation,
    ) -> Result<Option<QueueReceipt>, PreparationCommitError> {
        self.inspect_queue_admission_with_proof(
            original,
            &mut Proof::bounded(inspection::MAX_PROOF),
        )
        .await
    }
    pub(in crate::postgres) async fn inspect_queue_admission_with_proof(
        &self,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Option<QueueReceipt>, PreparationCommitError> {
        queue_admission::original(original, proof)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let result = Box::pin(read(&mut tx, original, proof)).await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
}
async fn read(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<Option<QueueReceipt>, PreparationCommitError> {
    let crate::session_owner::preparation::PreparedOperation::Queue { request, .. } =
        original.observed_operation()
    else {
        return Err(Error::Unsupported.into());
    };
    crate::postgres::deletion::ensure_live(tx, &request.session_id)
        .await
        .map_err(crate::deletion::owner_error)?;
    let Some(prefix) = crate::postgres::queue::protocol::admission_prefix(
        tx,
        &request.session_id,
        &request.id,
        proof,
    )
    .await
    .map_err(|error| match error {
        crate::public_session::v2::proof::ReadError::Budget(
            crate::public_session::v2::proof::Failure::Limited(_),
        ) => Error::Capacity,
        crate::public_session::v2::proof::ReadError::Domain(crate::StoreError::HostUnsupported) => {
            Error::Unsupported
        }
        _ => Error::UnknownStoreOutcome,
    })?
    else {
        let join = original.observed_grant().reconciliation()?.join_id;
        proof
            .measure(&(&request.session_id, &join))
            .map_err(|_| Error::Capacity)?;
        let committed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2 UNION ALL SELECT 1 FROM preparation_admission_witnesses WHERE session_id=$1 AND root_join=$2)")
            .bind(&request.session_id.0).bind(&join.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        return if committed {
            Err(Error::UnknownStoreOutcome.into())
        } else {
            Ok(None)
        };
    };
    let (enqueue, prepare, admit) = queue_admission::select(&prefix)?;
    let actual = queue_admission::prepared(original, enqueue, prepare, admit)?;
    let receipt = &admit.receipt;
    let run = &receipt
        .summary
        .admission
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?
        .run_id;
    if !crate::postgres::native_parent::source_protocol::protocol(
        tx,
        &request.session_id,
        run,
        proof,
    )
    .await
    .map_err(unknown)?
    {
        return Err(Error::Unsupported.into());
    }
    let source = super::super::drivers::source::original_read(tx, &request.session_id, run, proof)
        .await
        .map_err(|error| inspection::read_error(&error))?;
    queue_admission::verify(
        original,
        &actual,
        receipt,
        &queue_admission::AdmissionSources {
            session: &source.session,
            initial: &source.initial,
            witness: &source.witness,
            audits: source.audits(),
            init: source.init.as_ref().ok_or(Error::UnknownStoreOutcome)?,
        },
    )?;
    Ok(Some(receipt.clone()))
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

#[cfg(test)]
#[path = "queue_inspection_tests.rs"]
mod tests;
