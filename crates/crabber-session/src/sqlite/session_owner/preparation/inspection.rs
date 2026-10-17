//! Read-only original association; no completion/replay writer or current Run.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{
        JoinedPreparation, PreparationAdmissionReceipt, PreparationCommitError, inspection,
    },
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl super::super::SqliteStore {
    pub(in crate::sqlite) async fn inspect_initial_admission(
        &self,
        original: &JoinedPreparation,
    ) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
        self.inspect_initial_admission_with_proof(
            original,
            &mut Proof::bounded(inspection::MAX_PROOF),
        )
        .await
    }
    pub(in crate::sqlite) async fn inspect_initial_admission_with_proof(
        &self,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
        let session = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id;
        crate::session_owner::validation::identifier(&session.0, 512, false)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;

        crate::sqlite::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
        let result = read(&mut tx, original, proof).await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
}
async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<Option<PreparationAdmissionReceipt>, PreparationCommitError> {
    let bound = inspection::original(original, proof)?;
    let grant = original.observed_grant();
    let metadata = grant.reconciliation()?;
    let bytes: Option<i64> = sqlx::query_scalar(COST)
        .bind(&metadata.session_id.0)
        .bind(&metadata.join_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    let Some(bytes) = bytes else {
        proof.charge(1).map_err(|_| Error::Capacity)?;
        let committed: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2 UNION ALL SELECT 1 FROM session_owner_publications WHERE session_id=$1 AND operation_id=$2)")
            .bind(&metadata.session_id.0).bind(&metadata.join_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
        return if committed {
            Err(Error::UnknownStoreOutcome.into())
        } else {
            Ok(None)
        };
    };
    proof
        .charge(usize::try_from(bytes).map_err(unknown)?)
        .map_err(|_| Error::Capacity)?;
    let raw: Option<String> = sqlx::query_scalar(RUN)
        .bind(&metadata.session_id.0)
        .bind(&metadata.join_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let run = crabber_core::RunId(raw.ok_or(Error::UnknownStoreOutcome)?);
    if !crate::sqlite::native_parent::source_protocol::protocol(
        tx,
        &metadata.session_id,
        &run,
        proof,
    )
    .await
    .map_err(|error| inspection::read_error(&error.map_domain(|_| Error::UnknownStoreOutcome)))?
    {
        return Err(Error::Unsupported.into());
    }
    let source =
        super::super::drivers::source::original_read(tx, &metadata.session_id, &run, proof)
            .await
            .map_err(|error| inspection::read_error(&error))?;
    if source.witness.join_id()? != metadata.join_id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    source
        .witness
        .replay(
            grant,
            &source.session,
            &source.initial,
            &source.audits(),
            &bound.binding,
        )
        .map(Some)
        .map_err(Into::into)
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const COST: &str = "SELECT CAST(length(CAST(run_id AS BLOB)) AS BIGINT) FROM preparation_admission_witnesses WHERE session_id=$1 AND root_join=$2";
const RUN: &str = "SELECT CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END FROM preparation_admission_witnesses WHERE session_id=$1 AND root_join=$2";

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod tests;
