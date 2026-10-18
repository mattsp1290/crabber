//! Closed first-Pending operations under the original native Session gate.
mod operations;
mod storage;
mod write;
use crate::native_enrollment::pending_initial::{Assignment, SourceKey};
use crate::public_session::v2::proof::{Failure, Proof, ReadError};
use crate::session_owner::{
    SessionOwnerError as Error,
    owned_scope::{PendingInitialNativeCall, PendingInitialNativeReply},
    preparation::{PreparationCommitError, inspection},
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl crate::sqlite::SqliteStore {
    pub(in crate::sqlite) async fn pending_initial_native_impl(
        &self,
        call: PendingInitialNativeCall<'_>,
    ) -> Result<PendingInitialNativeReply, PreparationCommitError> {
        // Local original-task/join admission precedes native-future construction;
        // the SQL worker is authenticated by original sources+gate, not task identity.
        let mut parts: crate::session_owner::owned_scope::NativeInitialParts<'_> =
            call.into_native()?;

        let mut tx = crate::transaction::begin_with(&self.writer, "BEGIN IMMEDIATE")
            .await
            .map_err(unknown)?;
        let result = Box::pin(operations::apply(self, &mut tx, &mut parts)).await;
        match result {
            Ok(reply) => {
                tx.commit().await.map_err(unknown)?;
                #[cfg(test)]
                if self
                    .session_owner_fault
                    .load(std::sync::atomic::Ordering::SeqCst)
                    == 2
                {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                Ok(reply)
            }
            Err(error) => {
                tx.rollback().await.map_err(unknown)?;
                Err(error)
            }
        }
    }
}
async fn publish_origin(
    tx: &mut Transaction<'_, Sqlite>,
    assignment: &Assignment,
    first: &crate::native_enrollment::caller::initial::FirstPublication,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    let origin = crate::native_enrollment::Origin::pending_birth(
        assignment,
        &first.issuer,
        &first.source,
        proof,
    )
    .map_err(|error| inspection::read_error(&error.map_domain(|_| Error::UnknownStoreOutcome)))?;
    let digest = origin.digest(proof).map_err(|error| {
        inspection::read_error(&error.map_domain(|_| Error::UnknownStoreOutcome))
    })?;
    proof.measure(&origin).map_err(|_| Error::Capacity)?;
    let data = serde_json::to_string(&origin).map_err(unknown)?;
    sqlx::query("INSERT INTO native_execution_origins(session_id,run_id,first_issuer_digest,origin_digest,birth_kind,unclassified_run_id,pending_epoch,data) VALUES($1,$2,$3,$4,'pending',NULL,$5,$6)")
  .bind(&origin.session_id.0).bind(&origin.run_id.0).bind(origin.first_issuer_digest.as_str()).bind(digest.as_str()).bind(&assignment.correlation.identity.epoch.0).bind(data).execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
fn budget(error: Failure) -> PreparationCommitError {
    inspection::read_error(&ReadError::Budget(error))
}

pub(super) async fn assignment_for_run(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<Assignment, Failure> {
    use sqlx::Row;
    let size:Option<i64>=sqlx::query_scalar("SELECT CAST(length(CAST(session_id AS BLOB))+length(CAST(source_key AS BLOB))+length(CAST(epoch_id AS BLOB)) AS BIGINT) FROM pending_initial_assignments WHERE run_id=$1").bind(&run.0).fetch_optional(&mut **tx).await.map_err(crate::public_session::v2::proof::unknown)?;
    proof.charge(
        usize::try_from(size.ok_or_else(|| {
            crate::public_session::v2::proof::unknown("missing Pending assignment")
        })?)
        .map_err(crate::public_session::v2::proof::unknown)?,
    )?;
    let row=sqlx::query("SELECT CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END AS session_id,CASE WHEN length(CAST(source_key AS BLOB))<=64 THEN source_key END AS source_key,CASE WHEN length(CAST(epoch_id AS BLOB))<=128 THEN epoch_id END AS epoch_id FROM pending_initial_assignments WHERE run_id=$1").bind(&run.0).fetch_one(&mut **tx).await.map_err(crate::public_session::v2::proof::unknown)?;
    let stored: Option<String> = row
        .try_get("session_id")
        .map_err(crate::public_session::v2::proof::unknown)?;
    if stored.as_deref() != Some(session.0.as_str()) {
        return Err(crate::public_session::v2::proof::unknown(
            "Pending assignment scope",
        ));
    }
    let key = SourceKey {
        session_id: session.clone(),
        identity: crabber_core::InputFingerprint::new(
            row.try_get::<Option<String>, _>("source_key")
                .map_err(crate::public_session::v2::proof::unknown)?
                .ok_or_else(|| crate::public_session::v2::proof::unknown("Pending source key"))?,
        )
        .map_err(crate::public_session::v2::proof::unknown)?,
    };
    let epoch = crabber_core::ControlId(
        row.try_get::<Option<String>, _>("epoch_id")
            .map_err(crate::public_session::v2::proof::unknown)?
            .ok_or_else(|| crate::public_session::v2::proof::unknown("Pending epoch"))?,
    );
    let state = storage::load(tx, &key, &epoch, proof).await?;
    state.view().verify(&key, proof)?;
    let assignment = state
        .assignment
        .ok_or_else(|| crate::public_session::v2::proof::unknown("Pending assignment"))?;
    if assignment.run_id != *run {
        return Err(crate::public_session::v2::proof::unknown(
            "Pending assignment Run",
        ));
    }
    proof.measure(&assignment)?;
    Ok(assignment)
}

/// A public Raw completion must never borrow a pending source slot, even on its
/// early replay route. Any retained relation member conservatively refuses.
pub(in crate::sqlite) async fn refuse_raw(
    tx: &mut Transaction<'_, Sqlite>,
    original: &crate::session_owner::preparation::JoinedPreparation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    // Only original initial admissions can acquire this source slot. Preserve
    // the existing Raw successor/recovery and legacy non-owned routes.
    if original.observed_registration().is_none()
        || !matches!(
            original.observed_grant().original_reservation().purpose,
            crate::session_owner::preparation::PreparationPurpose::OrdinaryAdmission
        )
        || !matches!(
            original.observed_operation(),
            crate::session_owner::preparation::PreparedOperation::Admit(_)
                | crate::session_owner::preparation::PreparedOperation::AdmitKeyed(_)
        )
    {
        return Ok(());
    }
    let key = SourceKey::expected(original, proof)?;
    proof.charge(1).map_err(|_| Error::Capacity)?;
    let present:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_initial_cohorts WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_epochs WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_source_slots WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_registered_completes WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_assignments WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_closes WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_publications WHERE session_id=$1 AND source_key=$2 UNION ALL SELECT 1 FROM pending_initial_heads WHERE session_id=$1 AND source_key=$2)").bind(&key.session_id.0).bind(key.identity.as_str()).fetch_one(&mut **tx).await.map_err(unknown)?;
    if present {
        return Err(Error::Unsupported.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
