//! Queue's final completed R comes from its actual protected publication.
use super::super::{Error, source::Original};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{PreparationKey, PreparationPurpose, queue_admission};
use sqlx::Postgres;
pub(super) async fn revision(
    tx: &mut Transaction<'_, Postgres>,
    original: &Original,
    proof: &mut Proof,
) -> Result<crabber_core::SessionRevision, Error> {
    let origin = original.audits[0]
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if !matches!(origin.purpose, PreparationPurpose::Queue { .. }) {
        return Ok(original.witness.admitted_revision());
    }
    let PreparationKey::Queue(id) = &origin.key else {
        return Err(Error::UnknownStoreOutcome);
    };
    let prefix =
        crate::postgres::queue::protocol::admission_prefix(tx, &original.session.id, id, proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::UnknownStoreOutcome)?;
    queue_admission::publication_revision(
        &prefix,
        &queue_admission::AdmissionSources {
            session: &original.session,
            initial: &original.initial,
            witness: &original.witness,
            audits: original.audits(),
            init: original.init.as_ref().ok_or(Error::UnknownStoreOutcome)?,
        },
    )
    .map_err(|error| match error {
        crate::session_owner::preparation::PreparationCommitError::Owner(error) => error,
        _ => Error::UnknownStoreOutcome,
    })
}
