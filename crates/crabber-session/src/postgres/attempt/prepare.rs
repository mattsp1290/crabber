//! Actual Attempt row preparation retains the caller's selected proof.
use super::records;
use crate::public_session::v2::proof::Proof;
use crate::{AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation, attempt};
use crabber_core::Run;
use sqlx::{Postgres, Transaction};
pub(super) async fn record(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &AttemptMutation,
    authority: Option<&AttemptAuthority>,
    now: time::OffsetDateTime,
    concluding: bool,
    read_proof: &mut Proof,
) -> Result<(attempt::AttemptRecord, bool), Error> {
    let insert = matches!(
        request.command,
        AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
    );
    let id = &request.command.target().id;
    let (record, grant) = if insert {
        if records::exists(tx, id).await? {
            return Err(Error::SemanticConflict);
        }
        let latest = records::load_with_proof(tx, &run.id, None, read_proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let record = attempt::AttemptRecord::reserve(
            run,
            request,
            latest.as_ref().map(|(record, _)| &record.snapshot),
        )?;
        if latest.is_some() && !records::quiet(tx, &run.id).await? {
            return Err(Error::CleanupBlocked);
        }
        (record, true)
    } else {
        let (mut record, _) = records::load_with_proof(tx, &run.id, Some(id), read_proof)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?
            .ok_or(Error::NotFound)?;
        let quiet = if concluding || matches!(request.command, AttemptCommand::RecordCleanup { .. })
        {
            records::quiet(tx, &run.id).await?
        } else {
            true
        };
        let grant = record.change(run, request, authority, now, quiet)?;
        if concluding {
            record.cleanup(crate::AttemptCleanupResult::Complete, quiet)?;
        }
        (record, grant)
    };

    Ok((record, grant))
}
