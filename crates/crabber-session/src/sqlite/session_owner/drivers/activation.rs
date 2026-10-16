//! Fresh typed driver dispatch checks before any effect permit is published.
use super::super::{Error, Record};
use super::{fresh, proof};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerAuthority,
    owned_scope::{OwnedScopeActivation, OwnedScopeParentAuthority},
};
use sqlx::{Sqlite, Transaction};
pub(super) async fn record(
    tx: &mut Transaction<'_, Sqlite>,
    request: &OwnedScopeActivation,
    authority: &SessionOwnerAuthority,
    parent: Option<&OwnedScopeParentAuthority>,
    now: time::OffsetDateTime,
    budget: &mut Proof,
) -> Result<Record, Error> {
    proof::membership(tx, request.session_id(), budget).await?;
    super::super::protocol::require(tx, request.session_id(), &request.target().id, budget)
        .await
        .map_err(super::super::unknown)?;
    let mut record = proof::current(tx, request.session_id(), &request.target().id, budget).await?;
    let reserve = proof::audit(tx, request.session_id(), &request.source.reserve, budget)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let begin = proof::audit(tx, request.session_id(), &request.source.begin, budget)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::session_owner::owned_scope::source::verify(&request.source, &record, &reserve, &begin)?;
    if record.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    let Some(crate::session_owner::audit::RevisionState::Present(expected)) =
        begin.publication.map(|publication| publication.after)
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    match parent {
        Some(OwnedScopeParentAuthority::ExecutionDriver(grant)) => {
            let actual = record.driver.as_deref().ok_or(Error::UnknownStoreOutcome)?;
            if actual.identity()? != *grant.source_identity()
                || grant.authority().target() != &record.snapshot.target
            {
                return Err(Error::SemanticConflict);
            }
            let fresh = Box::pin(fresh::fresh(
                tx,
                grant.original_request(),
                Some(expected),
                now,
                budget,
            ))
            .await?;
            if &fresh != actual {
                return Err(Error::StaleOwner);
            }
        }
        Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant)) => {
            let actual = record
                .successor_driver
                .as_deref()
                .ok_or(Error::UnknownStoreOutcome)?;
            if actual.identity()? != *grant.source_identity()
                || grant.authority().target() != &record.snapshot.target
            {
                return Err(Error::SemanticConflict);
            }
            let fresh = Box::pin(super::successor::fresh::fresh(
                tx,
                grant.original_request(),
                Some(expected),
                now,
                budget,
            ))
            .await?;
            if &fresh != actual {
                return Err(Error::StaleOwner);
            }
        }
        _ => return Err(Error::InvalidRequest),
    }
    crate::session_owner::owned_scope::source::activate(&mut record, request, authority, now)?;
    Ok(record)
}
