//! Read-only staged proof, sharing one allowance including original coverage.
use crate::deletion::coverage::VerifiedDeletionCoverage;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES, Readiness, unknown};
use crate::transaction::Transaction;
use crate::{DeletionBlocker, DeletionError, sql_deletion::VerifiedScope};
use crabber_core::SessionId;
use sqlx::Sqlite;
mod domains;
mod native;

pub(super) async fn readiness(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
) -> std::result::Result<Readiness, DeletionError> {
    match check(tx, session, coverage).await {
        Ok(scope) => Ok(Readiness::Ready(scope)),
        Err(Failure::Blocked(reason)) => Ok(Readiness::Blocked(reason)),
        Err(Failure::Error(error)) => Err(error),
    }
}
async fn check(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
) -> crate::sql_deletion::readiness::Result<VerifiedScope> {
    if coverage.session_id() != session {
        return Err(unknown("coverage scope"));
    }
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    proof.charge(coverage.proof_bytes())?;
    let revision: Option<i64> =
        sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?;
    let revision = revision.ok_or(Failure::Error(DeletionError::NotFound))?;
    if u64::try_from(revision).map_err(unknown)? != coverage.revision().get() {
        return Err(Failure::Error(DeletionError::RevisionConflict));
    }
    proof.charge(8)?;
    let native_protocol: i32 =
        sqlx::query_scalar("SELECT CAST(native_run_protocol AS INTEGER) FROM sessions WHERE id=$1")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    let cross_scope: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if cross_scope {
        return Err(unknown("relational scope"));
    }
    let unproven_run_sources = native::runtime(tx, session, &mut proof).await?;
    super::super::fork::verify_imported_history(tx, session, &mut proof).await?;
    let unknown_native = native::attempts(tx, session, &mut proof).await?;
    let unknown_calls = native::phases(tx, session, &mut proof).await?;
    domains::continuations(tx, session, &mut proof).await?;
    native::released_suspensions(tx, session, &mut proof).await?;
    domains::queue(tx, session, &mut proof).await?;
    proof.charge(1)?;
    let unproven_queue: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND source_protocol=0)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    domains::pending_input(tx, session).await?;
    domains::controls(tx, session, &mut proof).await?;
    let unknown_owner = domains::owners(tx, session, &mut proof).await?;
    // Authenticated native history still requires a fixed known whole-Run
    // Runtime resource policy. The retained-Run gate remains conservative.
    if native_protocol != 1
        || unproven_run_sources
        || unproven_queue
        || unknown_native
        || unknown_calls
        || unknown_owner
    {
        return Err(Failure::Blocked(DeletionBlocker::OwnershipUnknown));
    }
    Ok(VerifiedScope::from_coverage(coverage))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod imported_tests;
