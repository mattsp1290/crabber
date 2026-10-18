//! Read-only staged proof, sharing one allowance including original coverage.
use crate::deletion::coverage::VerifiedDeletionCoverage;
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Failure, MAX_PROOF_BYTES, Readiness, unknown};
use crate::{DeletionBlocker, DeletionError, sql_deletion::VerifiedScope};
use crabber_core::SessionId;
use sqlx::Postgres;
mod domains;
mod native;

pub(super) async fn readiness(
    tx: &mut Transaction<'_, Postgres>,
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
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
) -> crate::sql_deletion::readiness::Result<VerifiedScope> {
    let mut proof = Proof::bounded(MAX_PROOF_BYTES);
    check_with_proof(tx, session, coverage, &mut proof).await
}
async fn check_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    coverage: &VerifiedDeletionCoverage,
    proof: &mut Proof,
) -> crate::sql_deletion::readiness::Result<VerifiedScope> {
    if coverage.session_id() != session {
        return Err(unknown("coverage scope"));
    }
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
    // Pre-Run original owners survive independently of Session/Run rows. Even
    // a closed first-producer relation has no whole-resource policy yet.
    proof.charge(1)?;
    let pending_initial: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pending_initial_cohorts WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_epochs WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_source_slots WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_registered_completes WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_assignments WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_closes WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_publications WHERE session_id=$1 UNION ALL SELECT 1 FROM pending_initial_heads WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_origins WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_cohorts WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_assignments WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_calls WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_results WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_roster WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_reverse WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_publications WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_heads WHERE session_id=$1 UNION ALL SELECT 1 FROM initial_consumer_closes WHERE session_id=$1)")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    let unproven_run_sources = native::runtime(tx, session, proof).await?;
    super::super::fork::verify_imported_history(tx, session, proof).await?;
    let unknown_native = native::attempts(tx, session, proof).await?;
    let unknown_calls = native::phases(tx, session, proof).await?;
    domains::continuations(tx, session, proof).await?;
    native::released_suspensions(tx, session, proof).await?;
    domains::queue(tx, session, proof).await?;
    proof.charge(1)?;
    let unproven_queue: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM future_queue WHERE session_id=$1 AND source_protocol=0)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    domains::pending_input(tx, session).await?;
    domains::controls(tx, session, proof).await?;
    let unknown_owner = domains::owners(tx, session, proof).await?;
    // Authenticated native history still requires a fixed known whole-Run
    // Runtime resource policy. The retained-Run gate remains conservative.
    if pending_initial
        || native_protocol != 1
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

#[cfg(test)]
pub(super) mod classifier_tests;
