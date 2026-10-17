//! Fresh successor effects require protected current native and owner membership.
use super::{super::proof, source};
use super::{Error, Proof, RegisterSuccessorExecutionDriver, unknown};
use crate::session_owner::{SessionOwnerState, driver::SuccessorDriverLink};
use crabber_core::{Run, Session, SessionRevision};
use sqlx::{Row, Sqlite, Transaction};
pub(in crate::sqlite::session_owner::drivers) async fn fresh(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RegisterSuccessorExecutionDriver,
    expected: Option<SessionRevision>,
    now: time::OffsetDateTime,
    budget: &mut Proof,
) -> Result<SuccessorDriverLink, Error> {
    request.fingerprint()?;
    let session = request.context.session_id();
    let id = &request.fence.run_id;
    proof::membership(tx, session, budget).await?;
    let run = run(tx, request, now, budget).await?;
    let latest = crate::sqlite::native_parent::source_protocol::current(tx, session, id, budget)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    latest.verify(&run).map_err(unknown)?;
    let binding = crate::sqlite::execution_binding::read_with_proof(tx, session, id, budget)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    if run.config_hash != binding.capsule.config_hash.as_str()
        || run.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let selected = Box::pin(source::original(
        tx,
        request,
        budget,
        Some(&binding.capsule),
    ))
    .await?;
    if latest.sequence != selected.head
        || crate::native_parent::issuance_digest(&latest).map_err(unknown)? != selected.head_digest
    {
        return Err(Error::StaleOwner);
    }
    if let Some((continuation, parent)) = &selected.approval {
        super::approval::check(tx, &run, continuation, parent, now, budget).await?;
    }
    if let Some((continuation, parent)) = &selected.tool {
        super::tool::check(
            tx,
            &run,
            continuation,
            parent,
            selected
                .tool_receipt
                .as_ref()
                .ok_or(Error::UnknownStoreOutcome)?,
            now,
            budget,
        )
        .await?;
    }
    let original = super::super::proof::audit(tx, session, &request.claim.root_join_id, budget)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    for audit in [&original, &selected.child_begin] {
        crate::sqlite::session_owner::protocol::require(
            tx,
            session,
            &audit.record.snapshot.target.id,
            budget,
        )
        .await
        .map_err(unknown)?;
    }
    let root = proof::current(tx, session, &original.record.snapshot.target.id, budget).await?;
    let child = proof::current(
        tx,
        session,
        &selected.child_begin.record.snapshot.target.id,
        budget,
    )
    .await?;
    if root != original.record
        || child.snapshot.target != selected.child_begin.record.snapshot.target
        || child.snapshot.state != SessionOwnerState::Active
        || child.resource != selected.child_begin.record.resource
        || child.resource.is_none()
        || child.owned_scope.is_none()
        || child.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    if child.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    context(tx, request, expected.unwrap_or(selected.revision), budget).await?;
    if latest.completion_source.as_ref().is_some_and(|source| {
        source.native_operation == crate::native_parent::CompletionOperation::AdmissionRecovery
    }) {
        super::super::fresh::keyed_journal(tx, &run, &binding, budget).await?;
    }
    Ok(selected.link)
}
async fn run(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RegisterSuccessorExecutionDriver,
    now: time::OffsetDateTime,
    budget: &mut Proof,
) -> Result<Run, Error> {
    proof::charge(
        tx,
        RUN_COST,
        &request.context.session_id().0,
        &request.fence.run_id.0,
        budget,
    )
    .await?;
    let row = sqlx::query(RUN)
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let run: Run = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if run.id != request.fence.run_id
        || run.session_id != *request.context.session_id()
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(&run.claim_token)
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(match run.status {
                crabber_core::RunStatus::Running => "running",
                crabber_core::RunStatus::Paused => "paused",
                _ => return Err(Error::InvalidTransition),
            })
        || crate::sqlite::records::micros(run.lease_until)
            != row.try_get::<i64, _>("lease_until").map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.claim_token != request.fence.claim_token || run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    Ok(run)
}
async fn context(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RegisterSuccessorExecutionDriver,
    expected: SessionRevision,
    budget: &mut Proof,
) -> Result<(), Error> {
    proof::charge(
        tx,
        SESSION_COST,
        &request.context.session_id().0,
        &request.fence.run_id.0,
        budget,
    )
    .await?;
    let row = sqlx::query(SESSION)
        .bind(&request.context.session_id().0)
        .bind(&request.fence.run_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let session: Session = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if session.id != *request.context.session_id()
        || session.workspace_id != request.context.workspace_id()
        || session.directory != request.context.directory()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if u64::try_from(row.try_get::<i64, _>("revision").map_err(unknown)?).map_err(unknown)?
        != expected.get()
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
const RUN_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+length(CAST(claim_token AS BLOB))+length(CAST(status AS BLOB))+8 AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const RUN: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data,CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,lease_until FROM runs WHERE session_id=$1 AND id=$2";
const SESSION_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+8 AS BIGINT) FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const SESSION: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=16384 THEN data END AS data,public_revision AS revision FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
