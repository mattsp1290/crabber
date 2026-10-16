//! Fresh first-driver eligibility is a narrow immutable source check.
use super::super::{Error, unknown};
use super::{proof, source};
use crate::{
    public_session::v2::proof::Proof,
    session_owner::driver::{DriverLink, RegisterExecutionDriver},
};
use crabber_core::{Run, SessionRevision};
use sqlx::{Row, Sqlite, Transaction};
pub(super) async fn fresh(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RegisterExecutionDriver,
    expected: Option<SessionRevision>,
    now: time::OffsetDateTime,
    budget: &mut Proof,
) -> Result<DriverLink, Error> {
    request.fingerprint()?;
    let session = request.context.session_id();
    let id = &request.fence.run_id;
    proof::membership(tx, session, budget).await?;
    proof::charge(tx, RUN_COST, &session.0, &id.0, budget).await?;
    let row = sqlx::query(RUN)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let raw = row
        .try_get::<Option<String>, _>("data")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    // This is the actual stored pristine Run, never a reconstructed checkpoint.
    // SQL refuses non-null checkpoint/error before the JSON can transfer.
    let run: Run = serde_json::from_str(&raw).map_err(unknown)?;
    if run.status != crabber_core::RunStatus::Running
        || run.id != *id
        || run.session_id != *session
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(&run.claim_token)
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some("running")
        || super::super::super::records::micros(run.lease_until)
            != row.try_get::<i64, _>("lease_until").map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    let latest =
        super::super::super::admission_journal::issuance_with_proof(tx, session, id, None, budget)
            .await
            .map_err(unknown)?;
    latest.verify(&run).map_err(unknown)?;
    if latest.sequence != 1 || latest.version != 2 || latest.ownership_handoff.is_none() {
        return Err(Error::Unsupported);
    }
    let binding = super::super::super::execution_binding::read_with_proof(tx, session, id, budget)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    Box::pin(pristine(tx, &run, &binding, budget)).await?;
    let original = source::original(tx, session, id, budget).await?;
    let declared = original.audits[0]
        .record
        .preparation
        .as_ref()
        .and_then(|origin| origin.resource_contract.as_ref());
    if declared.is_none()
        || crate::session_owner::preparation::resource_contract(&binding.capsule.runtime_semantics)?
            .as_ref()
            != declared
    {
        return Err(Error::Unsupported);
    }
    // SQL17 has no independent per-owner publication roster. This strict fresh
    // revision fence detects erased late cancellation plus restored current rows.
    // Metadata replay bypasses this gate; unrelated public mutations may refuse.
    revision(
        tx,
        session,
        expected.unwrap_or(original.witness.admitted_revision()),
        budget,
    )
    .await?;
    let child = proof::current(
        tx,
        session,
        &original.audits[4].receipt.owner.target.id,
        budget,
    )
    .await?;
    if child.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    if child.snapshot.target != original.audits[4].receipt.owner.target
        || child.snapshot.state != crate::session_owner::SessionOwnerState::Active
        || child.resource != original.audits[4].record.resource
        || child.resource.is_none()
        || child.owned_scope.is_none()
    {
        return Err(Error::StaleOwner);
    }
    DriverLink::initial(
        request,
        &original.session,
        &run,
        &original.initial,
        &original.witness,
        &original.audits(),
        &original.init,
    )
}
async fn pristine(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    budget: &mut Proof,
) -> Result<(), Error> {
    let session = &run.session_id;
    let id = &run.id;
    let count = crate::attempt::pristine::input_count(run, binding)
        .map_err(|_| Error::InvalidTransition)?;
    budget.charge(16).map_err(unknown)?;
    let count_actual: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) AS prefix").bind(&session.0).bind(&id.0).bind(i64::try_from(count+1).map_err(unknown)?).fetch_one(&mut **tx).await.map_err(unknown)?;
    let dirty: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM events WHERE session_id=$1 AND run_id=$2) OR EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$2) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$2) OR EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$2) OR EXISTS(SELECT 1 FROM epochs WHERE session_id=$1 AND run_id=$2 AND id<>$3)").bind(&session.0).bind(&id.0).bind(&run.epoch_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if dirty || count_actual != i64::try_from(count).map_err(unknown)? {
        return Err(Error::InvalidTransition);
    }
    let record = super::super::super::admission_execution::load_record_with_proof(tx, id, budget)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    if record.state != crate::AdmissionExecutionState::Unstarted {
        return Err(Error::InvalidTransition);
    }
    if record.capsule.request != binding.capsule.request
        || record.capsule.runtime_semantics != binding.capsule.runtime_semantics
        || record.capsule.input_envelope != binding.capsule.input_envelope
        || record.capsule.config_hash != binding.capsule.config_hash.as_str()
        || record.capsule.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
        || record.capsule.behavior_fingerprint != binding.capsule.behavior_fingerprint
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

async fn revision(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    expected: SessionRevision,
    budget: &mut Proof,
) -> Result<(), Error> {
    budget.charge(8).map_err(unknown)?;
    let actual: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if u64::try_from(actual).map_err(unknown)? != expected.get() {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
const RUN_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+length(CAST(claim_token AS BLOB))+length(CAST(status AS BLOB))+8 AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const RUN: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=32768 AND COALESCE(json_type(data,'$.checkpoint'),'null')='null' AND COALESCE(json_type(data,'$.error'),'null')='null' THEN data END AS data,CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,lease_until FROM runs WHERE session_id=$1 AND id=$2";
