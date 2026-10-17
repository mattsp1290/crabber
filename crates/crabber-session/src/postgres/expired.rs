//! Pure expired-run eligibility and original-source authentication.
#[path = "expired/unkeyed.rs"]
pub(super) mod unkeyed;
#[path = "expired/unstarted.rs"]
pub(super) mod unstarted;
use super::PostgresStore;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{ExpiredRunPreparation, InspectExpiredRunPreparation},
};
use crabber_core::{InputFingerprint, Run, RunStatus, SessionRevision};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn inspect(
    store: &PostgresStore,
    request: InspectExpiredRunPreparation,
) -> Result<ExpiredRunPreparation, Error> {
    request.validate()?;
    let mut tx = store
        .session_transaction(&request.session_id)
        .await
        .map_err(unknown)?;
    super::deletion::ensure_live(&mut tx, &request.session_id)
        .await
        .map_err(crate::deletion::owner_error)?;
    let result = context(&mut tx, request, store.clock.now())
        .await
        .and_then(|source| {
            crate::session_owner::preparation::expired::unstarted::validate_capture(&source)?;
            Ok(source)
        });
    tx.rollback().await.map_err(unknown)?;
    result
}
pub(super) async fn context(
    tx: &mut Transaction<'_, Postgres>,
    request: InspectExpiredRunPreparation,
    now: time::OffsetDateTime,
) -> Result<ExpiredRunPreparation, Error> {
    context_with_proof(
        tx,
        request,
        now,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}
pub(super) async fn context_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    request: InspectExpiredRunPreparation,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<ExpiredRunPreparation, Error> {
    crate::native_enrollment::caller::recovery::measure_expired_request(&request, proof)
        .map_err(unknown)?;
    request.validate()?;
    let run = load_run_with_proof(tx, &request, proof).await?;
    if run.error.is_some() {
        return Err(Error::UnknownStoreOutcome);
    }
    if run.claim_token != request.expected_fence.claim_token {
        return Err(Error::StaleOwner);
    }
    if run.lease_until > now {
        return Err(Error::LiveOwner);
    }
    proof.charge(1).map_err(unknown)?;
    if super::has_pending_continuation(tx, &run.session_id)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Busy);
    }
    let binding = super::execution_binding::read_with_proof(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    let issuer = super::native_parent::current_issuance_with_proof(tx, &run, proof)
        .await
        .map_err(unknown)?;
    let (native, origin) = match run.status {
        RunStatus::Paused => suspended(tx, &run, proof).await?,
        RunStatus::Running => initial(tx, &run, &binding, now, proof).await?,
        _ => return Err(Error::InvalidTransition),
    };
    proof.charge(8).map_err(unknown)?;
    let revision: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&run.session_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    let revision = SessionRevision::new(revision.try_into().map_err(unknown)?).map_err(unknown)?;
    let target = crate::session_owner::preparation::ExpiredRunTarget {
        issuance: issuer.sequence,
        fence_digest: issuer.state.token.clone(),
        issuer_digest: crate::native_parent::issuance_digest(&issuer).map_err(unknown)?,
        binding_digest: ExpiredRunPreparation::fingerprint_binding(&binding.capsule)?,
        native_source: native,
        origin,
        owner: request.owner.clone(),
        lease: request.lease,
    };
    proof.measure(&target).map_err(unknown)?;
    ExpiredRunPreparation::from_verified_source(request, revision, target)
}
async fn suspended(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    proof.charge(1).map_err(unknown)?;
    let excessive: bool = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM tool_phases WHERE run_id=$1 LIMIT 65) AS p)>64 OR (SELECT COUNT(*) FROM (SELECT 1 FROM tool_calls WHERE run_id=$1 LIMIT 65) AS c)>64")
        .bind(&run.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if excessive {
        return Err(Error::Capacity);
    }
    let original = super::run_control::suspension::verify_with_proof(tx, run, proof)
        .await
        .map_err(unknown)?
        .1;
    Ok((
        crate::session_owner::preparation::expired::suspension_digest(&original, run)?,
        crate::session_owner::preparation::ExpiredRunOrigin::Suspended(
            original.receipt.operation_id,
        ),
    ))
}
pub(super) async fn load_run_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    request: &InspectExpiredRunPreparation,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Run, Error> {
    super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2").bind(&request.session_id.0).bind(&request.expected_fence.run_id.0), proof).await.map_err(unknown)?;
    load_run(tx, request).await
}
pub(super) async fn load_run(
    tx: &mut Transaction<'_, Postgres>,
    request: &InspectExpiredRunPreparation,
) -> Result<Run, Error> {
    load_scoped_run(tx, &request.session_id, &request.expected_fence.run_id).await
}
pub(super) async fn load_scoped_run(
    tx: &mut Transaction<'_, Postgres>,
    session: &crabber_core::SessionId,
    run_id: &crabber_core::RunId,
) -> Result<Run, Error> {
    let row = sqlx::query(RUN)
        .bind(&session.0)
        .bind(&run_id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    if row.try_get::<Option<bool>, _>("valid").map_err(unknown)? != Some(true) {
        return Err(Error::UnknownStoreOutcome);
    }
    let text = row
        .try_get::<Option<String>, _>("body")
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let run: Run = serde_json::from_str(&text).map_err(unknown)?;
    if run.session_id != *session
        || run.id != *run_id
        || super::micros(run.lease_until)
            != row.try_get::<i64, _>("lease_until").map_err(unknown)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
async fn initial(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    now: time::OffsetDateTime,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    let initial = initial_provenance(tx, run, binding, proof).await?;
    unstarted::guard_with_proof(tx, run, binding, now, initial, proof).await
}
async fn initial_provenance(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    binding: &crate::RetainedExecutionBinding,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        InputFingerprint,
        crate::session_owner::preparation::ExpiredRunOrigin,
    ),
    Error,
> {
    if let Some(original) = unkeyed::read_with_proof(tx, &run.session_id, &run.id, proof).await? {
        original.verify_binding(&binding.capsule)?;
        return Ok((original.digest()?, original.origin()?));
    }
    let record = super::admission_execution::load_record_with_proof(tx, &run.id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    let init = super::admission_journal::read_with_proof(tx, &record, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::Unsupported)?;
    if record.state != crate::AdmissionExecutionState::Started {
        return Err(Error::InvalidTransition);
    }
    super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2").bind(&run.session_id.0).bind(record.key.as_str()), proof).await.map_err(unknown)?;
    let receipt = super::lookup_receipt(tx, &run.session_id, &record.key)
        .await
        .map_err(unknown)?;
    if receipt.as_ref() != Some(&record.receipt) {
        return Err(Error::UnknownStoreOutcome);
    }
    let frozen = &record.capsule;
    if frozen.request != binding.capsule.request
        || frozen.runtime_semantics != binding.capsule.runtime_semantics
        || frozen.input_envelope != binding.capsule.input_envelope
        || frozen.config_hash != binding.capsule.config_hash.as_str()
        || frozen.plan_fingerprint != binding.capsule.plan_fingerprint.as_str()
        || frozen.behavior_fingerprint != binding.capsule.behavior_fingerprint
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((
        crate::session_owner::validation::digest(&(
            "crabber.preparation.expired-initial.v1",
            init.digest().map_err(unknown)?,
        ))?,
        crate::session_owner::preparation::ExpiredRunOrigin::KeyedInitial(record.key),
    ))
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const RUN: &str = "SELECT CASE WHEN octet_length(data::text)<=4227072 THEN data::text END AS body,lease_until,(data->>'id'=id AND data->>'session_id'=session_id AND data->>'status'=status AND data->>'claim_token'=claim_token) AS valid FROM runs WHERE session_id=$1 AND id=$2 FOR UPDATE";

#[path = "expired/recovery.rs"]
mod recovery;

#[cfg(test)]
#[path = "expired/tests.rs"]
mod tests;

pub(super) async fn verify_begin(
    tx: &mut Transaction<'_, Postgres>,
    record: &crate::session_owner::record::Record,
    request: &crate::session_owner::MutateSessionOwner,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let Some((run_id, target)) =
        crate::session_owner::preparation::expired::native::begin_target(record, request)
    else {
        return Ok(());
    };
    let run = load_scoped_run(tx, &request.session_id, run_id).await?;
    let inspection =
        crate::session_owner::preparation::expired::native::begin_request(&run, target)?;
    let actual = context(tx, inspection, now).await?;
    if actual.target() != target {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
