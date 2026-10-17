//! Pure pre-effect source availability; never a native claim or loan.
use super::{Error, Proof, approval, source, unknown};
use crate::session_owner::preparation::{
    InspectPendingToolPreparation as Request, PendingToolPreparationMetadata as Metadata,
    pending_tool as choice,
};
use crabber_core::{Run, Session};
use sqlx::{Postgres, Row, Transaction};
impl crate::PostgresStore {
    pub(in crate::postgres) async fn inspect_pending_tool(
        &self,
        request: &Request,
    ) -> Result<Metadata, Error> {
        let mut tx = self.pool.begin().await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(unknown)?;
        let mut proof = Proof::bounded(32 << 20);
        let result = Box::pin(read(&mut tx, request, self.clock.now(), &mut proof)).await?;
        tx.commit().await.map_err(unknown)?;
        Ok(result)
    }
}
async fn read(
    tx: &mut Transaction<'_, Postgres>,
    request: &Request,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let metadata = choice::metadata_with_proof(request, proof)?;
    Box::pin(inspect(tx, request, metadata, now, proof)).await
}
async fn inspect(
    tx: &mut Transaction<'_, Postgres>,
    request: &Request,
    metadata: Metadata,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let claim = choice::claim(request);
    context(tx, request, proof).await?;
    proof.charge(8).map_err(unknown)?;
    crate::postgres::deletion::ensure_live(tx, &claim.session_id)
        .await
        .map_err(|e| match e {
            crate::StoreError::SessionDeleted => Error::Deleted,
            _ => Error::UnknownStoreOutcome,
        })?;
    let run = run(tx, request, proof).await?;
    let protected = crate::postgres::native_parent::source_protocol::current(
        tx,
        &claim.session_id,
        &claim.run_id,
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::Unsupported)?;
    protected.verify(&run).map_err(unknown)?;
    let binding = original_input(tx, request, proof).await?;
    let record = current_record(tx, &run, proof).await?;
    let original = if choice::recovery(request).is_some() {
        Some(
            crate::postgres::tool_continuation::read::receipt_with_proof(
                tx,
                &claim.session_id,
                &claim.id,
                proof,
            )
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?,
        )
    } else {
        None
    };
    choice::eligible(
        &record,
        &run,
        request,
        original.as_ref().map(|(receipt, _)| receipt),
    )?;
    let old = crate::postgres::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        Some(&record.batch.attempt.id),
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::NotFound)?
    .0;
    if old.snapshot.target != record.batch.attempt
        || old.snapshot.session_id != run.session_id
        || old.snapshot.run_id != run.id
        || old.snapshot.state != crate::AttemptState::Suspended
        || old.snapshot.cleanup != crate::AttemptCleanup::Complete
    {
        return Err(Error::Busy);
    }
    let current = crate::postgres::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?
        .0;
    choice::parent(&record, &run, request, &current, now)?;
    if choice::recovery(request).is_some() {
        approval::unstarted(tx, &run, &current.snapshot.target, proof).await?;
    }
    if choice::recovery(request).is_some() {
        Box::pin(provenance::check(
            tx,
            &run,
            &record,
            &binding.capsule,
            &protected,
            proof,
        ))
        .await?;
    }
    carriers::check(
        tx,
        &run,
        &record,
        choice::recovery(request).is_some(),
        proof,
    )
    .await?;
    Ok(metadata)
}
async fn current_record(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut Proof,
) -> Result<crate::tool_continuation::ToolContinuationRecord, Error> {
    let record = crate::postgres::tool_continuation::read::origin_with_proof(
        tx,
        &run.session_id,
        &run.id,
        None,
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::NotFound)?;
    crate::postgres::tool_continuation::current::verify_with_proof(tx, &record, proof)
        .await
        .map_err(unknown)?;
    proof.charge(8).map_err(unknown)?;
    let blocked: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&run.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if blocked {
        return Err(Error::Busy);
    }
    Ok(record)
}
async fn original_input(
    tx: &mut Transaction<'_, Postgres>,
    request: &Request,
    proof: &mut Proof,
) -> Result<crate::RetainedExecutionBinding, Error> {
    let claim = choice::claim(request);
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: claim.session_id.clone(),
        run: claim.run_id.clone(),
        continuation: claim.continuation.clone(),
    };
    if !crate::postgres::continuation_input::protocol(tx, &scope, proof)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let binding = crate::postgres::execution_binding::read_with_proof(
        tx,
        &claim.session_id,
        &claim.run_id,
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::Unsupported)?;
    source::approval::verify_input(
        tx,
        &claim.session_id,
        &claim.run_id,
        &binding.capsule,
        proof,
    )
    .await?;
    Ok(binding)
}
async fn run(
    tx: &mut Transaction<'_, Postgres>,
    request: &Request,
    proof: &mut Proof,
) -> Result<Run, Error> {
    let claim = choice::claim(request);
    super::super::proof::charge(tx, RUN_COST, &claim.session_id.0, &claim.run_id.0, proof).await?;
    let row = sqlx::query(RUN)
        .bind(&claim.session_id.0)
        .bind(&claim.run_id.0)
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
    if run.id != claim.run_id
        || run.session_id != claim.session_id
        || row
            .try_get::<Option<String>, _>("token")
            .map_err(unknown)?
            .as_deref()
            != Some(run.claim_token.as_str())
        || row
            .try_get::<Option<String>, _>("status")
            .map_err(unknown)?
            .as_deref()
            != Some(match run.status {
                crabber_core::RunStatus::Running => "running",
                crabber_core::RunStatus::Paused => "paused",
                _ => return Err(Error::InvalidTransition),
            })
        || row.try_get::<i64, _>("lease_until").map_err(unknown)?
            != crate::postgres::micros(run.lease_until)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
async fn context(
    tx: &mut Transaction<'_, Postgres>,
    request: &Request,
    proof: &mut Proof,
) -> Result<(), Error> {
    let claim = choice::claim(request);
    super::super::proof::charge(
        tx,
        SESSION_COST,
        &claim.session_id.0,
        &claim.run_id.0,
        proof,
    )
    .await?;
    let row = sqlx::query(SESSION)
        .bind(&claim.session_id.0)
        .bind(&claim.run_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let session: Session = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    if session.id != claim.session_id
        || session.workspace_id != request.context.workspace_id()
        || session.directory != request.context.directory()
    {
        return Err(Error::IdentityMismatch);
    }
    if u64::try_from(row.try_get::<i64, _>("revision").map_err(unknown)?).map_err(unknown)?
        != choice::revision(request).get()
    {
        return Err(Error::RevisionConflict);
    }
    Ok(())
}
const RUN_COST: &str = "SELECT CAST(octet_length(data::text)+octet_length(claim_token)+octet_length(status)+8 AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const RUN: &str = "SELECT CASE WHEN octet_length(data::text)<=4194304 THEN data::text END AS data,CASE WHEN octet_length(claim_token)<=512 THEN claim_token END AS token,CASE WHEN octet_length(status)<=32 THEN status END AS status,lease_until FROM runs WHERE session_id=$1 AND id=$2";
const SESSION_COST: &str = "SELECT CAST(octet_length(data::text)+8 AS BIGINT) FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const SESSION: &str = "SELECT CASE WHEN octet_length(data::text)<=16384 THEN data::text END AS data,public_revision AS revision FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";

#[path = "pending_tool/carriers.rs"]
mod carriers;

#[cfg(test)]
#[path = "pending_tool/tests.rs"]
mod tests;

#[path = "pending_tool/provenance.rs"]
mod provenance;

#[cfg(test)]
#[path = "pending_tool/latest_tests.rs"]
mod latest_tests;

#[cfg(test)]
#[path = "pending_tool/coherent_tests.rs"]
mod coherent_tests;
