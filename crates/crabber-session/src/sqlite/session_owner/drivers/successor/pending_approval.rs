//! Pure pre-effect source availability; never a native claim or loan.
use super::{Error, Proof, approval, source, unknown};
use crate::session_owner::preparation::{
    InspectPendingApprovalPreparation as Request, PendingApprovalPreparationMetadata as Metadata,
    PendingApprovalSource, pending_approval as choice,
};
use crabber_core::{Run, Session};
use sqlx::{Row, Sqlite, Transaction};
impl crate::SqliteStore {
    pub(in crate::sqlite) async fn inspect_pending_approval(
        &self,
        request: &Request,
    ) -> Result<Metadata, Error> {
        let metadata = choice::metadata(request)?;
        let mut tx = self.readers.begin().await.map_err(unknown)?;
        let mut proof = Proof::bounded(32 << 20);
        let result = Box::pin(inspect(
            &mut tx,
            request,
            metadata,
            self.clock.now(),
            &mut proof,
        ))
        .await?;
        tx.commit().await.map_err(unknown)?;
        Ok(result)
    }
}
pub(super) async fn inspect(
    tx: &mut Transaction<'_, Sqlite>,
    request: &Request,
    metadata: Metadata,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Metadata, Error> {
    let claim = choice::claim(request);
    context(tx, request, proof).await?;
    proof.charge(8).map_err(unknown)?;
    crate::sqlite::deletion::ensure_live(tx, &claim.session_id)
        .await
        .map_err(|e| match e {
            crate::StoreError::SessionDeleted => Error::Deleted,
            _ => Error::UnknownStoreOutcome,
        })?;
    let run = run(tx, request, proof).await?;
    let protected = crate::sqlite::native_parent::source_protocol::current(
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
    let (record, revision) =
        crate::sqlite::approval::load_with_proof(tx, &claim.session_id, &claim.run_id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
    if revision != choice::revision(request) {
        return Err(Error::RevisionConflict);
    }
    crate::sqlite::approval::write::active(tx, &record)
        .await
        .map_err(unknown)?;
    let original = match &request.source {
        PendingApprovalSource::Claim(_) => None,
        PendingApprovalSource::Recovery(recovery) => Some(
            crate::sqlite::approval::receipt_with_proof(
                tx,
                &claim.session_id,
                &recovery.original.id,
                proof,
            )
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?,
        ),
    };
    choice::eligible(&record, &run, &request.source, original.as_ref(), now)?;
    let old = crate::sqlite::attempt::load_host_attempt_with_proof(
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
    let current = crate::sqlite::attempt::load_host_attempt_with_proof(tx, &run.id, None, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?
        .0;
    choice::parent(&record, &run, &request.source, &current, now)?;
    if matches!(request.source, PendingApprovalSource::Recovery(_)) {
        approval::unstarted(tx, &run, &current.snapshot.target, proof).await?;
    }
    if matches!(request.source, PendingApprovalSource::Recovery(_)) {
        Box::pin(provenance::check(
            tx,
            &run,
            &record,
            &binding.capsule,
            &claim.id,
            &protected,
            proof,
        ))
        .await?;
    }
    calls(tx, &record, proof).await?;
    Ok(metadata)
}
async fn original_input(
    tx: &mut Transaction<'_, Sqlite>,
    request: &Request,
    proof: &mut Proof,
) -> Result<crate::RetainedExecutionBinding, Error> {
    let claim = choice::claim(request);
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Approval,
        session: claim.session_id.clone(),
        run: claim.run_id.clone(),
        continuation: claim.continuation.clone(),
    };
    if !crate::sqlite::continuation_input::protocol(tx, &scope, proof)
        .await
        .map_err(unknown)?
    {
        return Err(Error::Unsupported);
    }
    let binding = crate::sqlite::execution_binding::read_with_proof(
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
    tx: &mut Transaction<'_, Sqlite>,
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
            != crate::sqlite::records::micros(run.lease_until)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(run)
}
async fn context(
    tx: &mut Transaction<'_, Sqlite>,
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
async fn calls(
    tx: &mut Transaction<'_, Sqlite>,
    record: &crate::approval::ApprovalRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.charge(16).map_err(unknown)?;
    let blocked: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tool_continuation_barriers WHERE session_id=$1)",
    )
    .bind(&record.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if blocked {
        return Err(Error::Busy);
    }
    let count: i64=sqlx::query_scalar("SELECT COUNT(*) FROM (SELECT 1 FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running') LIMIT 65) bounded").bind(&record.run_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != record.batch.calls.len() {
        return Err(Error::Busy);
    }
    for frozen in &record.batch.calls {
        super::super::proof::charge(
            tx,
            approval::CALL_COST,
            &record.run_id.0,
            &frozen.target.call_id.0,
            proof,
        )
        .await?;
        let row = sqlx::query(approval::CALL)
            .bind(&record.run_id.0)
            .bind(&frozen.target.call_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let call: crabber_core::ToolCallRecord = serde_json::from_str(
            &row.try_get::<Option<String>, _>("record")
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(unknown)?;
        if call.id != frozen.target.call_id
            || row
                .try_get::<Option<String>, _>("status")
                .map_err(unknown)?
                .as_deref()
                != Some("pending")
        {
            return Err(Error::SemanticConflict);
        }
        choice::call(record, &call)?;
        proof.charge(8).map_err(unknown)?;
        let phase: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND call_id=$2)",
        )
        .bind(&record.run_id.0)
        .bind(&call.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
        if phase {
            return Err(Error::Busy);
        }
    }
    Ok(())
}
const RUN_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+length(CAST(claim_token AS BLOB))+length(CAST(status AS BLOB))+8 AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const RUN: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=4194304 THEN data END AS data,CASE WHEN length(CAST(claim_token AS BLOB))<=512 THEN claim_token END AS token,CASE WHEN length(CAST(status AS BLOB))<=32 THEN status END AS status,lease_until FROM runs WHERE session_id=$1 AND id=$2";
const SESSION_COST: &str = "SELECT CAST(length(CAST(data AS BLOB))+8 AS BIGINT) FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const SESSION: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=16384 THEN data END AS data,public_revision AS revision FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";

#[cfg(test)]
#[path = "pending_approval/tests.rs"]
mod tests;

#[path = "pending_approval/provenance.rs"]
mod provenance;
