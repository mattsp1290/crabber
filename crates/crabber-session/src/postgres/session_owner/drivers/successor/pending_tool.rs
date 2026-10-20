//! Pure pre-effect source availability; never a native claim or loan.
use super::{Error, Proof, approval, source, unknown};
use crate::postgres::transaction::Transaction;
use crate::session_owner::preparation::{
    InspectPendingToolPreparation as Request, PendingToolPreparationMetadata as Metadata,
    pending_tool as choice,
};
use crabber_core::Run;
use sqlx::Postgres;
impl crate::PostgresStore {
    pub(in crate::postgres) async fn inspect_pending_tool(
        &self,
        request: &Request,
    ) -> Result<Metadata, Error> {
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(unknown)?;
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
    super::scoped::context(
        tx,
        &claim.session_id,
        &claim.run_id,
        &request.context,
        choice::revision(request),
        proof,
    )
    .await?;
    proof.charge(8).map_err(unknown)?;
    crate::postgres::deletion::ensure_live(tx, &claim.session_id)
        .await
        .map_err(|e| match e {
            crate::StoreError::SessionDeleted => Error::Deleted,
            _ => Error::UnknownStoreOutcome,
        })?;
    let run = super::scoped::run(tx, &claim.session_id, &claim.run_id, proof).await?;
    let protected = crate::postgres::native_parent::source_protocol::raw_current(
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
