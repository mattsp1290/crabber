use super::{Error, ToolContinuationRecord, unknown};
use crate::native_enrollment::caller::{self, tool::IssuedPhases};
use crate::public_session::v2::proof::Proof;
use crate::transaction::Transaction;
use crate::{AttemptAuthority, ToolPhaseResume};
use crabber_core::{Run, ToolCallStatus};
use sqlx::Sqlite;
pub(super) async fn claim(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    activated: &Run,
    parent: &crate::attempt::AttemptRecord,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<ToolPhaseResume>, Error> {
    let mut issued = IssuedPhases::new(record.batch.phases.len(), proof).map_err(unknown)?;
    for frozen in &record.batch.phases {
        let old = super::super::phase::load_with_proof(tx, &run.id, &frozen.previous.id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let mut call =
            super::super::phase::load_call_with_proof(tx, run, &frozen.previous.call_id, proof)
                .await
                .map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        let native = old
            .resume_checkpoint(activated, parent, &call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        caller::measure_phase_record(&native, proof).map_err(unknown)?;
        caller::measure_authority(Some(authority), proof).map_err(unknown)?;
        proof
            .measure(&(&frozen.next, &frozen.identity, &frozen.checkpoint))
            .map_err(unknown)?;
        let capability = native.authority(authority.clone()).map_err(unknown)?;
        issued.grants.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: capability,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses_with_proof(frozen, proof)?,
        });
        super::super::phase::save_host_record(tx, &native, true)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Pending;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
        issued.records.push(native);
    }
    Ok(issued)
}
pub(super) async fn recover(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<ToolPhaseResume>, Error> {
    let mut issued = IssuedPhases::new(record.batch.phases.len(), proof).map_err(unknown)?;
    for frozen in &record.batch.phases {
        let mut native = super::super::phase::load_with_proof(tx, &run.id, &frozen.next.id, proof)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        caller::measure_phase_record(&native, proof).map_err(unknown)?;
        caller::measure_authority(Some(authority), proof).map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        issued.grants.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses_with_proof(frozen, proof)?,
        });
        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;
        issued.records.push(native);
    }
    Ok(issued)
}

pub(super) async fn claim_frontend(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    parent: &crate::attempt::AttemptRecord,
    record: &ToolContinuationRecord,
    claim: &crate::FrontendContinuationClaim,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<crate::FrontendPhaseResume>, Error> {
    claim.validate_for_batch(&record.batch)?;
    let mut issued = IssuedPhases::new(record.batch.frontend.len(), proof).map_err(unknown)?;
    for frozen in &record.batch.frontend {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE id=$1 OR (run_id=$2 AND call_id=$3))",
        )
        .bind(&frozen.target.id.0)
        .bind(&run.id.0)
        .bind(&frozen.target.call_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
        if exists {
            return Err(Error::CleanupBlocked);
        }
        let mut call =
            super::super::phase::load_call_with_proof(tx, run, &frozen.target.call_id, proof)
                .await
                .map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        let native = crate::phase::PhaseRecord::claim_frontend_result(run, parent, &call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        caller::measure_phase_record(&native, proof).map_err(unknown)?;
        caller::measure_authority(Some(authority), proof).map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        proof.measure(&claim.results).map_err(unknown)?;
        issued.grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            result: claim
                .results
                .iter()
                .find(|result| result.call_id() == &frozen.target.call_id)
                .ok_or(Error::SemanticConflict)?
                .clone(),
            frozen: frozen.clone(),
        });
        super::super::phase::save_host_record(tx, &native, true)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Pending;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
        issued.records.push(native);
    }
    Ok(issued)
}

pub(super) async fn recover_frontend(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
    proof: &mut Proof,
) -> Result<IssuedPhases<crate::FrontendPhaseResume>, Error> {
    let claim = record
        .frontend_claim
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    claim.validate_for_batch(&record.batch)?;
    let mut issued = IssuedPhases::new(record.batch.frontend.len(), proof).map_err(unknown)?;
    for frozen in &record.batch.frontend {
        let mut native =
            super::super::phase::load_with_proof(tx, &run.id, &frozen.target.id, proof)
                .await
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        caller::measure_phase_record(&native, proof).map_err(unknown)?;
        caller::measure_authority(Some(authority), proof).map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        caller::measure_phase_record(&native, proof).map_err(unknown)?;
        caller::measure_authority(Some(authority), proof).map_err(unknown)?;
        proof.measure(frozen).map_err(unknown)?;
        proof.measure(&claim.results).map_err(unknown)?;
        issued.grants.push(crate::FrontendPhaseResume {
            target: frozen.target.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            result: claim
                .results
                .iter()
                .find(|r| r.call_id() == &frozen.target.call_id)
                .ok_or(Error::UnknownStoreOutcome)?
                .clone(),
            frozen: frozen.clone(),
        });
        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;
        issued.records.push(native);
    }
    Ok(issued)
}

pub(super) async fn suspend(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    parent: &crate::attempt::AttemptRecord,
    batch: &crate::ToolContinuationSuspension,
    authority: &crate::ToolSuspensionAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    for (frozen, capability) in batch.phases.iter().zip(&authority.phases) {
        let mut call = super::super::phase::load_call(tx, run, &frozen.previous.call_id)
            .await
            .map_err(unknown)?;

        let mut native = super::super::phase::load_host_record(tx, &run.id, &frozen.previous.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;

        native
            .suspend_checkpoint(run, parent, &call, frozen, capability, now)
            .map_err(|_| Error::StaleOwner)?;

        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;

        call.status = ToolCallStatus::Waiting;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }

    super::super::frontend::mark_waiting(tx, run, batch).await?;
    Ok(())
}
