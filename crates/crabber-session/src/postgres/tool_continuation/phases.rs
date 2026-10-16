use super::{Error, ToolContinuationRecord, unknown};
use crate::{AttemptAuthority, ToolPhaseResume};
use crabber_core::{Run, ToolCallStatus};
use sqlx::{Postgres, Transaction};
pub(super) async fn claim(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    activated: &Run,
    parent: &crate::attempt::AttemptRecord,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        let old = super::super::phase::load_host_record(tx, &run.id, &frozen.previous.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::NotFound)?;
        let mut call = super::super::phase::load_call(tx, run, &frozen.previous.call_id)
            .await
            .map_err(unknown)?;
        let native = old
            .resume_checkpoint(activated, parent, &call, frozen)
            .map_err(|_| Error::CleanupBlocked)?;
        let capability = native.authority(authority.clone()).map_err(unknown)?;
        phases.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: capability,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses(frozen)?,
        });
        super::super::phase::save_host_record(tx, &native, true)
            .await
            .map_err(unknown)?;
        call.status = ToolCallStatus::Pending;
        super::super::phase::save_host_call(tx, &call)
            .await
            .map_err(unknown)?;
    }
    Ok(phases)
}
pub(super) async fn recover(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &ToolContinuationRecord,
    authority: &AttemptAuthority,
) -> Result<Vec<ToolPhaseResume>, Error> {
    let mut phases = Vec::new();
    for frozen in &record.batch.phases {
        let mut native = super::super::phase::load_host_record(tx, &run.id, &frozen.next.id)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
        native.token = uuid::Uuid::new_v4().to_string();
        phases.push(ToolPhaseResume {
            target: frozen.next.clone(),
            authority: native.authority(authority.clone()).map_err(unknown)?,
            identity: frozen.identity.clone(),
            checkpoint: frozen.checkpoint.clone(),
            responses: record.responses(frozen)?,
        });
        super::super::phase::save_host_record(tx, &native, false)
            .await
            .map_err(unknown)?;
    }
    Ok(phases)
}

pub(super) async fn suspend(
    tx: &mut Transaction<'_, Postgres>,
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

    Ok(())
}
