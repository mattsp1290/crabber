//! Parent authentication against the genuine native Begin and Run issuance.
use super::{
    Error, Postgres, ReserveSessionOwner, SessionOwnerParentAuthority, Transaction, unknown,
};
use crate::{AttemptCleanup, AttemptState, PhaseState};
use crabber_core::RunStatus;
pub(super) async fn proof(
    tx: &mut Transaction<'_, Postgres>,
    request: &ReserveSessionOwner,
    authority: Option<&SessionOwnerParentAuthority>,
    now: time::OffsetDateTime,
) -> Result<Option<crate::native_parent::Witness>, Error> {
    let Some(parent) = &request.binding.parent else {
        return if authority.is_none() {
            Ok(None)
        } else {
            Err(Error::InvalidRequest)
        };
    };
    let run = super::run::load(tx, &request.binding.session_id, &parent.run_id).await?;
    if run.session_id != request.binding.session_id || run.id != parent.run_id {
        return Err(Error::NotFound);
    }
    if run.status != RunStatus::Running || run.lease_until <= now {
        return Err(Error::StaleOwner);
    }
    let (attempt, phase) = match (authority, &parent.phase, &parent.call_id) {
        (Some(SessionOwnerParentAuthority::Attempt(authority)), None, None) => {
            let attempt =
                super::super::attempt::load_owned(tx, &run.id, &authority.snapshot.target.id)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::StaleOwner)?;
            if !authority.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token) {
                return Err(Error::StaleOwner);
            }
            (attempt, None)
        }
        (Some(SessionOwnerParentAuthority::Phase(authority)), Some(target), Some(call_id)) => {
            let phase = super::super::phase::load_owned(tx, &run.id, &target.id)
                .await
                .map_err(unknown)?
                .ok_or(Error::StaleOwner)?;
            phase.validate().map_err(unknown)?;
            if phase.snapshot.target != *target
                || phase.snapshot.session_id != run.session_id
                || phase.snapshot.run_id != run.id
                || phase.snapshot.state != PhaseState::Started
                || phase.snapshot.quiescence != crate::PhaseQuiescence::Pending
                || target.call_id != *call_id
                || !authority.matches_store(&phase.snapshot, &phase.token)
            {
                return Err(Error::StaleOwner);
            }
            let attempt = super::super::attempt::load_owned(tx, &run.id, &target.attempt.id)
                .await
                .map_err(unknown)?
                .ok_or(Error::StaleOwner)?;
            if !authority.matches_attempt_store(
                &attempt.snapshot,
                &attempt.token,
                &attempt.run_token,
            ) {
                return Err(Error::StaleOwner);
            }
            let call = super::super::phase::load_call(tx, &run, call_id)
                .await
                .map_err(unknown)?;
            if call.status != crabber_core::ToolCallStatus::Running
                || call.result.is_some()
                || crate::phase::call_binding(&call).map_err(unknown)?
                    != phase.snapshot.call_binding
            {
                return Err(Error::UnknownStoreOutcome);
            }
            (attempt, Some(phase))
        }
        _ => return Err(Error::StaleOwner),
    };
    attempt.validate().map_err(unknown)?;
    if attempt.snapshot.session_id != run.session_id
        || attempt.snapshot.run_id != run.id
        || attempt.snapshot.state != AttemptState::Started
        || attempt.snapshot.cleanup != AttemptCleanup::Pending
        || attempt.run_token != run.claim_token
        || attempt.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    let witness = if let Some(phase) = phase {
        super::super::native_parent::verify_phase(tx, &run, &attempt, &phase).await
    } else {
        super::super::native_parent::verify_attempt(tx, &run, &attempt).await
    }
    .map_err(unknown)?;
    Ok(Some(witness))
}
