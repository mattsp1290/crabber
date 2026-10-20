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
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<crate::native_parent::Witness>, Error> {
    let Some(parent) = &request.binding.parent else {
        return if authority.is_none() {
            Ok(None)
        } else {
            Err(Error::InvalidRequest)
        };
    };
    if parent.invocation.is_some() {
        let Some(SessionOwnerParentAuthority::Invocation(authority)) = authority else {
            return Err(Error::StaleOwner);
        };
        let original = super::current_owner_with_proof(
            tx,
            &request.binding.session_id,
            &authority.target().id,
            proof,
        )
        .await?;
        super::invocation::verify(tx, &original, proof).await?;
        let witness = crate::session_owner::invocation_parent::fresh(
            request, &original, authority, now, proof,
        )?;
        return Ok(Some(witness.clone()));
    }
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
    verify_live_attempt(&run, &attempt, now)?;
    let witness = if let Some(phase) = phase {
        super::super::native_parent::verify_phase(tx, &run, &attempt, &phase).await
    } else {
        super::super::native_parent::verify_attempt(tx, &run, &attempt).await
    }
    .map_err(unknown)?;
    Ok(Some(witness))
}

fn verify_live_attempt(
    run: &crabber_core::Run,
    attempt: &crate::attempt::AttemptRecord,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
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
    Ok(())
}
