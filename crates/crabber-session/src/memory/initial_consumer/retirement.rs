//! Actual Driver retirement and its original native corroboration under one lock.
use crate::memory::State;
use crate::native_enrollment::initial_consumer::{
    self as domain, Correlation, DriverRetirement, Selection,
};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerError as Error,
    owned_scope::{InitialRetirementWitness, JoinedOwnedScope, OwnedMainDisposition},
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crabber_core::{RunId, SessionContext, SessionOwnerId};

pub(in crate::memory) struct Prepared<'a> {
    disposition: OwnedMainDisposition,
    kind: Kind<'a>,
}
enum Kind<'a> {
    NoDriver,
    Activated {
        joined: JoinedOwnedScope,
        authority: &'a SessionOwnerAuthority,
    },
    Unactivated(domain::VerifiedUnactivatedRetirement<'a>),
}
pub(in crate::memory) fn prepare<'a>(
    state: &State,
    source: (&JoinedPreparation, &SessionContext, &Correlation),
    view: &'a Selection<'_>,
    witness: &'a InitialRetirementWitness<'a>,
    proof: &mut Proof,
) -> Result<Prepared<'a>, PreparationCommitError> {
    let (original, context, expected) = source;
    let local = witness.verify(original, context, expected, proof)?;
    let kind = if let Some(register) = view.registered_driver() {
        witness.verify_register(register, proof)?;
        let current = state
            .session_owners
            .initial_retirement_current(
                &expected.key.session_id,
                &register.begin.record.snapshot.target.id,
                proof,
            )?
            .ok_or(Error::UnknownStoreOutcome)?;
        if let Some(activation) = view.activated_driver() {
            let actual = local.activation.ok_or(Error::UnknownStoreOutcome)?;
            let grant = local.grant.ok_or(Error::UnknownStoreOutcome)?;
            if actual != &activation.request
                || current != &activation.audit.record
                || !grant.authority().authenticates(
                    &expected.key.session_id,
                    &current.snapshot.target,
                    &current.token,
                )
            {
                return Err(Error::StaleOwner.into());
            }
            let joined = witness.joined_scope(actual, proof)?;
            Kind::Activated {
                joined,
                authority: grant.authority(),
            }
        } else {
            Kind::Unactivated(domain::verify_unactivated_retirement(
                witness, source, register, current, proof,
            )?)
        }
    } else {
        verify_absent(state, expected, view, proof).map_err(|error| domain::failure(&error))?;
        Kind::NoDriver
    };
    Ok(Prepared {
        disposition: local.disposition,
        kind,
    })
}
pub(in crate::memory) fn publish(
    state: &mut State,
    prepared: Prepared<'_>,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<(DriverRetirement, OwnedMainDisposition), PreparationCommitError> {
    let retirement = match prepared.kind {
        Kind::NoDriver => DriverRetirement::NoDriver,
        Kind::Activated { joined, authority } => {
            let session = joined.activation().session_id();
            let revision = crate::memory::session_owner::revision(state, session);
            let audit = state
                .session_owners
                .join_initial_scope(&joined, authority, revision, now, proof)?;
            crate::memory::bump_public_revision(state, session)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            proof
                .charge(std::mem::size_of::<crate::session_owner::audit::Audit>())
                .map_err(|error| domain::failure(&error))?;
            DriverRetirement::Activated {
                audit: Box::new(audit),
            }
        }
        Kind::Unactivated(witness) => {
            proof
                .measure(&witness.request().session_id)
                .map_err(|error| domain::failure(&error))?;
            let session = witness.request().session_id.clone();
            let revision = crate::memory::session_owner::revision(state, &session);
            let audit = state
                .session_owners
                .retire_unactivated_initial(witness, revision, now, proof)?;
            crate::memory::bump_public_revision(state, &session)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            proof
                .charge(std::mem::size_of::<crate::session_owner::audit::Audit>())
                .map_err(|error| domain::failure(&error))?;
            DriverRetirement::Unactivated {
                audit: Box::new(audit),
            }
        }
    };
    Ok((retirement, prepared.disposition))
}
pub(in crate::memory) fn verify_retained(
    state: &State,
    expected: &Correlation,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let Some((retirement, disposition)) = view.retirement() else {
        return Ok(());
    };
    retirement.verify(view, expected, disposition, proof)?;
    let Some(saved) = retirement.audit() else {
        return verify_absent(state, expected, view, proof);
    };
    let actual = state
        .session_owners
        .source_audit(&saved.receipt.session_id, &saved.receipt.operation_id)
        .ok_or_else(|| unknown("missing native retirement Join"))?;
    proof.measure(actual)?;
    if domain::digest(actual)? != domain::digest(saved)? {
        return Err(unknown("changed native retirement Join"));
    }
    let current = state
        .session_owners
        .initial_retirement_current(
            &saved.receipt.session_id,
            &saved.receipt.owner.target.id,
            proof,
        )
        .map_err(unknown)?
        .ok_or_else(|| unknown("missing joined Driver"))?;
    if current != &saved.record {
        return Err(unknown("retirement current original differs"));
    }
    Ok(())
}
pub(in crate::memory) fn verify_absent(
    state: &State,
    expected: &Correlation,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    // Includes origins/cohorts with no retained Driver and reverse missing Run or
    // assignment. A current-epoch orphan cannot disappear through NoDriver.
    crate::memory::native_enrollment::routes(state, &expected.key.session_id, proof)?;
    for ((session, run), origin) in &state.execution_enrollment_origins {
        if session == &expected.key.session_id && origin.is_pending() {
            proof.measure(&(session, run, origin))?;
            let assignment = state
                .pending_initial
                .assignment_for_run(session, run, proof)?
                .ok_or_else(|| unknown("Pending origin missing assignment"))?;
            if assignment.correlation.identity.epoch == expected.identity.epoch
                && view
                    .assignment
                    .is_none_or(|selected| selected.assignment != *assignment)
            {
                return Err(unknown("NoDriver hides this epoch's native assignment"));
            }
        }
    }
    if let Some(assignment) = view.assignment {
        let id = driver_id(&expected.key.session_id, assignment.run_id(), proof)?;
        if state
            .session_owners
            .initial_retirement_current(&expected.key.session_id, &id, proof)
            .map_err(unknown)?
            .is_some()
        {
            return Err(unknown("NoDriver hides actual native owner"));
        }
    }
    for driver in state
        .session_owners
        .initial_retirement_drivers(&expected.key.session_id, proof)
        .map_err(unknown)?
    {
        crate::memory::session_owner::drivers::original_read(state, driver, proof)
            .map_err(crate::public_session::v2::proof::ReadError::public)?;
        let run = driver.run_id();
        match crate::memory::native_enrollment::original(
            state,
            &expected.key.session_id,
            run,
            proof,
        )? {
            crate::native_enrollment::Status::Legacy
            | crate::native_enrollment::Status::Unclassified => {}
            crate::native_enrollment::Status::Pending => {
                let assignment = state
                    .pending_initial
                    .assignment_for_run(&expected.key.session_id, run, proof)?
                    .ok_or_else(|| unknown("native Driver without Pending assignment"))?;
                if assignment.correlation.identity.epoch == expected.identity.epoch {
                    return Err(unknown("NoDriver hides this epoch's original"));
                }
            }
        }
    }
    Ok(())
}
fn driver_id(
    session: &crabber_core::SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<SessionOwnerId, Failure> {
    proof.measure(&(session, run))?;
    let identity = crate::session_owner::validation::digest(&(
        "crabber.execution-driver.initial.identity.v1",
        session,
        run,
    ))
    .map_err(unknown)?;
    proof.charge("driver-owner-".len() + 64 + 2)?;
    Ok(SessionOwnerId(format!(
        "driver-owner-{}",
        identity.as_str()
    )))
}
