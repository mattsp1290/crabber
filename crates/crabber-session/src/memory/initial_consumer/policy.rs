//! Actual fixed original and child retirement on the enclosing State/Proof.
use super::FixedClosedSource;
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::initial_consumer::{
    self as domain, OriginalPolicy, PolicyTerminal, Selection,
};
use crate::native_enrollment::pending_initial::Correlation;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crate::session_owner::coverage::FixedInitialOwnerRole;
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerError as Error,
    audit::Audit,
    owned_scope::{
        InitialResourceAssociation, JoinedOwnedScope, OriginalPolicyRetirementView,
        OriginalPolicyView, OwnedScopeActivation,
    },
    preparation::{JoinedPreparation, PreparationCommitError, PreparedOperation},
};

pub(in crate::memory) fn verify_purpose<'a>(
    store: &MemoryStore,
    original: &JoinedPreparation,
    expected: &Correlation,
    selected: &Selection<'_>,
    purpose: Option<(&'a OriginalPolicyView<'a>, &InitialResourceAssociation<'_>)>,
    proof: &mut Proof,
) -> Result<Option<&'a OwnedScopeActivation>, PreparationCommitError> {
    let stored = selected.origin.and_then(domain::Origin::original_policy);
    let Some((purpose, association)) = purpose else {
        if stored.is_some() {
            return Err(Error::Unsupported.into());
        }
        return Ok(None);
    };
    let activation = purpose.verify_open(store, association, original, expected, proof)?;
    if original.observed_registration() != Some(activation) {
        return Err(Error::IdentityMismatch.into());
    }
    if let Some(origin) = selected.origin {
        match origin {
            domain::Origin::Opened(_) if stored.is_none() => return Err(Error::Unsupported.into()),
            _ => (),
        }
    }
    if stored.is_some_and(|p| p.preparation_activation != *activation) {
        return Err(Error::IdentityMismatch.into());
    }
    let binding = match original.observed_operation() {
        PreparedOperation::Admit(value) => &value.binding,
        PreparedOperation::AdmitKeyed(value) => &value.binding,
        _ => return Err(Error::Unsupported.into()),
    };
    crate::session_owner::owned_scope::verify_fixed_binding(
        binding,
        original
            .observed_grant()
            .original_reservation()
            .resource_contract
            .as_ref(),
        proof,
    )?;
    Ok(Some(activation))
}
pub(in crate::memory) fn original(
    activation: &OwnedScopeActivation,
    proof: &mut Proof,
) -> Result<OriginalPolicy, PreparationCommitError> {
    proof
        .measure(activation)
        .map_err(|error| domain::failure(&error))?;
    Ok(OriginalPolicy {
        version: 1,
        kind: domain::SourceKind::FixedMemorySystemClockLiteralScriptedV1,
        preparation_activation: activation.clone(),
    })
}
pub(in crate::memory) struct Prepared<'a> {
    joined: JoinedOwnedScope,
    authority: &'a SessionOwnerAuthority,
}
pub(in crate::memory) fn prepare<'a>(
    state: &State,
    tail: &'a OriginalPolicyRetirementView<'a>,
    activation: &OwnedScopeActivation,
    proof: &mut Proof,
) -> Result<Prepared<'a>, PreparationCommitError> {
    let joined = tail.joined_scope(proof)?;
    if joined.activation() != activation {
        return Err(Error::IdentityMismatch.into());
    }
    let current = state
        .session_owners
        .initial_retirement_current(activation.session_id(), &activation.target().id, proof)?
        .ok_or(Error::UnknownStoreOutcome)?;
    proof
        .measure(current)
        .map_err(|error| domain::failure(&error))?;
    if current.owned_scope.as_ref() != Some(activation)
        || !tail.authority().authenticates(
            activation.session_id(),
            activation.target(),
            &current.token,
        )
    {
        return Err(Error::StaleOwner.into());
    }
    Ok(Prepared {
        joined,
        authority: tail.authority(),
    })
}
pub(in crate::memory) fn publish(
    state: &mut State,
    prepared: &Prepared<'_>,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Audit, PreparationCommitError> {
    let session = prepared.joined.activation().session_id();
    let revision = crate::memory::session_owner::revision(state, session);
    let audit = state.session_owners.join_initial_scope(
        &prepared.joined,
        prepared.authority,
        revision,
        now,
        proof,
    )?;
    crate::memory::bump_public_revision(state, session).map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(audit)
}
pub(in crate::memory) fn prepare_optional<'a>(
    state: &State,
    tail: (
        Option<&'a OriginalPolicyRetirementView<'a>>,
        Option<&OwnedScopeActivation>,
    ),
    proof: &mut Proof,
) -> Result<Option<Prepared<'a>>, PreparationCommitError> {
    match tail {
        (Some(tail), Some(activation)) => prepare(state, tail, activation, proof).map(Some),
        (None, None) => Ok(None),
        _ => Err(Error::Unsupported.into()),
    }
}
pub(in crate::memory) fn publish_terminal(
    state: &mut State,
    view: &Selection<'_>,
    prepared: Option<&Prepared<'_>>,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<Option<PolicyTerminal>, PreparationCommitError> {
    let child_join = prepared
        .map(|prepared| publish(state, prepared, now, proof))
        .transpose()?;
    match (
        view.origin.and_then(domain::Origin::original_policy),
        child_join,
    ) {
        (Some(original), Some(audit)) => terminal(original, audit, proof).map(Some),
        (None, _) => Ok(None),
        _ => Err(Error::Unsupported.into()),
    }
}
pub(in crate::memory) fn terminal(
    original: &OriginalPolicy,
    audit: Audit,
    proof: &mut Proof,
) -> Result<PolicyTerminal, PreparationCommitError> {
    let original_policy_digest = original.digest().map_err(|error| domain::failure(&error))?;
    proof
        .charge(std::mem::size_of::<Audit>())
        .map_err(|error| domain::failure(&error))?;
    let terminal = PolicyTerminal {
        version: 1,
        original_policy_digest,
        preparation_join: Box::new(audit),
    };
    terminal
        .verify(original, proof)
        .map_err(|error| domain::failure(&error))?;
    Ok(terminal)
}
pub(in crate::memory) fn verify_retained(
    state: &State,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let Some(original) = view.origin.and_then(domain::Origin::original_policy) else {
        return Ok(());
    };
    let activation = &original.preparation_activation;
    proof.charge(3 * std::mem::size_of::<&Audit>())?;
    let mut selected = Vec::with_capacity(3);
    for id in [
        &activation.source.reserve,
        &activation.source.begin,
        &activation.id,
    ] {
        let audit = state
            .session_owners
            .source_audit(activation.session_id(), id)
            .ok_or_else(|| unknown("fixed policy child original"))?;
        proof.measure(audit)?;
        selected.push(audit);
    }
    crate::session_owner::owned_scope::source::current(
        &selected[2].record,
        selected[0],
        selected[1],
        Some(selected[2]),
    )
    .map_err(unknown)?;
    if selected[2].record.owned_scope.as_ref() != Some(activation) {
        return Err(unknown("fixed policy child activation differs"));
    }
    let Some(close) = view.close else {
        return Ok(());
    };
    let terminal = close
        .policy_terminal
        .as_ref()
        .ok_or_else(|| unknown("fixed policy missing terminal"))?;
    terminal.verify(original, proof)?;
    let saved = terminal.preparation_join.as_ref();
    domain::retirement_verify_child(saved, &selected[2].record, proof)?;
    verify_join(state, saved, proof)
}
fn verify_join(state: &State, saved: &Audit, proof: &mut Proof) -> Result<(), Failure> {
    let actual = state
        .session_owners
        .source_audit(&saved.receipt.session_id, &saved.receipt.operation_id)
        .ok_or_else(|| unknown("fixed policy missing native Join"))?;
    proof.measure(actual)?;
    if domain::digest(actual)? != domain::digest(saved)? {
        return Err(unknown("fixed policy changed native Join"));
    }
    let current = state
        .session_owners
        .initial_retirement_current(
            &saved.receipt.session_id,
            &saved.record.snapshot.target.id,
            proof,
        )
        .map_err(unknown)?
        .ok_or_else(|| unknown("fixed policy missing Joined child"))?;
    if current != &saved.record || !current.token.is_empty() {
        return Err(unknown("fixed policy Joined child differs"));
    }
    Ok(())
}
impl MemoryStore {
    #[cfg(test)]
    pub(in crate::memory) fn classify_initial_resource_policy(
        state: &State,
        run: &crabber_core::Run,
        proof: &mut Proof,
    ) -> Result<bool, Failure> {
        Self::fixed_closed_initial_source(state, run, proof).map(|source| source.is_some())
    }
    pub(in crate::memory) fn fixed_closed_initial_source<'state>(
        state: &'state State,
        run: &'state crabber_core::Run,
        proof: &mut Proof,
    ) -> Result<Option<FixedClosedSource<'state>>, Failure> {
        let result = read_fixed_source(state, run, proof);
        proof.charge(0)?;
        result
    }
}
fn read_fixed_source<'state>(
    state: &'state State,
    run: &'state crabber_core::Run,
    proof: &mut Proof,
) -> Result<Option<FixedClosedSource<'state>>, Failure> {
    let assignment = state
        .pending_initial
        .assignment_for_run(&run.session_id, &run.id, proof)?
        .ok_or_else(|| unknown("fixed policy missing assignment"))?;
    let expected = &assignment.correlation;
    // The report already authenticated all source/Main members on this
    // State. A small independent original comparison lets generic sources
    // remain Unknown without loading their full private results again.
    if fixed_original(state, expected, proof)?.is_none() {
        return Ok(None);
    }
    let view = super::selected(state, expected, proof)?;
    let birth = state
        .pending_initial
        .selected(&expected.key, &expected.identity, proof)?;
    view.verify_birth(birth, proof)?;
    super::native::verify_members(state, &view, proof).map_err(unknown)?;
    if domain::classify_closed(&view, birth, run, proof)?
        != domain::InitialConsumption::CompletedSource
    {
        return Ok(None);
    }
    let Some(original) = view.origin.and_then(domain::Origin::original_policy) else {
        return Ok(None);
    };
    let binding =
        crate::memory::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(ReadError::public)?
            .ok_or_else(|| unknown("fixed policy original binding"))?;
    // Recompute the original identity from the authentic root/child sources,
    // not the policy's digest or current mutable projection.
    let (_, _, witness, audits) =
        crate::memory::session_owner::drivers::sources_read(state, &run.id, proof)
            .map_err(ReadError::public)?;
    let preparation = audits
        .reserve
        .record
        .preparation
        .as_ref()
        .ok_or_else(|| unknown("fixed policy original preparation"))?;
    crate::session_owner::owned_scope::verify_fixed_binding(
        &binding.capsule,
        preparation.resource_contract.as_ref(),
        proof,
    )
    .map_err(unknown)?;
    let key = domain::SourceKey::from_original_identities(
        &run.session_id,
        &audits.reserve.receipt.operation_id,
        &audits.begin.receipt.operation_id,
        &audits.begin.record.snapshot.target,
        &original.preparation_activation,
        proof,
    )?;
    if key != expected.key {
        return Err(unknown("fixed policy original source key"));
    }
    verify_retained(state, &view, proof)?;
    // The root is retired by the real Complete publisher. Child and Driver
    // retirement cannot imply that its current projection still matches Join.
    verify_join(state, audits.join, proof)?;
    let roles = fixed_roles(state, &view, &audits, proof)?;
    Ok(Some(FixedClosedSource {
        run,
        birth,
        consumer: view,
        witness,
        roles,
    }))
}

fn fixed_roles<'state>(
    state: &'state State,
    view: &Selection<'state>,
    audits: &crate::session_owner::preparation::admission::SourceAudits<'state>,
    proof: &mut Proof,
) -> Result<[FixedInitialOwnerRole<'state>; 3], Failure> {
    let policy = view
        .origin
        .and_then(domain::Origin::original_policy)
        .ok_or_else(|| unknown("fixed source policy original"))?;
    let activation = &policy.preparation_activation;
    let actual = |id| {
        state
            .session_owners
            .source_audit(activation.session_id(), id)
            .ok_or_else(|| unknown("fixed source role original"))
    };
    let child_activation = actual(&activation.id)?;
    let saved_child = view
        .close
        .and_then(|c| c.policy_terminal.as_ref())
        .ok_or_else(|| unknown("fixed source child terminal"))?
        .preparation_join
        .as_ref();
    let child_join = actual(&saved_child.receipt.operation_id)?;
    let registered = view
        .results
        .iter()
        .copied()
        .find_map(|r| match &r.original {
            domain::Original::Register(value) => Some(value.as_ref()),
            _ => None,
        })
        .ok_or_else(|| unknown("fixed source Driver Register"))?;
    let activated = view
        .results
        .iter()
        .copied()
        .find_map(|r| match &r.original {
            domain::Original::Activate(value) => Some(value.as_ref()),
            _ => None,
        })
        .ok_or_else(|| unknown("fixed source Driver Activate"))?;
    let saved_driver = view
        .close
        .and_then(|c| c.retirement.audit())
        .ok_or_else(|| unknown("fixed source Driver Join"))?;
    let driver_join = actual(&saved_driver.receipt.operation_id)?;
    // New stack metadata has no owned payload. All actual leaves were measured
    // by selected/native verification, and the defining Ledger reauthenticates
    // them under this same Proof before using the borrowed roles.
    proof.measure(child_activation)?;
    Ok([
        FixedInitialOwnerRole {
            reserve: audits.reserve,
            begin: audits.begin,
            activation: None,
            joined: audits.join,
        },
        FixedInitialOwnerRole {
            reserve: audits.child_reserve,
            begin: audits.child_begin,
            activation: Some(child_activation),
            joined: child_join,
        },
        FixedInitialOwnerRole {
            reserve: &registered.reserve,
            begin: &registered.begin,
            activation: Some(&activated.audit),
            joined: driver_join,
        },
    ])
}

pub(in crate::memory) fn verify_unopened_retirement(
    state: &State,
    activation: &OwnedScopeActivation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let current = state
        .session_owners
        .initial_retirement_current(activation.session_id(), &activation.target().id, proof)
        .map_err(unknown)?
        .ok_or_else(|| unknown("fixed no-Open missing child"))?;
    proof.measure(current)?;
    let joined = current
        .owned_join
        .as_ref()
        .ok_or_else(|| unknown("fixed no-Open unretired child"))?;
    if joined.activation != *activation {
        return Err(unknown("fixed no-Open foreign child"));
    }
    let request = joined.request().map_err(unknown)?;
    let saved = state
        .session_owners
        .source_audit(activation.session_id(), &request.id)
        .ok_or_else(|| unknown("fixed no-Open missing Join"))?;
    proof.measure(saved)?;
    let activated = state
        .session_owners
        .source_audit(activation.session_id(), &activation.id)
        .ok_or_else(|| unknown("fixed no-Open missing Activate"))?;
    proof.measure(activated)?;
    domain::retirement_verify_child(saved, &activated.record, proof)?;
    verify_join(state, saved, proof)
}

impl MemoryStore {
    /// Borrow/precharge the real managed Session before controller root I/O.
    pub(crate) fn fixed_initial_context(
        &self,
        context: &crabber_core::SessionContext,
        expected: crabber_core::SessionRevision,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state, context.session_id())
            .map_err(crate::deletion::owner_error)?;
        let session = state
            .sessions
            .get(context.session_id())
            .ok_or(Error::NotFound)?;
        let revision = crate::memory::session_owner::revision(&state, context.session_id());
        proof
            .measure(&(
                session,
                context.session_id(),
                context.workspace_id(),
                context.directory(),
                revision,
                state
                    .ownership_protocol_sessions
                    .contains(context.session_id()),
            ))
            .map_err(|error| domain::failure(&error))?;
        session
            .ensure_identity(context.workspace_id(), context.directory())
            .map_err(|_| Error::IdentityMismatch)?;
        if revision != crate::session_owner::audit::RevisionState::Present(expected) {
            return Err(Error::RevisionConflict.into());
        }
        let origin = state
            .ownership_origins
            .get(context.session_id())
            .ok_or(Error::Unsupported)?;
        proof
            .measure(origin)
            .map_err(|error| domain::failure(&error))?;
        if !state
            .ownership_protocol_sessions
            .contains(context.session_id())
            || origin.kind != crate::deletion::origin::OriginKind::ManagedCreate
        {
            return Err(Error::Unsupported.into());
        }
        proof
            .measure(&(&session.id, &origin.operation_id))
            .map_err(|error| domain::failure(&error))?;
        let key = (session.id.clone(), origin.operation_id.clone());
        let Some(crate::memory::OperationReceipt::Management(receipt)) =
            state.operation_receipts.get(&key)
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let audit = state
            .management_audits
            .get(&key)
            .ok_or(Error::UnknownStoreOutcome)?;
        proof
            .measure(&(receipt, audit, &session.id))
            .map_err(|error| domain::failure(&error))?;
        crate::deletion::coverage::managed(origin, session, expected, audit, receipt, proof.used())
            .map_err(|_| Error::UnknownStoreOutcome)?;
        Ok(())
    }
}

fn fixed_original<'a>(
    state: &'a State,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Option<&'a OriginalPolicy>, Failure> {
    let original = state
        .initial_consumer
        .origin(&expected.key)
        .ok_or_else(|| unknown("fixed policy missing origin"))?;
    let cohort = state
        .initial_consumer_cohorts
        .get(&expected.key)
        .ok_or_else(|| unknown("fixed policy missing cohort"))?;
    proof.measure(&(original, cohort.as_ref()))?;
    original.validate(proof)?;
    if original.correlation() != expected || cohort.digest()? != original.digest()? {
        return Err(unknown("fixed policy changed original cohort"));
    }
    Ok(original.original_policy())
}
