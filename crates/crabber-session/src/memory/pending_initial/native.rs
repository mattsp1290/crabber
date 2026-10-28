//! Actual opaque ingress and first publishers, compiled only by the native root.
use super::{failure, routes};
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::pending_initial::{
    self as protocol, ActualRawMetadata, Bundle, CloseDecision, Correlation, OpenDecision,
    OpenRecord, RecordDelta,
};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::owned_scope::{
    NativeInitialOperation, PendingInitialNativeCall, PendingInitialNativeReply,
};
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crabber_core::SessionContext;
fn selected<'a>(
    state: &'a State,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Bundle<'a>, PreparationCommitError> {
    routes(state, &expected.key.session_id, proof).map_err(|error| failure(proof, &error))?;
    let view = state
        .pending_initial
        .selected(&expected.key, &expected.identity, proof)
        .map_err(|error| failure(proof, &error))?;
    if state.pending_initial_protocols.contains_key(&expected.key) == view.empty() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(view)
}
fn verify_consumer_context(
    original: &JoinedPreparation,
    expected: &Correlation,
    context: &SessionContext,
    consumer: Option<&crate::session_owner::owned_scope::InitialResourceAssociation<'_>>,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    verify_context(original, expected, context)?;
    if let Some(consumer) = consumer {
        consumer.verify(original, context, expected, proof)?;
    }
    Ok(())
}
fn selected_consumer<'a>(
    state: &'a State,
    expected: &Correlation,
    birth: Bundle<'_>,
    consumer: Option<&crate::session_owner::owned_scope::InitialResourceAssociation<'_>>,
    proof: &mut Proof,
) -> Result<crate::native_enrollment::initial_consumer::Selection<'a>, PreparationCommitError> {
    let selected = crate::memory::initial_consumer::selected(state, expected, proof)
        .map_err(|error| failure(proof, &error))?;
    if !selected.empty() {
        selected
            .verify_birth(birth, proof)
            .map_err(|error| failure(proof, &error))?;
        crate::memory::initial_consumer::native::verify_members(state, &selected, proof)?;
        if consumer.is_none() {
            return Err(Error::Unsupported.into());
        }
    }
    Ok(selected)
}
/// A consumer Seal must carry its same-task joined retirement witness;
/// legacy birth-only Seal carries neither consumer nor witness.
fn seal_witness<'a>(
    consumer: Option<&crate::session_owner::owned_scope::InitialResourceAssociation<'_>>,
    retirement: Option<&'a crate::session_owner::owned_scope::InitialRetirementWitness<'a>>,
) -> Result<
    Option<&'a crate::session_owner::owned_scope::InitialRetirementWitness<'a>>,
    PreparationCommitError,
> {
    match (consumer, retirement) {
        (Some(_), Some(witness)) => Ok(Some(witness)),
        (None, None) => Ok(None),
        _ => Err(Error::Unsupported.into()),
    }
}
pub(in crate::memory) fn historical<'a>(
    state: &'a State,
    original: &'a JoinedPreparation,
    selected: &crate::native_enrollment::caller::initial::SelectedOriginal<'_>,
    proof: &mut Proof,
) -> Result<protocol::prefix::HistoricalInitialPrefix<'a>, PreparationCommitError> {
    state.session_owners.validate_driver_membership(
        &selected
            .original()
            .observed_grant()
            .original_reservation()
            .owner
            .binding
            .session_id,
        proof,
    )?;
    let ids = protocol::prefix::select(original, proof)?;
    let session = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id;
    let audit = |id| {
        state
            .session_owners
            .source_audit(session, id)
            .ok_or(Error::UnknownStoreOutcome)
    };
    protocol::prefix::verify_selected(
        original,
        protocol::prefix::InitialPrefixAudits {
            root_reserve: audit(&ids.root_reserve)?,
            root_begin: audit(&ids.root_begin)?,
            child_reserve: audit(&ids.child_reserve)?,
            child_begin: audit(&ids.child_begin)?,
            activated: audit(&ids.activated)?,
        },
        Some(selected),
        proof,
    )
}
fn published_initial_reply(
    view: Bundle<'_>,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Option<PendingInitialNativeReply>, PreparationCommitError> {
    let Some(assignment) = view.assignment else {
        return Ok(None);
    };
    if assignment.correlation != *expected {
        return Err(Error::SemanticConflict.into());
    }
    proof.measure(assignment).map_err(|error| failure(proof, &error))?;
    PendingInitialNativeReply::already_published(assignment.clone(), proof).map(Some)
}
fn raw_completed_reply(
    raw: ActualRawMetadata,
    view: Bundle<'_>,
    operation: NativeInitialOperation,
    proof: &mut Proof,
) -> Result<PendingInitialNativeReply, PreparationCommitError> {
    if !view.empty() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    if operation == NativeInitialOperation::Complete {
        return Err(Error::Unsupported.into());
    }
    PendingInitialNativeReply::raw_completed(raw, proof)
}
pub(in crate::memory) fn verify_context(
    original: &JoinedPreparation,
    expected: &Correlation,
    context: &SessionContext,
) -> Result<(), PreparationCommitError> {
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    if context.session_id() != &expected.key.session_id
        || context.workspace_id() != binding.workspace_id
        || context.directory() != binding.directory
    {
        return Err(Error::IdentityMismatch.into());
    }
    Ok(())
}
pub(in crate::memory) fn publish(
    state: &mut State,
    expected: &Correlation,
    delta: RecordDelta,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    let original = if let Some(slot) = &delta.slot {
        if slot.key != expected.key || state.pending_initial_protocols.contains_key(&slot.key) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        // Charge both independent map key and immutable full original before
        // cloning; only a genuine new Open/Seal delta reaches this insertion.
        proof
            .measure(&(&slot.key, slot))
            .map_err(|error| failure(proof, &error))?;
        Some((slot.key.clone(), slot.clone()))
    } else {
        None
    };
    state
        .pending_initial
        .publish(delta, proof)
        .map_err(|error| failure(proof, &error))?;
    if let Some((key, slot)) = original {
        state.pending_initial_protocols.insert(key, slot);
    }
    Ok(())
}
impl MemoryStore {
    fn pending_open_publication(
        &self,
        state: &State,
        original: &JoinedPreparation,
        expected: &Correlation,
        plan: (OpenRecord, Box<RecordDelta>),
        proof: &mut Proof,
    ) -> Result<(State, PendingInitialNativeReply), PreparationCommitError> {
        let mut verified = state.session_owners.verify_preparation_with_proof(
            original.observed_grant(),
            crate::memory::session_owner::revision(state, &expected.key.session_id),
            self.clock.now(),
            proof,
        ).map_err(|error| crate::session_owner::preparation::inspection::read_error(proof, error))?;
        verified.bind_observed_operation(original)?;
        let mut next = state.clone();
        publish(&mut next, expected, *plan.1, proof)?;
        let reply = PendingInitialNativeReply::opened(plan.0, proof)?;
        Ok((next, reply))
    }
    fn pending_seal_publication(
        state: &State,
        source: (
            &JoinedPreparation,
            &SessionContext,
            &Correlation,
            &protocol::prefix::HistoricalInitialPrefix<'_>,
        ),
        selected: (
            Bundle<'_>,
            &crate::native_enrollment::initial_consumer::Selection<'_>,
        ),
        witnesses: (
            Option<&crate::session_owner::owned_scope::InitialRetirementWitness<'_>>,
            Option<&crate::session_owner::owned_scope::OriginalPolicyRetirementView<'_>>,
            Option<&crate::session_owner::owned_scope::OwnedScopeActivation>,
        ),
        now: time::OffsetDateTime,
        proof: &mut Proof,
    ) -> Result<(Option<State>, PendingInitialNativeReply), PreparationCommitError> {
        let (original, context, expected, prefix) = source;
        let (view, consumer_view) = selected;
        let (witness, policy_tail, policy_activation) = witnesses;
        if let Some(witness) = witness {
            let local = witness.verify(original, context, expected, proof)?;
            if let Some(register) = consumer_view.registered_driver() {
                witness.verify_register(register, proof)?;
            }
            if let Some((saved, _)) = consumer_view.retirement() {
                let plan = crate::native_enrollment::initial_consumer::prepare_paired_policy_close(
                    consumer_view,
                    view,
                    prefix,
                    expected,
                    local.calls,
                    (
                        saved,
                        local.disposition,
                        consumer_view.close.and_then(|c| c.policy_terminal.as_ref()),
                    ),
                    proof,
                )?;
                return match plan {
                    crate::native_enrollment::initial_consumer::PairedCloseDecision::Replayed(
                        record,
                    ) => Ok((None, PendingInitialNativeReply::closed(*record, proof)?)),
                    crate::native_enrollment::initial_consumer::PairedCloseDecision::Publish(_) => {
                        Err(Error::UnknownStoreOutcome.into())
                    }
                };
            }
            let prepared = crate::memory::initial_consumer::retirement::prepare(
                state,
                (original, context, expected),
                consumer_view,
                witness,
                proof,
            )?;
            let policy_prepared = crate::memory::initial_consumer::policy::prepare_optional(
                state,
                (policy_tail, policy_activation),
                proof,
            )?;
            let mut next = state.clone();
            let policy_terminal = crate::memory::initial_consumer::policy::publish_terminal(
                &mut next,
                consumer_view,
                policy_prepared.as_ref(),
                now,
                proof,
            )?;
            let (retirement, disposition) = crate::memory::initial_consumer::retirement::publish(
                &mut next, prepared, now, proof,
            )?;
            return match crate::native_enrollment::initial_consumer::prepare_paired_policy_close(
                consumer_view,
                view,
                prefix,
                expected,
                local.calls,
                (&retirement, disposition, policy_terminal.as_ref()),
                proof,
            )? {
                crate::native_enrollment::initial_consumer::PairedCloseDecision::Publish(plan) => {
                    publish(&mut next, expected, *plan.birth, proof)?;
                    crate::memory::initial_consumer::publish(&mut next, plan.consumer, proof)
                        .map_err(|error| failure(proof, &error))?;
                    Ok((
                        Some(next),
                        PendingInitialNativeReply::closed(plan.record, proof)?,
                    ))
                }
                crate::native_enrollment::initial_consumer::PairedCloseDecision::Replayed(_) => {
                    Err(Error::UnknownStoreOutcome.into())
                }
            };
        }
        match protocol::prepare_close(prefix, expected, view, proof)? {
            CloseDecision::Publish { record, delta } => {
                let mut next = state.clone();
                publish(&mut next, expected, *delta, proof)?;
                Ok((
                    Some(next),
                    PendingInitialNativeReply::closed(record, proof)?,
                ))
            }
            CloseDecision::Replayed(record) => {
                Ok((None, PendingInitialNativeReply::closed(record, proof)?))
            }
            CloseDecision::Conflict => Err(Error::SemanticConflict.into()),
        }
    }
    fn pending_open_selected(
        &self,
        state: &State,
        source: (
            &JoinedPreparation,
            &Correlation,
            &protocol::prefix::HistoricalInitialPrefix<'_>,
        ),
        selected: (
            Bundle<'_>,
            &crate::native_enrollment::initial_consumer::Selection<'_>,
        ),
        purpose: (
            Option<&crate::session_owner::owned_scope::InitialResourceAssociation<'_>>,
            Option<&crate::session_owner::owned_scope::OwnedScopeActivation>,
        ),
        proof: &mut Proof,
    ) -> Result<(Option<State>, PendingInitialNativeReply), PreparationCommitError> {
        let (original, expected, prefix) = source;
        let (view, consumer_view) = selected;
        let (consumer, policy_activation) = purpose;
        match protocol::prepare_open(prefix, expected, view, proof)? {
            OpenDecision::Publish { record, delta } => {
                let consumer_delta = consumer
                    .map(|_| {
                        if let Some(activation) = policy_activation {
                            let policy = crate::memory::initial_consumer::policy::original(activation, proof)?;
                            return crate::native_enrollment::initial_consumer::prepare_fixed_anchor(prefix, &record, consumer_view, &policy, proof);
                        }
                        crate::native_enrollment::initial_consumer::prepare_anchor(
                            prefix,
                            &record,
                            consumer_view,
                            proof,
                        )
                    })
                    .transpose()?;
                let (mut next, reply) = self.pending_open_publication(
                    state,
                    original,
                    expected,
                    (record, delta),
                    proof,
                )?;
                if let Some(delta) = consumer_delta {
                    crate::memory::initial_consumer::publish(&mut next, delta, proof)
                        .map_err(|error| failure(proof, &error))?;
                }
                Ok((Some(next), reply))
            }
            OpenDecision::Replayed(record) => {
                if consumer.is_some() && consumer_view.empty() {
                    return Err(Error::Unsupported.into());
                }
                Ok((
                    None,
                    PendingInitialNativeReply::already_opened(record.epoch.correlation, proof)?,
                ))
            }
            OpenDecision::Closed(record) => {
                Ok((None, PendingInitialNativeReply::closed(record, proof)?))
            }
            OpenDecision::Conflict => Err(Error::SemanticConflict.into()),
        }
    }
    fn pending_complete_publication(
        &self,
        state: &State,
        original: &JoinedPreparation,
        verified: &protocol::VerifiedInitialCall<'_>,
        consumer: Option<(
            &crate::native_enrollment::initial_consumer::Selection<'_>,
            Bundle<'_>,
        )>,
        proof: &mut Proof,
    ) -> Result<(State, PendingInitialNativeReply), PreparationCommitError> {
        self.pending_initial_preflight(state, original, proof)?;
        let mut next = state.clone();
        let data = self.complete_pending_initial(&mut next, original, verified, proof)?;
        if let Some((selected, birth)) = consumer {
            let delta = crate::native_enrollment::initial_consumer::link_initial(
                selected, birth, &data, proof,
            )?;
            crate::memory::initial_consumer::publish(&mut next, delta, proof)
                .map_err(|error| failure(proof, &error))?;
        }
        Ok((next, PendingInitialNativeReply::completed(data)))
    }
    /// All native writes, including retirement and both closes, become visible
    /// by the one original State replacement after the last carrier charge.
    fn commit_pending(mut state: std::sync::MutexGuard<'_, State>, next: Option<State>) {
        if let Some(next) = next {
            *state = next;
        }
        // The guard is always released before the caller's after-commit hook.
    }
    fn verify_policy_ingress<'a>(
        &self,
        state: &State,
        selected: &crate::native_enrollment::initial_consumer::Selection<'_>,
        source: (&JoinedPreparation, &Correlation),
        purpose: (
            Option<&'a crate::session_owner::owned_scope::OriginalPolicyView<'a>>,
            Option<&crate::session_owner::owned_scope::InitialResourceAssociation<'_>>,
            Option<&crate::session_owner::owned_scope::OriginalPolicyRetirementView<'_>>,
            NativeInitialOperation,
        ),
        proof: &mut Proof,
    ) -> Result<
        Option<&'a crate::session_owner::owned_scope::OwnedScopeActivation>,
        PreparationCommitError,
    > {
        let (original, expected) = source;
        let (policy, association, retirement, operation) = purpose;
        let purpose = match (policy, association) {
            (Some(policy), Some(association)) => Some((policy, association)),
            (None, _) => None,
            _ => return Err(Error::Unsupported.into()),
        };
        let activation = crate::memory::initial_consumer::policy::verify_purpose(
            self, original, expected, selected, purpose, proof,
        )?;
        if let Some(activation) = activation
            && matches!(
                selected.origin,
                Some(crate::native_enrollment::initial_consumer::Origin::NoOpenedMember(_))
            )
        {
            crate::memory::initial_consumer::policy::verify_unopened_retirement(
                state, activation, proof,
            )
            .map_err(|error| failure(proof, &error))?;
        }
        if let Some(tail) = retirement {
            tail.verify_close(
                self,
                association.ok_or(Error::Unsupported)?,
                original,
                expected,
                proof,
            )?;
        }
        if (operation == NativeInitialOperation::Seal && activation.is_some())
            != retirement.is_some()
        {
            return Err(Error::Unsupported.into());
        }
        Ok(activation)
    }
    pub(in crate::memory) fn apply_pending_initial(
        &self,
        call: PendingInitialNativeCall<'_>,
    ) -> Result<PendingInitialNativeReply, PreparationCommitError> {
        let parts: crate::session_owner::owned_scope::NativeInitialParts<'_> =
            call.into_native()?;
        let original = parts.original;
        let expected = parts.expected;
        let proof = parts.proof;
        parts
            .selected
            .verify(original, proof)
            .map_err(|error| failure(proof, &error))?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state, parts.context.session_id())
            .map_err(crate::deletion::owner_error)?;
        verify_consumer_context(original, expected, parts.context, parts.consumer, proof)?;
        let prefix = historical(&state, original, parts.selected, proof)?;
        if prefix.correlation(&expected.identity, proof)? != *expected {
            return Err(Error::SemanticConflict.into());
        }
        // Authenticate every retained source member before any Raw historical
        // metadata return; a Raw result cannot hide a retained Pending slot.
        let view = selected(&state, expected, proof)?;
        let consumer_view = selected_consumer(&state, expected, view, parts.consumer, proof)?;
        let policy_activation = self.verify_policy_ingress(
            &state,
            &consumer_view,
            (original, expected),
            (
                parts.original_policy,
                parts.consumer,
                parts.policy_retirement,
                parts.operation,
            ),
            proof,
        )?;
        if let Some(raw) =
            Self::pending_initial_raw_completed(&state, original, expected, parts.selected, proof)?
        {
            if !consumer_view.empty() {
                return Err(Error::UnknownStoreOutcome.into());
            }
            return raw_completed_reply(raw, view, parts.operation, proof);
        }
        let (next, reply) = match parts.operation {
            NativeInitialOperation::Open => self.pending_open_selected(
                &state,
                (original, expected, &prefix),
                (view, &consumer_view),
                (parts.consumer, policy_activation),
                proof,
            )?,
            NativeInitialOperation::Seal => {
                let witness = seal_witness(parts.consumer, parts.retirement)?;
                let now = self.clock.now();
                #[cfg(test)]
                let now = parts
                    .policy_retirement
                    .map_or(now, |tail| tail.presentation_now(now));
                Self::pending_seal_publication(
                    &state,
                    (original, parts.context, expected, &prefix),
                    (view, &consumer_view),
                    (witness, parts.policy_retirement, policy_activation),
                    now,
                    proof,
                )?
            }
            NativeInitialOperation::Complete => {
                if let Some(reply) = published_initial_reply(view, expected, proof)? {
                    return Ok(reply);
                }
                if parts.consumer.is_some() && consumer_view.empty() {
                    return Err(Error::Unsupported.into());
                }
                let opened = parts.opened.ok_or(Error::Unsupported)?;
                if opened.correlation() != expected {
                    return Err(Error::SemanticConflict.into());
                }
                let verified =
                    protocol::verify_complete(&prefix, expected, parts.selected, view, proof)?;
                let (next, reply) = self.pending_complete_publication(
                    &state,
                    original,
                    &verified,
                    parts.consumer.map(|_| (&consumer_view, view)),
                    proof,
                )?;
                (Some(next), reply)
            }
        };
        #[cfg(test)]
        let committed = next.is_some();
        #[cfg(test)]
        if committed {
            before(self, parts.operation, proof)?;
        }
        Self::commit_pending(state, next);
        #[cfg(test)]
        if committed {
            after(self, parts.operation)?;
        }
        Ok(reply)
    }
}
#[cfg(test)]
fn before(
    store: &MemoryStore,
    operation: NativeInitialOperation,
    proof: &Proof,
) -> Result<(), PreparationCommitError> {
    // Observation only: this is the actual caller's cumulative usage after all
    // native carrier work and before the unchanged transaction commit/fault.
    store
        .pending_initial_test_state
        .precommit_usage
        .lock()
        .unwrap()
        .push((operation, proof.used()));
    if (operation == NativeInitialOperation::Seal
        && store
            .pending_initial_test_state
            .fixed_policy_seal_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1)
        || store
            .pending_initial_test_state
            .fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 1
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
#[cfg(test)]
fn after(
    store: &MemoryStore,
    operation: NativeInitialOperation,
) -> Result<(), PreparationCommitError> {
    if (operation == NativeInitialOperation::Seal
        && store
            .pending_initial_test_state
            .fixed_policy_seal_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2)
        || store
            .pending_initial_test_state
            .fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
