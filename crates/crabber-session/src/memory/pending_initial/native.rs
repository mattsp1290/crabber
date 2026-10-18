//! Actual opaque ingress and first publishers, compiled only by the native root.
use super::{failure, routes};
use crate::memory::{MemoryStore, State};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crate::native_enrollment::pending_initial::{
    self as protocol, ActualRawMetadata, Bundle, CloseDecision, Correlation,
    OpenDecision, OpenRecord, RecordDelta,
};
use crate::session_owner::owned_scope::{
    NativeInitialOperation, PendingInitialNativeCall, PendingInitialNativeReply,
};
use crabber_core::SessionContext;
fn selected<'a>(
    state: &'a State,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Bundle<'a>, PreparationCommitError> {
    routes(state, &expected.key.session_id, proof).map_err(|error| failure(&error))?;
    let view = state
        .pending_initial
        .selected(&expected.key, &expected.identity, proof)
        .map_err(|error| failure(&error))?;
    if state.pending_initial_protocols.contains_key(&expected.key) == view.empty() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(view)
}
fn historical<'a>(
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
fn verify_context(
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
fn publish(
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
            .map_err(|error| failure(&error))?;
        Some((slot.key.clone(), slot.clone()))
    } else {
        None
    };
    state
        .pending_initial
        .publish(delta, proof)
        .map_err(|error| failure(&error))?;
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
        )?;
        verified.bind_observed_operation(original)?;
        let mut next = state.clone();
        publish(&mut next, expected, *plan.1, proof)?;
        let reply = PendingInitialNativeReply::opened(plan.0, proof)?;
        Ok((next, reply))
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
            .map_err(|error| failure(&error))?;
        let mut state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state, parts.context.session_id())
            .map_err(crate::deletion::owner_error)?;
        verify_context(original, expected, parts.context)?;
        let prefix = historical(&state, original, parts.selected, proof)?;
        if prefix.correlation(&expected.identity, proof)? != *expected {
            return Err(Error::SemanticConflict.into());
        }
        // Authenticate every retained source member before any Raw historical
        // metadata return; a Raw result cannot hide a retained Pending slot.
        let view = selected(&state, expected, proof)?;
        if let Some(raw) =
            Self::pending_initial_raw_completed(&state, original, expected, parts.selected, proof)?
        {
            return raw_completed_reply(raw, view, parts.operation, proof);
        }
        match parts.operation {
            NativeInitialOperation::Open => {
                match protocol::prepare_open(&prefix, expected, view, proof)? {
                    OpenDecision::Publish { record, delta } => {
                        let (next, reply) = self.pending_open_publication(
                            &state,
                            original,
                            expected,
                            (record, delta),
                            proof,
                        )?;
                        #[cfg(test)]
                        before(self, parts.operation, proof)?;
                        *state = next;
                        drop(state);
                        #[cfg(test)]
                        after(self)?;
                        Ok(reply)
                    }
                    OpenDecision::Replayed(record) => {
                        PendingInitialNativeReply::already_opened(record.epoch.correlation, proof)
                    }
                    OpenDecision::Closed(record) => {
                        PendingInitialNativeReply::closed(record, proof)
                    }
                    OpenDecision::Conflict => Err(Error::SemanticConflict.into()),
                }
            }
            NativeInitialOperation::Seal => {
                match protocol::prepare_close(&prefix, expected, view, proof)? {
                    CloseDecision::Publish { record, delta } => {
                        let mut next = state.clone();
                        publish(&mut next, expected, *delta, proof)?;
                        let reply = PendingInitialNativeReply::closed(record, proof)?;
                        #[cfg(test)]
                        before(self, parts.operation, proof)?;
                        *state = next;
                        drop(state);
                        #[cfg(test)]
                        after(self)?;
                        Ok(reply)
                    }
                    CloseDecision::Replayed(record) => {
                        PendingInitialNativeReply::closed(record, proof)
                    }
                    CloseDecision::Conflict => Err(Error::SemanticConflict.into()),
                }
            }
            NativeInitialOperation::Complete => {
                if let Some(assignment) = view.assignment {
                    if assignment.correlation != *expected {
                        return Err(Error::SemanticConflict.into());
                    }
                    proof.measure(assignment).map_err(|error| failure(&error))?;
                    return PendingInitialNativeReply::already_published(assignment.clone(), proof);
                }
                let opened = parts.opened.ok_or(Error::Unsupported)?;
                if opened.correlation() != expected {
                    return Err(Error::SemanticConflict.into());
                }
                let verified =
                    protocol::verify_complete(&prefix, expected, parts.selected, view, proof)?;
                self.pending_initial_preflight(&state, original, proof)?;
                let mut next = state.clone();
                let data = self.complete_pending_initial(&mut next, original, &verified, proof)?;
                #[cfg(test)]
                before(self, parts.operation, proof)?;
                let reply = PendingInitialNativeReply::completed(data);
                *state = next;
                drop(state);
                #[cfg(test)]
                after(self)?;
                Ok(reply)
            }
        }
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
        .pending_initial_test_state.precommit_usage
        .lock()
        .unwrap()
        .push((operation, proof.used()));
    if store
        .pending_initial_test_state.fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 1
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
#[cfg(test)]
fn after(store: &MemoryStore) -> Result<(), PreparationCommitError> {
    if store
        .pending_initial_test_state.fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 2
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
