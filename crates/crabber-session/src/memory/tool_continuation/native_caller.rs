//! Actual same-lock compound inputs and fresh-only Raw publication boundary.
use super::{Error, State};
use crate::native_enrollment::caller::{
    Caller, Fresh,
    tool::{Tool, ToolCompletion, ToolInput},
};
use crate::public_session::v2::proof::Proof;
pub(super) fn digest(
    state: &State,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::InputFingerprint, Error> {
    let original = input.original();
    if !state
        .operation_receipts
        .keys()
        .any(|(session, id)| *session == original.session_id && id == input.id())
    {
        input
            .measure(proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    input.fingerprint()
}
pub(in crate::memory) fn selected(
    state: &State,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let run = selected_run(state, input, proof)?;
    super::super::approval::verify_session_with_proof(state, &run.session_id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::pending_in_session_with_proof(state, &run.session_id, proof)?;
    for parent in state
        .attempts
        .values()
        .filter(|p| p.snapshot.run_id == run.id)
    {
        crate::native_enrollment::caller::measure_record(parent, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for call in state.calls.values().filter(|c| c.run_id == run.id) {
        proof
            .measure(call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    for phase in state
        .phases
        .values()
        .filter(|p| p.snapshot.run_id == run.id)
    {
        crate::native_enrollment::caller::measure_phase_record(phase, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    super::super::native_parent::source_protocol::current(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::super::native_enrollment::original(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(())
}
pub(super) fn replay(
    state: &State,
    input: ToolInput<'_>,
    digest: &crabber_core::InputFingerprint,
    proof: &mut Proof,
) -> Result<Option<crate::ToolContinuationReceipt>, Error> {
    let original = input.original();
    let saved = super::receipt_with_proof(state, &original.session_id, input.id(), proof)
        .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?;
    if let Some(saved) = &saved {
        if saved.run_id != original.run_id || saved.semantic_digest != *digest {
            return Err(Error::SemanticConflict);
        }
        input.verify_receipt(saved)?;
    }
    Ok(saved)
}
pub(super) fn before(
    state: &State,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<Option<(ToolMutation, bool)>, Error> {
    let digest = digest(state, input, proof)?;
    if let Some(saved) = replay(state, input, &digest, proof)? {
        return Ok(Some((ToolMutation::Replayed(saved), false)));
    }
    selected(state, input, proof)?;
    Ok(None)
}
pub(super) fn selected_run(
    state: &State,
    input: ToolInput<'_>,
    proof: &mut Proof,
) -> Result<crabber_core::Run, Error> {
    let original = input.original();
    let run = state
        .runs
        .get(&original.run_id)
        .filter(|r| r.session_id == original.session_id)
        .ok_or(Error::NotFound)?;
    proof.measure(run).map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(run.clone())
}
pub(super) fn observe(
    state: &State,
    caller: Caller,
    done: ToolCompletion<'_>,
    outcome: &crate::ToolContinuationClaimOutcome,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<(), Error> {
    let crate::ToolContinuationClaimOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    super::super::native_enrollment::record_raw_native(
        state,
        caller,
        &Fresh::Tool(Tool {
            input: done.input,
            previous: done.previous,
            current: done.current,
            record: done.record,
            previous_parent: done.previous_parent,
            parent: done.parent,
            native_request: done.native_request,
            phases: done.phases,
            publication: done.publication,
            completion: done.completion,
            receipt,
            event,
            grant,
        }),
        proof,
        #[cfg(test)]
        probe,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}

impl crate::MemoryStore {
    pub(in crate::memory) fn tool_with_proof(
        &self,
        input: ToolInput<'_>,
        proof: &mut Proof,
    ) -> Result<crate::ToolContinuationClaimOutcome, Error> {
        match input {
            ToolInput::Claim(r) => self.claim_compound_with_proof(r, None, proof),
            ToolInput::FrontendClaim(r) => self.claim_compound_with_proof(&r.claim, Some(r), proof),
            ToolInput::Recovery(_) | ToolInput::FrontendRecovery(_) => {
                self.recover_compound_with_proof(input, proof)
            }
        }
    }
}

pub(super) fn issue(
    state: &State,
    caller: Caller,
    done: ToolCompletion<'_>,
    parts: crate::native_enrollment::caller::tool::ToolGrantParts,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<ToolMutation, Error> {
    let outcome = done.issue(parts, proof)?;
    let outcome = match preparation {
        Some(preparation) => ToolMutation::Prepared(
            crate::memory::session_owner::verify_tool(state, preparation.grant, outcome, proof)
                .map_err(|_| Error::UnknownStoreOutcome)?,
        ),
        None => ToolMutation::Applied(outcome),
    };
    observe(
        state,
        caller,
        done,
        outcome.outcome().ok_or(Error::UnknownStoreOutcome)?,
        proof,
        #[cfg(test)]
        probe,
    )?;
    Ok(outcome)
}

pub(super) fn finish(
    state: &mut State,
    caller: Caller,
    done: ToolCompletion<'_>,
    output: crate::native_enrollment::caller::tool::ToolGrantOutput,
    preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
    proof: &mut Proof,
    #[cfg(test)] probe: &crate::native_enrollment::caller::Probe,
) -> Result<ToolMutation, Error> {
    proof
        .measure(&done.parent.snapshot)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let outcome = if done.input.target().is_some() {
        done.record
            .recovered_outcome(done.parent.snapshot.clone())?
    } else {
        done.record.claimed_outcome(done.parent.snapshot.clone())
    };
    let receipt = super::publish_with_proof(
        state,
        done.previous,
        done.input.id().clone(),
        output.digest,
        outcome,
        done.current.updated_at,
        proof,
    )?;
    issue(
        state,
        caller,
        done,
        crate::native_enrollment::caller::tool::ToolGrantParts {
            fence: output.fence,
            authority: output.authority,
            phases: output.phases,
            frontend: output.frontend,
            receipt,
        },
        preparation,
        proof,
        #[cfg(test)]
        probe,
    )
}

pub(super) fn save_record(
    state: &mut State,
    record: &crate::tool_continuation::ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof
        .measure(record)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    record.validate()?;
    state
        .tool_continuations
        .insert(record.batch.continuation.id.clone(), record.clone());
    Ok(())
}

pub(in crate::memory) enum ToolMutation {
    Applied(crate::ToolContinuationClaimOutcome),
    Prepared(crate::memory::session_owner::VerifiedTool),
    Replayed(crate::ToolContinuationReceipt),
}
impl ToolMutation {
    pub(in crate::memory) fn into_outcome(self) -> crate::ToolContinuationClaimOutcome {
        match self {
            Self::Applied(v) => v,
            Self::Prepared(v) => v.into_outcome(),
            Self::Replayed(r) => crate::ToolContinuationClaimOutcome::Replayed(r),
        }
    }
    fn outcome(&self) -> Option<&crate::ToolContinuationClaimOutcome> {
        match self {
            Self::Applied(v) => Some(v),
            Self::Prepared(v) => Some(v.outcome()),
            Self::Replayed(_) => None,
        }
    }
}
