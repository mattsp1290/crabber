use crate::{
    AttemptAuthority, PhaseAuthority, PhaseCommand, PhaseError, PhaseMutation, PhaseQuiescence,
    PhaseQuiescenceResult, PhaseSnapshot, PhaseState, PhaseTarget, UncertainPhaseChoice,
};
use crate::{AttemptCleanup, AttemptState, attempt::AttemptRecord};
use crabber_core::{InputFingerprint, PhaseGeneration, ToolOutput};
use crabber_core::{Run, RunStatus, ToolCallRecord, ToolCallStatus};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

/// Private committed state. Neither native tokens nor the final output belong
/// in metadata responses. A committed outcome alone does not prove quiescence.
#[derive(Clone)]
pub(crate) struct PhaseRecord {
    pub snapshot: PhaseSnapshot,
    pub token: String,
    pub output: Option<ToolOutput>,
}
impl PhaseRecord {
    pub(crate) fn validate(&self) -> Result<(), PhaseError> {
        crate::phase::validate_snapshot(&self.snapshot)?;
        if self.token.is_empty()
            || self.token.len() > 512
            || !self.token.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        let valid = match (&self.output, self.snapshot.state) {
            (Some(output), PhaseState::Completed) => !output.is_error(),
            (Some(output), PhaseState::Failed) => output.is_error(),
            (
                None,
                PhaseState::Claimed
                | PhaseState::Started
                | PhaseState::Suspended
                | PhaseState::Uncertain,
            ) => true,
            _ => false,
        };
        if !valid {
            return Err(PhaseError::UnknownStoreOutcome);
        }
        Ok(())
    }

    pub(crate) fn claim(
        run: &Run,
        attempt: &AttemptRecord,
        call: &ToolCallRecord,
        request: &PhaseMutation,
        authority: &AttemptAuthority,
        now: OffsetDateTime,
        previous: Option<&PhaseSnapshot>,
    ) -> Result<Self, PhaseError> {
        crate::phase::validate_request(request)?;
        if !matches!(request.command, PhaseCommand::Claim { .. }) {
            return Err(PhaseError::InvalidTransition);
        }
        let target = request.command.target();
        verify_attempt(run, attempt, target, authority, now)?;
        if run.status != RunStatus::Running
            || call.status != ToolCallStatus::Pending
            || call.result.is_some()
            || attempt.snapshot.state != AttemptState::Started
        {
            return Err(PhaseError::InvalidTransition);
        }
        if call.run_id != run.id || call.id != target.call_id {
            return Err(PhaseError::NotFound);
        }
        // A suspended phase can only be resumed by the future compound
        // checkpoint/barrier operation, never by an ordinary fresh claim.
        if previous.is_some() {
            return Err(PhaseError::Busy);
        }
        if target.generation != PhaseGeneration::INITIAL {
            return Err(PhaseError::GenerationConflict);
        }
        Ok(Self {
            snapshot: PhaseSnapshot {
                session_id: run.session_id.clone(),
                run_id: run.id.clone(),
                target: target.clone(),
                call_binding: call_binding(call)?,
                state: PhaseState::Claimed,
                quiescence: PhaseQuiescence::Pending,
            },
            token: uuid::Uuid::new_v4().to_string(),
            output: None,
        })
    }

    pub(crate) fn authority(
        &self,
        attempt: AttemptAuthority,
    ) -> Result<PhaseAuthority, PhaseError> {
        PhaseAuthority::from_store(self.snapshot.clone(), self.token.clone(), attempt)
    }

    pub(crate) fn change(
        &mut self,
        run: &Run,
        attempt: &AttemptRecord,
        call: &ToolCallRecord,
        request: &PhaseMutation,
        authority: Option<&PhaseAuthority>,
        now: OffsetDateTime,
    ) -> Result<(), PhaseError> {
        self.validate()?;
        crate::phase::validate_request(request)?;
        if self.snapshot.session_id != run.session_id
            || self.snapshot.run_id != run.id
            || self.snapshot.target != *request.command.target()
            || attempt.snapshot.target != self.snapshot.target.attempt
            || attempt.snapshot.run_id != run.id
            || attempt.snapshot.session_id != run.session_id
            || call.run_id != run.id
            || call.id != self.snapshot.target.call_id
        {
            return Err(PhaseError::NotFound);
        }
        if call_binding(call)? != self.snapshot.call_binding {
            return Err(PhaseError::SemanticConflict);
        }
        match &request.command {
            PhaseCommand::MarkExpiredUncertain { .. } => {
                if attempt.lease_until > now && attempt.run_token == run.claim_token {
                    return Err(PhaseError::LiveOwner);
                }
                self.mark_uncertain()?;
            }
            PhaseCommand::MarkStoppedUncertain { .. } => self.mark_uncertain()?,
            PhaseCommand::ResolveUncertain { choice, .. } => self.resolve(choice)?,
            PhaseCommand::RecordQuiescence {
                result, host_claim, ..
            } => {
                if host_claim.is_none() {
                    self.verify_worker(run, attempt, authority, now)?;
                }
                self.record_quiescence(*result)?;
            }
            PhaseCommand::Begin { .. } | PhaseCommand::Complete { .. } => {
                self.verify_worker(run, attempt, authority, now)?;
                let expected_call_state = match request.command {
                    PhaseCommand::Begin { .. } => ToolCallStatus::Pending,
                    _ => ToolCallStatus::Running,
                };
                if call.status != expected_call_state || call.result.is_some() {
                    return Err(PhaseError::InvalidTransition);
                }
                if attempt.snapshot.state != AttemptState::Started {
                    return Err(PhaseError::StaleOwner);
                }
                self.worker_change(run, &request.command)?;
            }
            PhaseCommand::Claim { .. } => return Err(PhaseError::InvalidTransition),
        }
        Ok(())
    }

    fn verify_worker(
        &self,
        run: &Run,
        attempt: &AttemptRecord,
        authority: Option<&PhaseAuthority>,
        now: OffsetDateTime,
    ) -> Result<(), PhaseError> {
        let authority = authority.ok_or(PhaseError::StaleOwner)?;
        if !authority.matches_store(&self.snapshot, &self.token) {
            return Err(PhaseError::StaleOwner);
        }
        verify_parent(run, attempt, &self.snapshot.target, now)?;
        if !authority.matches_attempt_store(&attempt.snapshot, &attempt.token, &attempt.run_token) {
            return Err(PhaseError::StaleOwner);
        }
        Ok(())
    }

    fn worker_change(&mut self, run: &Run, command: &PhaseCommand) -> Result<(), PhaseError> {
        if self.snapshot.state == PhaseState::Uncertain {
            return Err(PhaseError::Uncertain);
        }
        match command {
            PhaseCommand::Begin { .. } => {
                if self.snapshot.state != PhaseState::Claimed || run.status != RunStatus::Running {
                    return Err(PhaseError::InvalidTransition);
                }
                self.snapshot.state = PhaseState::Started;
            }
            PhaseCommand::Complete { output, .. } => {
                if self.snapshot.state != PhaseState::Started {
                    return Err(PhaseError::InvalidTransition);
                }
                self.set_output(output.clone());
            }
            _ => return Err(PhaseError::InvalidTransition),
        }
        Ok(())
    }

    fn mark_uncertain(&mut self) -> Result<(), PhaseError> {
        if !matches!(
            self.snapshot.state,
            PhaseState::Claimed | PhaseState::Started
        ) {
            return Err(PhaseError::InvalidTransition);
        }
        self.snapshot.state = PhaseState::Uncertain;
        // Reconciliation must never revive the old worker's authority.
        self.token = uuid::Uuid::new_v4().to_string();
        Ok(())
    }

    fn resolve(&mut self, choice: &UncertainPhaseChoice) -> Result<(), PhaseError> {
        if self.snapshot.state != PhaseState::Uncertain {
            return Err(PhaseError::InvalidTransition);
        }
        match choice {
            UncertainPhaseChoice::Success { output } => self.set_output(output.clone()),
            UncertainPhaseChoice::Failure { output } => {
                let output = match output {
                    Some(output) => output.clone().with_error(),
                    None => ToolOutput::new(vec![], true)
                        .map_err(|_| PhaseError::UnknownStoreOutcome)?,
                };
                self.set_output(output);
            }
            UncertainPhaseChoice::Refuse => (),
        }
        Ok(())
    }

    fn set_output(&mut self, output: ToolOutput) {
        self.snapshot.state = if output.is_error() {
            PhaseState::Failed
        } else {
            PhaseState::Completed
        };
        self.output = Some(output);
    }

    fn record_quiescence(&mut self, result: PhaseQuiescenceResult) -> Result<(), PhaseError> {
        if matches!(
            self.snapshot.state,
            PhaseState::Claimed | PhaseState::Started
        ) || self.snapshot.quiescence == PhaseQuiescence::Complete
        {
            return Err(PhaseError::InvalidTransition);
        }
        let next = match result {
            PhaseQuiescenceResult::Complete => PhaseQuiescence::Complete,
            PhaseQuiescenceResult::Incomplete => PhaseQuiescence::Incomplete,
        };
        if self.snapshot.quiescence == next {
            return Err(PhaseError::InvalidTransition);
        }
        self.snapshot.quiescence = next;
        Ok(())
    }
}

fn verify_attempt(
    run: &Run,
    attempt: &AttemptRecord,
    target: &PhaseTarget,
    authority: &AttemptAuthority,
    now: OffsetDateTime,
) -> Result<(), PhaseError> {
    verify_parent(run, attempt, target, now)?;
    if !authority.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token) {
        return Err(PhaseError::StaleOwner);
    }
    Ok(())
}
fn verify_parent(
    run: &Run,
    attempt: &AttemptRecord,
    target: &PhaseTarget,
    now: OffsetDateTime,
) -> Result<(), PhaseError> {
    attempt
        .validate()
        .map_err(|_| PhaseError::UnknownStoreOutcome)?;
    if attempt.snapshot.run_id != run.id
        || attempt.snapshot.session_id != run.session_id
        || attempt.snapshot.target != target.attempt
        || attempt.snapshot.state == AttemptState::Unstarted
        || attempt.snapshot.cleanup == AttemptCleanup::Complete
        || attempt.run_token != run.claim_token
        || attempt.lease_until <= now
    {
        return Err(PhaseError::StaleOwner);
    }
    Ok(())
}

fn call_binding(call: &ToolCallRecord) -> Result<InputFingerprint, PhaseError> {
    let mut value = serde_json::to_value((
        "crabber.phase-call.v1",
        &call.run_id,
        &call.id,
        &call.name,
        &call.arguments,
        call.retry_safe,
    ))
    .map_err(|_| PhaseError::InvalidRequest)?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
    .map_err(|_| PhaseError::InvalidRequest)
}
