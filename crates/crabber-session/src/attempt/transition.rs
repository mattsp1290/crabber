use crate::{
    AttemptAuthority, AttemptCleanup, AttemptCleanupResult, AttemptCommand, AttemptError,
    AttemptMutation, AttemptSnapshot, AttemptState,
};
use crabber_core::{AttemptGeneration, Run, RunFence, RunStatus};
use time::OffsetDateTime;

#[derive(Clone)]
pub(crate) struct AttemptRecord {
    pub snapshot: AttemptSnapshot,
    pub owner: String,
    pub token: String,
    pub run_token: String,
    pub lease_until: OffsetDateTime,
}
impl AttemptRecord {
    pub(crate) fn validate(&self) -> Result<(), AttemptError> {
        if self.owner.is_empty()
            || self.owner.len() > 512
            || self.owner.chars().any(char::is_control)
            || matches!(
                self.snapshot.state,
                AttemptState::Unstarted | AttemptState::Started
            ) && self.snapshot.cleanup != AttemptCleanup::Pending
        {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        for token in [&self.token, &self.run_token] {
            if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic())
            {
                return Err(AttemptError::UnknownStoreOutcome);
            }
        }
        Ok(())
    }
    pub(crate) fn authority(&self) -> Result<AttemptAuthority, AttemptError> {
        self.validate()?;
        AttemptAuthority::from_store(
            self.snapshot.clone(),
            self.token.clone(),
            self.run_token.clone(),
        )
        .map_err(|_| AttemptError::UnknownStoreOutcome)
    }
    pub(crate) fn reserve(
        run: &Run,
        mutation: &AttemptMutation,
        latest: Option<&AttemptSnapshot>,
    ) -> Result<Self, AttemptError> {
        if !matches!(
            mutation.command,
            AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
        ) || !matches!(run.status, RunStatus::Running | RunStatus::Paused)
        {
            return Err(AttemptError::InvalidTransition);
        }
        if matches!(mutation.command, AttemptCommand::ReserveBound { .. }) && latest.is_some() {
            return Err(AttemptError::InvalidTransition);
        }
        let expected = if let Some(previous) = latest {
            if previous.cleanup != AttemptCleanup::Complete
                || matches!(
                    previous.state,
                    AttemptState::Unstarted | AttemptState::Started | AttemptState::OutcomeUnknown
                )
            {
                return Err(AttemptError::Busy);
            }
            if previous.state != AttemptState::Suspended {
                return Err(AttemptError::InvalidTransition);
            }
            previous
                .target
                .generation
                .next()
                .map_err(|_| AttemptError::GenerationExhausted)?
        } else {
            if run.status == RunStatus::Paused {
                return Err(AttemptError::InvalidTransition);
            }
            AttemptGeneration::INITIAL
        };
        if mutation.command.target().generation != expected {
            return Err(AttemptError::GenerationConflict);
        }
        Ok(Self {
            snapshot: AttemptSnapshot {
                session_id: run.session_id.clone(),
                run_id: run.id.clone(),
                target: mutation.command.target().clone(),
                state: AttemptState::Unstarted,
                cleanup: AttemptCleanup::Pending,
            },
            owner: run.owner.clone(),
            token: uuid::Uuid::new_v4().to_string(),
            run_token: run.claim_token.clone(),
            lease_until: run.lease_until,
        })
    }
    pub(crate) fn change(
        &mut self,
        run: &Run,
        mutation: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        now: OffsetDateTime,
        phases_quiescent: bool,
    ) -> Result<bool, AttemptError> {
        self.validate()?;
        if self.snapshot.run_id != run.id
            || self.snapshot.session_id != run.session_id
            || self.snapshot.target != *mutation.command.target()
        {
            return Err(AttemptError::NotFound);
        }
        if matches!(mutation.command, AttemptCommand::ReclaimUnstarted { .. }) {
            if self.snapshot.state != AttemptState::Unstarted || run.status != RunStatus::Running {
                return Err(AttemptError::InvalidTransition);
            }
            if self.lease_until > now {
                return Err(AttemptError::LiveOwner);
            }
            self.owner.clone_from(&run.owner);
            self.run_token.clone_from(&run.claim_token);
            self.token = uuid::Uuid::new_v4().to_string();
            self.lease_until = run.lease_until;
            return Ok(true);
        }
        let authority = authority.ok_or(AttemptError::StaleOwner)?;
        if !authority.matches_store(&self.snapshot, &self.token, &self.run_token)
            || self.run_token != run.claim_token
            || self.lease_until <= now
        {
            return Err(AttemptError::StaleOwner);
        }
        match mutation.command {
            AttemptCommand::Begin { .. } => {
                if self.snapshot.state != AttemptState::Unstarted
                    || run.status != RunStatus::Running
                {
                    return Err(AttemptError::InvalidTransition);
                }
                self.snapshot.state = AttemptState::Started;
            }
            AttemptCommand::Finish { outcome, .. } => {
                if self.snapshot.state != AttemptState::Started {
                    return Err(AttemptError::InvalidTransition);
                }
                self.snapshot.state = outcome.into();
            }
            AttemptCommand::RecordCleanup { result, .. } => {
                self.cleanup(result, phases_quiescent)?;
            }
            AttemptCommand::ReserveBound { .. }
            | AttemptCommand::Reserve { .. }
            | AttemptCommand::ReclaimUnstarted { .. } => {
                return Err(AttemptError::InvalidTransition);
            }
        }
        Ok(false)
    }
    pub(crate) fn cleanup(
        &mut self,
        result: AttemptCleanupResult,
        phases_quiescent: bool,
    ) -> Result<(), AttemptError> {
        if matches!(
            self.snapshot.state,
            AttemptState::Unstarted | AttemptState::Started
        ) || self.snapshot.cleanup == AttemptCleanup::Complete
        {
            return Err(AttemptError::InvalidTransition);
        }
        let next = result.into();
        if next == self.snapshot.cleanup {
            return Err(AttemptError::InvalidTransition);
        }
        if next == AttemptCleanup::Complete && !phases_quiescent {
            return Err(AttemptError::CleanupBlocked);
        }
        self.snapshot.cleanup = next;
        Ok(())
    }
}
pub(crate) fn verify_run(
    run: &Run,
    fence: &RunFence,
    now: OffsetDateTime,
) -> Result<(), AttemptError> {
    if run.id != fence.run_id
        || run.claim_token != fence.claim_token
        || run.status.is_terminal()
        || run.lease_until <= now
    {
        return Err(AttemptError::StaleOwner);
    }
    Ok(())
}

#[cfg(test)]
#[path = "transition_tests.rs"]
mod tests;
