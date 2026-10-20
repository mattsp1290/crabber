//! Only the compound checkpoint continuation may create a later native phase.
use super::{
    AttemptRecord, AttemptState, OffsetDateTime, PhaseAuthority, PhaseError, PhaseQuiescence,
    PhaseRecord, PhaseSnapshot, PhaseState, Run, ToolCallRecord, ToolCallStatus, call_binding,
};
impl PhaseRecord {
    /// This initial phase processes an authenticated external result. It is
    /// reserved only by the compound continuation transaction, before Begin.
    pub(crate) fn claim_frontend_result(
        run: &Run,
        parent: &AttemptRecord,
        call: &ToolCallRecord,
        frozen: &crate::FrozenFrontendCall,
    ) -> Result<Self, PhaseError> {
        parent
            .validate()
            .map_err(|_| PhaseError::UnknownStoreOutcome)?;
        frozen.validate().map_err(|_| PhaseError::InvalidRequest)?;
        if run.status != crabber_core::RunStatus::Running
            || parent.snapshot.state != AttemptState::Unstarted
            || parent.snapshot.cleanup != crate::AttemptCleanup::Pending
            || parent.snapshot.target != frozen.target.attempt
            || parent.snapshot.session_id != run.session_id
            || parent.snapshot.run_id != run.id
            || parent.run_token != run.claim_token
            || call.run_id != run.id
            || call.id != frozen.target.call_id
            || call.status != ToolCallStatus::Waiting
            || call.result.is_some()
            || call_binding(call)? != frozen.call_binding
        {
            return Err(PhaseError::InvalidTransition);
        }
        let next = Self {
            snapshot: PhaseSnapshot {
                session_id: run.session_id.clone(),
                run_id: run.id.clone(),
                target: frozen.target.clone(),
                call_binding: frozen.call_binding.clone(),
                state: PhaseState::Claimed,
                quiescence: PhaseQuiescence::Pending,
            },
            token: uuid::Uuid::new_v4().to_string(),
            output: None,
        };
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn suspend_checkpoint(
        &mut self,
        run: &Run,
        parent: &AttemptRecord,
        call: &ToolCallRecord,
        frozen: &crate::FrozenToolPhase,
        authority: &PhaseAuthority,
        now: OffsetDateTime,
    ) -> Result<(), PhaseError> {
        self.validate()?;
        self.verify_worker(run, parent, Some(authority), now)?;
        if self.snapshot.target != frozen.previous
            || self.snapshot.call_binding != frozen.call_binding
            || call_binding(call)? != frozen.call_binding
            || call.id != frozen.previous.call_id
            || call.run_id != run.id
            || call.status != ToolCallStatus::Running
            || call.result.is_some()
            || self.snapshot.state != PhaseState::Started
            || self.snapshot.quiescence != PhaseQuiescence::Pending
        {
            return Err(PhaseError::InvalidTransition);
        }
        self.snapshot.state = PhaseState::Suspended;
        self.snapshot.quiescence = PhaseQuiescence::Complete;
        self.token = uuid::Uuid::new_v4().to_string();
        self.validate()
    }
    pub(crate) fn resume_checkpoint(
        &self,
        run: &Run,
        parent: &AttemptRecord,
        call: &ToolCallRecord,
        frozen: &crate::FrozenToolPhase,
    ) -> Result<Self, PhaseError> {
        self.validate()?;
        parent
            .validate()
            .map_err(|_| PhaseError::UnknownStoreOutcome)?;
        if self.snapshot.target != frozen.previous
            || self.snapshot.session_id != run.session_id
            || self.snapshot.run_id != run.id
            || self.snapshot.call_binding != frozen.call_binding
            || call_binding(call)? != frozen.call_binding
            || call.run_id != run.id
            || call.id != frozen.next.call_id
            || call.status != ToolCallStatus::Waiting
            || call.result.is_some()
            || self.snapshot.state != PhaseState::Suspended
            || self.snapshot.quiescence != PhaseQuiescence::Complete
            || parent.snapshot.target != frozen.next.attempt
            || parent.snapshot.state != AttemptState::Unstarted
            || parent.snapshot.run_id != run.id
            || parent.snapshot.session_id != run.session_id
            || frozen.previous.generation.next().ok() != Some(frozen.next.generation)
        {
            return Err(PhaseError::InvalidTransition);
        }
        let next = Self {
            snapshot: PhaseSnapshot {
                session_id: run.session_id.clone(),
                run_id: run.id.clone(),
                target: frozen.next.clone(),
                call_binding: frozen.call_binding.clone(),
                state: PhaseState::Claimed,
                quiescence: PhaseQuiescence::Pending,
            },
            token: uuid::Uuid::new_v4().to_string(),
            output: None,
        };
        next.validate()?;
        Ok(next)
    }
}
