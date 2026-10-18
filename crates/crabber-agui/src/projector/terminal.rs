//! Wire envelope and terminal presentation, independent of native execution.
use ag_ui_core::event::{
    BaseEvent, Event, RunErrorEvent, RunFinishedEvent, RunStartedEvent, StepFinishedEvent,
};
use ag_ui_core::types::RunFinishedOutcome;

use super::{Completion, MessagePhase, ProjectionError, Projector, base};

impl Projector {
    pub(super) fn start_event(&self, base: BaseEvent) -> Event {
        Event::RunStarted(Box::new(RunStartedEvent {
            base,
            thread_id: self.thread_alias.clone().into(),
            run_id: self.run_alias.clone().into(),
            protocol_version: Some("1.0".into()),
            parent_run_id: None,
            input: None,
        }))
    }
    // A wire envelope can precede the native RunStarted callback. It conveys
    // selected aliases only and never changes source execution authority.
    pub(super) fn open_exchange(&mut self, base: &BaseEvent, output: &mut Vec<Event>) {
        if !self.lifecycle.wire_open {
            self.lifecycle.wire_open = true;
            output.push(self.start_event(base.clone()));
        }
    }
    pub(super) fn error_event(code: &str) -> Event {
        Event::RunError(RunErrorEvent {
            base: base(None),
            message: "The live run could not be projected.".into(),
            code: Some(code.into()),
            usage: None,
        })
    }
    pub(super) fn finished_event(&self, completion: Completion) -> Event {
        let outcome = match completion {
            Completion::Cancelled => RunFinishedOutcome::Cancelled,
            Completion::Paused => match self.waiting_outcome() {
                Ok(outcome) => outcome,
                Err(error) => return Self::error_event(error.code()),
            },
            _ => RunFinishedOutcome::Success {
                pending_tool_call_ids: None,
            },
        };
        Event::RunFinished(RunFinishedEvent {
            base: base(None),
            thread_id: self.thread_alias.clone().into(),
            run_id: self.run_alias.clone().into(),
            result: None,
            outcome: Some(outcome),
            usage: None,
        })
    }

    /// Emit the sole terminal after the receiver drains and `RunHandle::done` resolves.
    /// Repeated calls return an empty batch. Task failures outrank durable status hints.
    ///
    /// # Errors
    /// Returns a validation error if even the configured fixed error frame is invalid.
    /// Oversized variable closures or interrupts become one bounded error terminal.
    pub fn finish(&mut self, completion: Completion) -> Result<Vec<Event>, ProjectionError> {
        if self.lifecycle.terminal {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        self.open_exchange(&base(None), &mut output);
        self.check_completion(completion);
        self.close_finished_scopes(&mut output);
        output.push(self.terminal_event(completion));
        self.validate_terminal_batch(&mut output)?;
        self.lifecycle.terminal = true;
        Ok(output)
    }

    fn check_completion(&mut self, completion: Completion) {
        if self.active.is_some() {
            self.fail(ProjectionError::Order);
        }
        if self.ui_active.values().any(|id| {
            self.ui_messages
                .get(id)
                .is_some_and(|message| message.invocation.is_none())
        }) {
            self.fail(ProjectionError::Order);
        }
        if self.child_exchange()
            && completion == Completion::Completed
            && self.binding.child_terminal != Some(Completion::Completed)
        {
            self.fail(ProjectionError::SourceAttemptFailed);
        }
        if !self.child_exchange()
            && completion == Completion::Completed
            && (!(self.lifecycle.source_started || self.resume.ready)
                || self.settled != Some(Completion::Completed)
                || self.step.is_some()
                || self
                    .messages
                    .values()
                    .any(|m| m.public && m.phase != MessagePhase::Committed)
                || self.calls.values().any(|c| !c.settled))
        {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Cancelled
            && (self.child_exchange() || self.settled != Some(Completion::Cancelled))
        {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Paused && !self.paused {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Paused
            && let Err(error) = self.waiting_outcome()
        {
            self.fail(error);
        }
    }

    fn close_finished_scopes(&mut self, output: &mut Vec<Event>) {
        if let Some(id) = self.active.take() {
            output.extend(self.close_message(&id, &base(None)));
        }
        output.extend(self.close_presentations());
        output.extend(self.close_invocations());
        if let Some(step_name) = self.step.take() {
            output.push(Event::StepFinished(StepFinishedEvent {
                base: base(None),
                step_name,
            }));
        }
    }

    fn terminal_event(&self, completion: Completion) -> Event {
        if let Some(error) = self.fault {
            Self::error_event(error.code())
        } else if completion == Completion::LeaseLost {
            Self::error_event("crabber_lease_lost")
        } else if completion == Completion::Failed {
            Self::error_event("crabber_runtime_failed")
        } else {
            self.finished_event(completion)
        }
    }

    fn validate_terminal_batch(&mut self, output: &mut Vec<Event>) -> Result<(), ProjectionError> {
        if output.len() > self.config.max_batch {
            // An error terminal is valid without promising successful stream closure.
            // Never exceed the configured batch or silently drop a success terminal.
            output.clear();
            self.fail(ProjectionError::Limit);
            output.push(Self::error_event(
                self.fault.unwrap_or(ProjectionError::Limit).code(),
            ));
        }
        if let Err(error) = self.validate_batch(output.as_slice()) {
            self.fail(error);
            if error != ProjectionError::Limit {
                return Err(error);
            }
            output.clear();
            output.push(Self::error_event(self.fault.unwrap_or(error).code()));
            self.validate_batch(output.as_slice())?;
        }
        Ok(())
    }
}
