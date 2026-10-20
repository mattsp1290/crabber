//! Fixed message/event writes and terminal settlement after real resource joins.
use super::super::resources::ModelFailure;
use super::{
    Arc, ConsumerInput, ConsumerOutput, Error, FreshBegun, PreparationCommitError, Run, Step,
};
use crate::native_enrollment::initial_consumer::{CallKind, InitialLifecycleKind};
use crate::{
    AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, AttemptMutationOutcome,
    RunBoundary,
};
use crabber_core::{
    ContentBlock, ControlId, EventRecord, Message, MessageId, Part, PartId, PartKind, Role,
    RunStatus, Usage,
};

pub(super) enum TurnFailure {
    Native(PreparationCommitError),
    Model(ModelFailure),
    Stopped,
}
impl From<PreparationCommitError> for TurnFailure {
    fn from(error: PreparationCommitError) -> Self {
        Self::Native(error)
    }
}
impl Run<'_> {
    pub(super) async fn turn(
        &mut self,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), TurnFailure> {
        for kind in [
            InitialLifecycleKind::RunAdmitted,
            InitialLifecycleKind::RunStarted,
            InitialLifecycleKind::TurnStarted,
        ] {
            self.lifecycle(kind, serde_json::Value::Null).await?;
        }
        if self.retained.stopped() {
            return Err(TurnFailure::Stopped);
        }
        let resources = self
            .retained
            .consumer
            .as_ref()
            .ok_or(Error::Unsupported)
            .map_err(PreparationCommitError::from)?;
        let context = &self.retained.source.context;
        proof
            .measure(&(
                context.session_id(),
                context.workspace_id(),
                context.directory(),
            ))
            .map_err(|_| TurnFailure::Native(Error::Capacity.into()))?;
        self.effect_entered = true;
        let text = resources
            .model(context.clone(), proof)
            .await
            .map_err(TurnFailure::Model)?;
        if self.retained.stopped() {
            return Err(TurnFailure::Stopped);
        }
        let id = self.message(text).await?;
        self.lifecycle(
            InitialLifecycleKind::MessageCommitted,
            serde_json::json!({"message_id":id,"role":Role::Assistant}),
        )
        .await?;
        self.lifecycle(InitialLifecycleKind::TurnCompleted, serde_json::Value::Null)
            .await?;
        Ok(())
    }
    async fn lifecycle(
        &mut self,
        kind: InitialLifecycleKind,
        payload: serde_json::Value,
    ) -> Result<(), PreparationCommitError> {
        let mut step = Step::new(self.retained, kind.call_kind())?;
        step.proof
            .measure(&(
                self.retained.source.context.session_id(),
                &self.birth.assignment.run_id,
                &self.turn,
                &payload,
            ))
            .map_err(|_| Error::Capacity)?;
        let event = EventRecord {
            cursor: None,
            session_id: self.retained.source.context.session_id().clone(),
            run_id: self.birth.assignment.run_id.clone(),
            turn_id: Some(self.turn.clone()),
            kind: kind.event_kind(),
            payload,
            correlation: None,
            live_only: false,
            created_at: super::super::proof::created_at(&self.retained.calls),
        };
        let (output, record, _) = step
            .invoke(&ConsumerInput::Lifecycle {
                kind,
                event: &event,
            })
            .await?;
        if !matches!(output, ConsumerOutput::Lifecycle { .. }) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        self.accepted(&record)
    }
    async fn message(&mut self, text: String) -> Result<MessageId, PreparationCommitError> {
        let mut step = Step::new(self.retained, CallKind::AssistantMessage)?;
        let id = MessageId::new();
        step.proof
            .measure(&(
                self.retained.source.context.session_id(),
                &self.birth.assignment.run_id,
                &id,
            ))
            .map_err(|_| Error::Capacity)?;
        let message = Message {
            id: id.clone(),
            session_id: self.retained.source.context.session_id().clone(),
            run_id: Some(self.birth.assignment.run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: id,
                ordinal: 0,
                kind: PartKind::AssistantText,
                content: ContentBlock::Text { text },
            }],
            created_at: super::super::proof::created_at(&self.retained.calls),
        };
        let (output, record, mut proof) = step
            .invoke(&ConsumerInput::AssistantMessage(&message))
            .await?;
        if !matches!(output, ConsumerOutput::Message { .. }) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        self.accepted(&record)?;
        proof.measure(&message.id).map_err(|_| Error::Capacity)?;
        Ok(message.id)
    }
    pub(super) async fn conclude(
        &mut self,
        begun: &Arc<FreshBegun>,
        status: RunStatus,
    ) -> Result<(), PreparationCommitError> {
        let resources = self.retained.consumer.as_ref().ok_or(Error::Unsupported)?;
        let cleanup = resources.drain().await?;
        let mut step = Step::new(self.retained, CallKind::Conclude)?;
        step.proof
            .measure(&(&begun.receipt.attempt.target, &self.turn))
            .map_err(|_| Error::Capacity)?;
        let outcome = match status {
            RunStatus::Completed => AttemptFinish::Completed,
            RunStatus::Failed => AttemptFinish::Failed,
            RunStatus::Interrupted => AttemptFinish::Interrupted,
            _ => return Err(Error::InvalidTransition.into()),
        };
        let request = AttemptConclusion {
            finish: AttemptMutation {
                id: ControlId::new(),
                expected_revision: self.revision,
                command: AttemptCommand::Finish {
                    target: begun.receipt.attempt.target.clone(),
                    outcome,
                },
            },
            boundary: RunBoundary::Terminal {
                status,
                error: None,
                usage: Usage::default(),
            },
            created_at: super::super::proof::created_at(&self.retained.calls),
            turn_id: Some(self.turn.clone()),
            elapsed: super::super::proof::elapsed(&self.retained.calls, self.started),
        };
        let input = ConsumerInput::Conclude {
            request: &request,
            authority: &begun.authority,
            cleanup: &cleanup,
        };
        let result = if status == RunStatus::Interrupted {
            step.invoke_cleanup(&input, Arc::clone(begun)).await
        } else {
            step.invoke(&input).await
        }?;
        let ConsumerOutput::Attempt(outcome) = result.0 else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        if !matches!(
            *outcome,
            AttemptMutationOutcome::Applied {
                authority: None,
                ..
            }
        ) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        self.accepted(&result.1)
    }
}
