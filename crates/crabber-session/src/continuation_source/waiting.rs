//! Canonical safe event from actual frozen input and its acknowledged receipt.
use crate::{
    ApprovalOutcome, ApprovalReceipt, ApprovalSuspension, CallApproval, ToolContinuationOutcome,
    ToolContinuationReceipt, ToolContinuationSuspension,
};
use crabber_core::continuation_source::{
    AttemptIdentity, ContinuationIdentity, HumanInterrupt, WaitingSource,
};
use crabber_core::{
    ControlId, EventCursor, EventKind, EventRecord, InputFingerprint, RunId, SessionId,
    SessionRevision,
};

impl ApprovalSuspension {
    /// Materialize public waiting data; possession of a receipt grants no work.
    /// # Errors
    /// Rejects a receipt for different frozen input or malformed public summary.
    pub fn waiting_event(
        &self,
        receipt: &ApprovalReceipt,
    ) -> Result<EventRecord, crate::ApprovalError> {
        if receipt.operation_id != self.id
            || receipt.semantic_digest != self.fingerprint(&receipt.session_id, &receipt.run_id)?
            || receipt.outcome
                != (ApprovalOutcome::Suspended {
                    continuation: self.continuation.clone(),
                })
        {
            return Err(crate::ApprovalError::UnknownStoreOutcome);
        }
        let mut source = base(
            &receipt.session_id,
            &receipt.run_id,
            &self.attempt,
            &self.continuation,
            (
                &receipt.operation_id,
                &receipt.semantic_digest,
                receipt.revision,
                receipt.event_cursor,
            ),
            self.source_turn_id.as_ref(),
        );
        for call in &self.calls {
            source.backend_pending.push(call.target.call_id.clone());
            if let CallApproval::Ask { id, description } = &call.decision {
                source
                    .interrupts
                    .push(interrupt(id, &call.target.call_id, description));
            }
        }
        event(
            source,
            self.source_created_at
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
        )
        .map_err(|_| crate::ApprovalError::InvalidRequest)
    }
}
impl ToolContinuationSuspension {
    /// Materialize only frozen public identities and declared human prompts.
    /// # Errors
    /// Rejects mismatched receipt semantics or unbounded public summary.
    pub fn waiting_event(
        &self,
        receipt: &ToolContinuationReceipt,
    ) -> Result<EventRecord, crate::ToolContinuationError> {
        if receipt.operation_id != self.id
            || receipt.semantic_digest != self.fingerprint(&receipt.session_id, &receipt.run_id)?
            || receipt.outcome
                != (ToolContinuationOutcome::Suspended {
                    continuation: self.continuation.clone(),
                })
        {
            return Err(crate::ToolContinuationError::UnknownStoreOutcome);
        }
        let mut source = base(
            &receipt.session_id,
            &receipt.run_id,
            &self.attempt,
            &self.continuation,
            (
                &receipt.operation_id,
                &receipt.semantic_digest,
                receipt.revision,
                receipt.event_cursor,
            ),
            self.source_turn_id.as_ref(),
        );
        for call in &self.calls {
            if self
                .frontend
                .iter()
                .any(|frontend| frontend.target.call_id == call.target.call_id)
            {
                source.frontend_pending.push(call.target.call_id.clone());
            } else {
                source.backend_pending.push(call.target.call_id.clone());
            }
            if let CallApproval::Ask { id, description } = &call.decision {
                source
                    .interrupts
                    .push(interrupt(id, &call.target.call_id, description));
            }
        }
        for phase in &self.phases {
            for question in &phase.interactions {
                source.interrupts.push(interrupt(
                    &question.id,
                    &phase.previous.call_id,
                    &question.description,
                ));
            }
        }
        event(source, self.source_created_at)
            .map_err(|_| crate::ToolContinuationError::InvalidRequest)
    }
}
fn base(
    session: &SessionId,
    run: &RunId,
    attempt: &crate::AttemptTarget,
    continuation: &crate::ContinuationTarget,
    receipt: (&ControlId, &InputFingerprint, SessionRevision, EventCursor),
    turn: Option<&crabber_core::TurnId>,
) -> WaitingSource {
    WaitingSource {
        version: 1,
        session_id: session.clone(),
        run_id: run.clone(),
        attempt: AttemptIdentity {
            id: attempt.id.clone(),
            generation: attempt.generation,
        },
        current_turn: turn.cloned(),
        continuation: ContinuationIdentity {
            id: continuation.id.clone(),
            generation: continuation.generation,
        },
        operation_id: receipt.0.clone(),
        semantic_digest: receipt.1.clone(),
        revision: receipt.2,
        source_event_cursor: receipt.3,
        interrupts: Vec::new(),
        frontend_pending: Vec::new(),
        backend_pending: Vec::new(),
    }
}
fn interrupt(
    id: &crabber_core::InteractionId,
    call: &crabber_core::ToolCallId,
    description: &crabber_core::InteractionDescription,
) -> HumanInterrupt {
    HumanInterrupt {
        interaction_id: id.clone(),
        call_id: call.clone(),
        kind: description.kind(),
        name: description.name().to_owned(),
        prompt: description.prompt().to_owned(),
        response_schema: description.response_schema().clone(),
    }
}
fn event(
    source: WaitingSource,
    created_at: time::OffsetDateTime,
) -> Result<EventRecord, crabber_core::CoreError> {
    source.validate()?;
    Ok(EventRecord {
        cursor: None,
        session_id: source.session_id.clone(),
        run_id: source.run_id.clone(),
        turn_id: source.current_turn.clone(),
        kind: EventKind::ContinuationWaiting,
        correlation: Some(source.operation_id.0.clone()),
        live_only: false,
        payload: serde_json::to_value(source)
            .map_err(|_| crabber_core::CoreError::Validation("invalid waiting source".into()))?,
        created_at,
    })
}
