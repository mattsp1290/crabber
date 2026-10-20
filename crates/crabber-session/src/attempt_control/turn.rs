//! Derive source identity from durable records, never caller presentation fields.
use crate::AttemptError;
use crabber_core::{EventKind, EventRecord, Run, TurnId};
use time::OffsetDateTime;
#[derive(Default)]
pub(crate) struct TurnContext {
    pub source: Option<TurnId>,
    pub close: Option<TurnId>,
}
pub(crate) struct TurnState {
    open: Option<TurnId>,
    checkpoint: Option<TurnId>,
    checkpoint_closed: bool,
    last: Option<TurnId>,
}
impl TurnState {
    pub(crate) fn new(run: &Run, source: Option<TurnId>) -> Result<Self, AttemptError> {
        let checkpoint = source.or(run
            .checkpoint
            .as_ref()
            .and_then(|v| v.get("source_turn_id"))
            .filter(|v| !v.is_null())
            .map(|v| {
                serde_json::from_value::<TurnId>(v.clone())
                    .map_err(|_| AttemptError::UnknownStoreOutcome)
            })
            .transpose()?);
        if checkpoint.as_ref().is_some_and(|id| {
            id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control)
        }) {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        Ok(Self {
            open: None,
            checkpoint,
            checkpoint_closed: false,
            last: None,
        })
    }
    pub(crate) fn observe(&mut self, run: &Run, event: &EventRecord) -> Result<(), AttemptError> {
        if event.run_id != run.id || event.session_id != run.session_id || event.live_only {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        let Some(id) = &event.turn_id else {
            if event.kind == EventKind::TurnStarted
                || (event.kind == EventKind::TurnCompleted && self.open.is_some())
            {
                return Err(AttemptError::UnknownStoreOutcome);
            }
            return Ok(());
        };
        if id.0.is_empty() || id.0.len() > 512 || id.0.chars().any(char::is_control) {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        self.last = Some(id.clone());
        match event.kind {
            EventKind::TurnStarted => {
                if self.open.is_some() {
                    return Err(AttemptError::UnknownStoreOutcome);
                }
                self.open = Some(id.clone());
            }
            EventKind::TurnCompleted => {
                if self.open.as_ref().is_some_and(|open| open != id) {
                    return Err(AttemptError::UnknownStoreOutcome);
                }
                self.open = None;
                if self.checkpoint.as_ref() == Some(id) {
                    self.checkpoint_closed = true;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> TurnContext {
        let close = self.open.clone().or_else(|| {
            (!self.checkpoint_closed)
                .then_some(self.checkpoint.clone())
                .flatten()
        });
        let source = self.open.or(self.checkpoint).or(self.last);
        TurnContext { source, close }
    }
}
pub(crate) fn close_turn(run: &Run, turn: TurnId, now: OffsetDateTime) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: Some(turn),
        kind: EventKind::TurnCompleted,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: now,
    }
}
