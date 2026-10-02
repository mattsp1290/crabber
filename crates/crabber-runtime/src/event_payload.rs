//! Synchronous presentation cleanup also runs when lease supervision drops a model future.
use crate::{Orchestrator, TurnSnapshot};
use crabber_core::{
    EventKind, MessageId, ToolCallId,
    event_payload::{CallIdentity, MessageEnded, StreamOutcome},
};

/// Keep public settlement fields typed while preserving existing flat diagnostics.
#[derive(serde::Serialize)]
pub(crate) struct Settlement<'a> {
    #[serde(flatten)]
    pub result: crabber_core::event_payload::CallSettled<'a>,
    #[serde(flatten)]
    pub diagnostics: serde_json::Value,
}

pub(crate) struct Presentation<'a> {
    runtime: &'a Orchestrator,
    snapshot: &'a TurnSnapshot,
    message_id: MessageId,
    open_calls: Vec<ToolCallId>,
    ended: bool,
}
impl<'a> Presentation<'a> {
    pub(crate) fn new(
        runtime: &'a Orchestrator,
        snapshot: &'a TurnSnapshot,
        message_id: MessageId,
    ) -> Self {
        Self {
            runtime,
            snapshot,
            message_id,
            open_calls: Vec::new(),
            ended: false,
        }
    }
    pub(crate) fn start_call(&mut self, call_id: ToolCallId) {
        self.open_calls.push(call_id);
    }
    pub(crate) fn complete_call(&mut self, call_id: &ToolCallId) {
        if let Some(index) = self.open_calls.iter().position(|id| id == call_id) {
            self.open_calls.remove(index);
            self.runtime.live_payload(
                self.snapshot,
                EventKind::ToolCallArgsCompleted,
                CallIdentity {
                    message_id: self.message_id.clone(),
                    call_id: call_id.clone(),
                },
            );
        }
    }
    pub(crate) fn finish(&mut self, outcome: StreamOutcome) {
        if self.ended {
            return;
        }
        for call_id in std::mem::take(&mut self.open_calls) {
            self.runtime.live_payload(
                self.snapshot,
                EventKind::ToolCallArgsCompleted,
                CallIdentity {
                    message_id: self.message_id.clone(),
                    call_id,
                },
            );
        }
        self.ended = true;
        self.runtime.live_payload(
            self.snapshot,
            EventKind::MessageStreamEnded,
            MessageEnded {
                message_id: self.message_id.clone(),
                outcome,
            },
        );
    }
}
impl Drop for Presentation<'_> {
    fn drop(&mut self) {
        self.finish(StreamOutcome::Failed);
    }
}
