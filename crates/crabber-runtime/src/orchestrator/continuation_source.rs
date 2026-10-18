//! Presentation follows actual acknowledged acquisition/Begin, never null events.
use super::{Orchestrator, RuntimeError};
use crabber_core::continuation_source::{ContinuationIdentity, ResumeMode, ResumeSource};
use crabber_core::{EventKind, EventRecord, RunFence, TurnId};
use crabber_session::StoreError;

impl Orchestrator {
    pub(super) async fn committed_resume_event(
        &self,
        fence: &RunFence,
        session: &crabber_core::SessionId,
        unstarted: bool,
        turn: Option<TurnId>,
        continuation: Option<ContinuationIdentity>,
    ) -> Result<EventRecord, RuntimeError> {
        let owner = self
            .owned_attempt
            .as_ref()
            .ok_or(StoreError::HostUnsupported)?;
        let snapshot = match self
            .store
            .public_continuation_source(fence, &owner.authority)
            .await
        {
            Ok(snapshot) => snapshot,
            // A custom Store can retain legacy execution support without this
            // optional presentation read. Null never establishes an AG-UI start.
            Err(crabber_session::PublicStateError::Unsupported) => {
                return Ok(self.event(session, &fence.run_id, EventKind::RunResumed));
            }
            Err(_) => {
                return Err(StoreError::Validation(
                    "native continuation source unavailable".into(),
                )
                .into());
            }
        };
        if snapshot.session_id != *session
            || snapshot.run_id != fence.run_id
            || snapshot.attempt.id != owner.target.id
            || snapshot.attempt.generation != owner.target.generation
        {
            return Err(
                StoreError::Validation("native continuation source mismatch".into()).into(),
            );
        }
        let source = ResumeSource {
            version: 1,
            session_id: snapshot.session_id,
            run_id: snapshot.run_id,
            attempt: snapshot.attempt,
            current_turn: turn,
            continuation,
            mode: if unstarted {
                ResumeMode::NativeUnstarted
            } else {
                ResumeMode::Continued
            },
            revision: snapshot.revision,
            contents: snapshot.contents,
            calls: snapshot.calls,
        };
        source
            .validate()
            .map_err(|_| StoreError::Validation("invalid public continuation source".into()))?;
        let mut event = self.event(&source.session_id, &source.run_id, EventKind::RunResumed);
        event.turn_id.clone_from(&source.current_turn);
        event.payload = serde_json::to_value(source)
            .map_err(|_| StoreError::Validation("invalid public continuation source".into()))?;
        Ok(event)
    }
}
