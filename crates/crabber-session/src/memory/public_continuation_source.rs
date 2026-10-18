//! One State guard selects current ownership, pending calls, parents and UI state.
use super::MemoryStore;
use crate::public_session::v2::proof::Proof;
use crate::{AttemptAuthority, PublicStateError as Error};
use crabber_core::continuation_source::{AttemptIdentity, ResumeSnapshot};
use crabber_core::{ContentBlock, Role, RunFence, ToolCallStatus};

impl MemoryStore {
    pub(super) fn read_continuation_source(
        &self,
        fence: &RunFence,
        authority: &AttemptAuthority,
    ) -> Result<ResumeSnapshot, Error> {
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        let mut proof = Proof::bounded(32 << 20);
        proof.measure(fence).map_err(|_| Error::InvalidRequest)?;
        let run = state.runs.get(&fence.run_id).ok_or(Error::NotFound)?;
        super::deletion::ensure_live(&state, &run.session_id).map_err(|_| Error::NotFound)?;
        super::credential_access::check(&state, run, &mut proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        super::native_parent::current_issuance_with_proof(&state, run, &mut proof)
            .map_err(|_| Error::StaleOwner)?;
        let attempt = state
            .attempts
            .values()
            .filter(|entry| entry.snapshot.run_id == run.id)
            .max_by_key(|entry| entry.snapshot.target.generation)
            .ok_or(Error::StaleOwner)?;
        proof
            .measure(&attempt.snapshot)
            .map_err(|_| Error::InvalidRequest)?;
        crate::continuation_source::verify_owner(run, fence, attempt, authority, self.clock.now())?;
        let mut calls = Vec::new();
        for call in state.calls.values().filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                )
        }) {
            if calls.len() >= 64 {
                return Err(Error::InvalidRequest);
            }
            proof.measure(call).map_err(|_| Error::InvalidRequest)?;
            let mut selected = None;
            for message in state
                .messages
                .for_session(&run.session_id)
                .iter()
                .filter(|message| message.run_id.as_ref() == Some(&run.id))
            {
                for part in &message.parts {
                    if matches!(&part.content, ContentBlock::ToolCall { call_id, .. } if *call_id == call.id)
                    {
                        if selected.is_some()
                            || message.role != Role::Assistant
                            || message.session_id != run.session_id
                        {
                            return Err(Error::UnknownStoreOutcome);
                        }
                        selected = Some((message, part));
                    }
                }
            }
            let (message, part) = selected.ok_or(Error::UnknownStoreOutcome)?;
            proof.measure(part).map_err(|_| Error::InvalidRequest)?;
            calls.push(crate::continuation_source::frozen_call(
                &run.session_id,
                &run.id,
                call,
                &message.id,
                part,
                &mut proof,
            )?);
        }
        let empty = crabber_core::PublicStateContents::default();
        let contents = state.public_contents.get(&run.session_id).unwrap_or(&empty);
        proof
            .measure(&(contents, &run.session_id, &run.id, &attempt.snapshot.target))
            .map_err(|_| Error::InvalidRequest)?;
        let result = ResumeSnapshot {
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            attempt: AttemptIdentity {
                id: attempt.snapshot.target.id.clone(),
                generation: attempt.snapshot.target.generation,
            },
            revision: state
                .public_revisions
                .get(&run.session_id)
                .copied()
                .unwrap_or(crabber_core::SessionRevision::INITIAL),
            contents: contents.clone(),
            calls,
        };
        proof.measure(&result).map_err(|_| Error::InvalidRequest)?;
        Ok(result)
    }
}
#[cfg(test)]
#[tokio::test]
async fn continuation_source_exact_native_parent_and_foreign_authority() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    crate::continuation_source::tests::run(&store).await;
    crate::continuation_source::tests::waiting_atomic(&store, &clock, &store.approval_fault).await;
    crate::continuation_source::tests::tool_waiting_atomic(&store, &store.tool_continuation_fault)
        .await;
}
