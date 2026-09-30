use super::{State, insert_event, insert_message};
#[cfg(test)]
use crate::StoreError;
use crate::abandonment::{AbandonEvidence, interrupted_tool, terminal_event};
use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest, EventKind};
use crabber_core::{Run, RunStatus, ToolCallStatus};
use time::OffsetDateTime;

pub(super) fn abandon_transaction(
    state: &mut State,
    request: AbandonRequest,
    now: OffsetDateTime,
) -> Result<AbandonOutcome, AbandonError> {
    let mut run = state
        .runs
        .get(&request.expected.run_id)
        .cloned()
        .ok_or(AbandonError::NotFound)?;
    if run.status.is_terminal() {
        return replay(state, &run, &request);
    }
    if run.owner != request.expected_owner || run.claim_token != request.expected.claim_token {
        return Err(AbandonError::StaleOwner);
    }
    if request.authority == AbandonAuthority::ExpiredLease && run.lease_until > now {
        return Err(AbandonError::LiveLease);
    }
    // Revoke first in the transaction candidate. Publication of any
    // change waits until all settlements and evidence have succeeded.
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until = now;
    run.updated_at = now;
    state.runs.insert(run.id.clone(), run.clone());
    let calls: Vec<_> = state
        .calls
        .values()
        .filter(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running
                )
        })
        .cloned()
        .collect();
    let interrupted_tools = calls.iter().map(|call| call.id.clone()).collect();
    for call in calls {
        let (result, message, event) = interrupted_tool(&run, &call, now);
        insert_message(state, &run, message)?;
        insert_event(state, &run, event)?;
        let entry = state.calls.get_mut(&call.id).expect("selected call exists");
        entry.status = ToolCallStatus::Interrupted;
        entry.result = Some(result);
    }
    #[cfg(test)]
    if state.abandon_fail_after_tools {
        return Err(StoreError::Validation("injected abandonment failure".into()).into());
    }
    run.status = RunStatus::Interrupted;
    // Retain usage, checkpoint and prior error/history verbatim.
    state.runs.insert(run.id.clone(), run.clone());
    let evidence = AbandonEvidence {
        request,
        run: run.clone(),
        interrupted_tools,
    };
    insert_event(state, &run, terminal_event(&evidence, now)?)?;
    let terminal_event = state
        .events
        .last()
        .expect("inserted terminal event")
        .clone();
    Ok(AbandonOutcome {
        run,
        terminal_event,
        interrupted_tools: evidence.interrupted_tools,
    })
}

fn replay(
    state: &State,
    run: &Run,
    request: &AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let (event, evidence) = state
        .events
        .iter()
        .find_map(|event| {
            if event.run_id != run.id || event.kind != EventKind::RunSettled {
                return None;
            }
            let evidence: AbandonEvidence =
                serde_json::from_value(event.payload.get("abandonment_v1")?.clone()).ok()?;
            // Caller-authored history cannot know the newly rotated terminal token.
            // Only evidence for the exact current terminal snapshot is authoritative.
            (evidence.run == *run).then_some((event, evidence))
        })
        .ok_or(AbandonError::AlreadyTerminal)?;
    if &evidence.request != request {
        return Err(AbandonError::StaleOwner);
    }
    Ok(AbandonOutcome {
        run: evidence.run,
        terminal_event: event.clone(),
        interrupted_tools: evidence.interrupted_tools,
    })
}
