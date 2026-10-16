use super::{State, insert_event, insert_message};
use crate::StoreError;
use crate::abandonment::{AbandonCommit, AbandonEvidence, interrupted_tool, terminal_event};
use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};
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
    if state
        .attempts
        .values()
        .any(|record| record.snapshot.run_id == run.id)
        || state
            .phases
            .values()
            .any(|record| record.snapshot.run_id == run.id)
        || state.approval_barriers.contains_key(&run.session_id)
    {
        return Err(AbandonError::NativeReconciliationRequired);
    }
    // Revoke first in the transaction candidate. Publication of any
    // change waits until all settlements and evidence have succeeded.
    revoke_run(state, &mut run, now)?;
    #[cfg(test)]
    fail_boundary(state, 1)?;
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
        #[cfg(test)]
        fail_boundary(state, 2)?;
        insert_event(state, &run, event)?;
        #[cfg(test)]
        fail_boundary(state, 3)?;
        let entry = state.calls.get_mut(&call.id).expect("selected call exists");
        entry.status = ToolCallStatus::Interrupted;
        entry.result = Some(result);
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
        #[cfg(test)]
        fail_boundary(state, 4)?;
    }
    #[cfg(test)]
    if state.abandon_fail_after_tools {
        return Err(StoreError::Validation("injected abandonment failure".into()).into());
    }
    let previous = fence_state(&run)?;
    run.status = RunStatus::Interrupted;
    // Retain usage, checkpoint and prior error/history verbatim.
    persist_run(state, &previous, &run)?;
    #[cfg(test)]
    fail_boundary(state, 5)?;
    let evidence = AbandonEvidence {
        request,
        run: run.clone(),
        interrupted_tools,
    };
    insert_event(state, &run, terminal_event(&evidence, now)?)?;
    #[cfg(test)]
    fail_boundary(state, 6)?;
    let terminal_event = state
        .events
        .last()
        .expect("inserted terminal event")
        .clone();
    let outcome = AbandonOutcome {
        run,
        terminal_event,
        interrupted_tools: evidence.interrupted_tools,
    };
    state.abandonments.insert(
        outcome.run.id.clone(),
        AbandonCommit {
            request: evidence.request,
            outcome: outcome.clone(),
        },
    );
    #[cfg(test)]
    fail_boundary(state, 7)?;
    Ok(outcome)
}

fn revoke_run(state: &mut State, run: &mut Run, now: OffsetDateTime) -> Result<(), AbandonError> {
    let previous = fence_state(run)?;
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until = now;
    run.updated_at = now;
    persist_run(state, &previous, run)
}

fn fence_state(run: &Run) -> Result<crate::native_parent::RunState, AbandonError> {
    crate::native_parent::RunState::from_run(run)
        .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()).into())
}
fn persist_run(
    state: &mut State,
    previous: &crate::native_parent::RunState,
    run: &Run,
) -> Result<(), AbandonError> {
    super::native_parent::record_run_states(state, Some(previous), fence_state(run)?)
        .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()))?;
    state.runs.insert(run.id.clone(), run.clone());
    Ok(())
}

fn replay(
    state: &State,
    run: &Run,
    request: &AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let commit = state
        .abandonments
        .get(&run.id)
        .ok_or(AbandonError::AlreadyTerminal)?;
    if state.calls.values().any(|call| {
        call.run_id == run.id
            && matches!(
                call.status,
                ToolCallStatus::Pending | ToolCallStatus::Running
            )
    }) {
        return Err(StoreError::Validation("unfinished abandoned tools".into()).into());
    }
    commit.replay(run, request)
}

#[cfg(test)]
fn fail_boundary(state: &State, boundary: u8) -> Result<(), AbandonError> {
    if state.abandon_fault_boundary == boundary {
        return Err(StoreError::Validation(format!("injected boundary {boundary}")).into());
    }
    Ok(())
}
