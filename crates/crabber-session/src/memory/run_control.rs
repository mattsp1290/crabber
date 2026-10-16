#[path = "run_control/reads.rs"]
mod reads;
use reads::{receipt, records, revision, summaries};
#[path = "run_control/suspension.rs"]
pub(super) mod suspension;
#[path = "run_control/waiting.rs"]
mod waiting;
use super::{MemoryExecution, MemoryStore, OperationReceipt, State};
use crate::{
    RunControlError as Error,
    run_control::{
        MAX_CONTROLS, MAX_RUN_CONTROL_BYTES, Record, RunControlAction, RunControlBoundary,
        RunControlKind, RunControlOutcome, RunControlPage, RunControlReceipt, RunControlRequest,
        RunControlStatus, RunControlTarget, accepted, event, kind, reserve_ids, terminal_status,
        unknown, valid_scope, verify_target,
    },
};
use crabber_core::{ControlId, Run, SessionId, TurnId};

fn publish(
    state: &mut State,
    run: &Run,
    record: &mut Record,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    super::bump_public_revision(state, &run.session_id).map_err(unknown)?;
    record.summary.revision = revision(state, &run.session_id);
    record.summary.event_cursor = state.events.next_cursor().map_err(unknown)?;
    state
        .events
        .append(event(run, &record.summary, now)?)
        .map_err(unknown)?;
    state.run_control_audits.insert(
        (
            run.session_id.clone(),
            record.request.id.clone(),
            record.summary.event_cursor.0,
        ),
        record.summary.clone(),
    );
    state.run_controls.insert(
        (run.session_id.clone(), record.request.id.clone()),
        record.clone(),
    );
    Ok(())
}
impl MemoryStore {
    pub(super) fn control_binding(
        &self,
        session: &SessionId,
        run_id: &crabber_core::RunId,
    ) -> Result<crate::RunControlBinding, Error> {
        valid_scope(session, &ControlId("read".into()))?;
        crate::attempt::validate_identifier(&run_id.0).map_err(|_| Error::InvalidRequest)?;
        let state = self.state.lock().unwrap();
        let run = state
            .runs
            .get(run_id)
            .filter(|record| record.session_id == *session)
            .ok_or(Error::NotFound)?;
        if run.id != *run_id || run.config_hash.len() > 512 || run.plan_fingerprint.len() > 512 {
            return Err(Error::UnknownStoreOutcome);
        }
        let target = RunControlTarget::of(run);
        crate::run_control::validate_target(&target).map_err(unknown)?;
        Ok(crate::RunControlBinding {
            revision: revision(&state, session),
            target,
        })
    }

    pub(super) fn control_lookup(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<RunControlReceipt>, Error> {
        valid_scope(session, id)?;
        receipt(&self.state.lock().unwrap(), session, id)
    }
    pub(super) fn control_page(&self, target: &RunControlTarget) -> Result<RunControlPage, Error> {
        crate::run_control::validate_target(target)?;
        let state = self.state.lock().unwrap();
        let run = state.runs.get(&target.run_id).ok_or(Error::NotFound)?;
        verify_target(target, run)?;
        let controls = summaries(&state, target)?;
        for summary in &controls {
            crate::run_control::verify_terminal(summary, run.status)?;
        }
        Ok(RunControlPage {
            revision: revision(&state, &target.session_id),
            controls,
        })
    }
    pub(super) fn control_accept(
        &self,
        request: RunControlRequest,
    ) -> Result<RunControlOutcome, Error> {
        let digest = request.fingerprint()?;
        let result = self.transact(|state| {
            if let Some(saved) = receipt(state, &request.target.session_id, &request.id)? {
                if saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(RunControlOutcome::Replayed(saved));
            }
            let run = state
                .runs
                .get(&request.target.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            verify_target(&request.target, &run)?;
            if revision(state, &run.session_id) != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            if run.status.is_terminal() {
                return Err(Error::Terminal);
            }
            let previous = records(state, &request.target)?;
            let (message_ids, part_ids) = reserve_ids(&request, &previous)?;
            if state.messages.iter().any(|message| {
                message_ids.contains(message.id.0.as_str())
                    || message
                        .parts
                        .iter()
                        .any(|part| part_ids.contains(part.id.0.as_str()))
            }) {
                return Err(Error::SemanticConflict);
            }
            if previous.len() >= MAX_CONTROLS {
                return Err(Error::Capacity);
            }
            let bytes = previous.iter().try_fold(
                crate::run_control::reserved_bytes(&request)?,
                |n, record| {
                    Ok::<_, Error>(n + crate::run_control::reserved_bytes(&record.request)?)
                },
            )?;
            if bytes > MAX_RUN_CONTROL_BYTES {
                return Err(Error::Capacity);
            }
            let saved = RunControlReceipt {
                operation_id: request.id.clone(),
                target: request.target.clone(),
                semantic_digest: digest.clone(),
                revision: request.expected_revision.next().map_err(unknown)?,
                event_cursor: state.events.next_cursor().map_err(unknown)?,
                sequence: previous.len() as u64 + 1,
                kind: kind(&request),
            };
            let mut record = Record {
                request,
                summary: accepted(&saved),
            };
            publish(state, &run, &mut record, self.clock.now())?;
            state.operation_receipts.insert(
                (run.session_id.clone(), saved.operation_id.clone()),
                OperationReceipt::RunControl(saved.clone()),
            );
            cancel_waiting(state, &run, &record.request, self.clock.now())?;
            #[cfg(test)]
            crate::binding_fault::trip(&self.control_fault, 1).map_err(unknown)?;
            Ok(RunControlOutcome::Applied(saved))
        });
        #[cfg(test)]
        if matches!(&result, Ok(RunControlOutcome::Applied(_))) {
            crate::binding_fault::trip(&self.control_fault, 2).map_err(unknown)?;
        }
        result
    }
}
impl MemoryExecution {
    pub(super) fn control_boundary(
        &self,
        turn: Option<&TurnId>,
    ) -> Result<RunControlBoundary, Error> {
        crate::run_control::validate_turn(turn)?;
        let result = self.store.transact(|state| {
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            crate::attempt::verify_run(&run, &self.fence, self.store.clock.now())
                .map_err(|_| Error::StaleOwner)?;
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(Error::StaleOwner);
            }
            let run = &run;
            let mut pending = records(state, &RunControlTarget::of(run))?;
            let cancel = pending.iter().any(|r| {
                r.summary.receipt.kind == RunControlKind::Cancel
                    && matches!(
                        r.summary.status,
                        RunControlStatus::Accepted | RunControlStatus::Observed
                    )
            });
            let mut result = RunControlBoundary {
                cancel_observed: cancel,
                incorporated: 0,
            };
            for record in &mut pending {
                if record.summary.status != RunControlStatus::Accepted {
                    continue;
                }
                match &record.request.action {
                    RunControlAction::Cancel { .. } => {
                        record.summary.status = RunControlStatus::Observed;
                    }
                    RunControlAction::Steer { input } if !cancel && turn.is_some() => {
                        let messages = input
                            .materialize_messages(&run.session_id, self.store.clock.now())
                            .map_err(unknown)?;
                        for mut message in messages {
                            message.run_id = Some(run.id.clone());
                            super::insert_message(state, run, message).map_err(unknown)?;
                        }
                        record.summary.status = RunControlStatus::Incorporated {
                            turn_id: turn.unwrap().clone(),
                        };
                        result.incorporated += 1;
                    }
                    RunControlAction::Steer { .. } => continue,
                }
                publish(state, run, record, self.store.clock.now())?;
            }
            #[cfg(test)]
            if result.cancel_observed || result.incorporated > 0 {
                crate::binding_fault::trip(&self.store.control_fault, 1).map_err(unknown)?;
            }
            Ok(result)
        });
        #[cfg(test)]
        if result
            .as_ref()
            .is_ok_and(|result| result.cancel_observed || result.incorporated > 0)
        {
            crate::binding_fault::trip(&self.store.control_fault, 2).map_err(unknown)?;
        }
        result
    }
}
fn control_store_error(_: Error) -> crate::StoreError {
    crate::StoreError::Validation("run control integrity failure".into())
}
/// Called in the same transaction that commits native terminal truth.
pub(super) fn finalize(
    state: &mut State,
    run: &Run,
    now: time::OffsetDateTime,
) -> Result<(), crate::StoreError> {
    if !run.status.is_terminal() {
        return Ok(());
    }
    for mut record in records(state, &RunControlTarget::of(run)).map_err(control_store_error)? {
        if let Some(status) = terminal_status(&record, run.status) {
            record.summary.status = status;
            publish(state, run, &mut record, now).map_err(control_store_error)?;
        }
    }
    Ok(())
}

pub(super) fn pending(state: &State, run: &Run) -> Result<bool, Error> {
    Ok(records(state, &RunControlTarget::of(run))?.iter().any(|r| {
        matches!(
            r.summary.status,
            RunControlStatus::Accepted | RunControlStatus::Observed
        )
    }))
}

fn cancel_waiting(
    state: &mut State,
    run: &Run,
    request: &RunControlRequest,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let RunControlAction::Cancel { resolver } = &request.action else {
        return Ok(());
    };
    if run.status != crabber_core::RunStatus::Paused {
        return Ok(());
    }
    let accepted =
        receipt(state, &run.session_id, &request.id)?.ok_or(Error::UnknownStoreOutcome)?;
    if super::tool_continuation::cancel_waiting(state, run, request, accepted, now).map_err(
        |error| match error {
            crate::ToolContinuationError::UnknownStoreOutcome => Error::UnknownStoreOutcome,
            _ => Error::Busy,
        },
    )? {
        return Ok(());
    }
    let Some(id) = state.approval_barriers.get(&run.session_id) else {
        return suspension::cancel(state, run, request, now);
    };
    let record = state.approvals.get(id).ok_or(Error::UnknownStoreOutcome)?;
    let termination = crate::ApprovalTermination {
        id: crate::approval::derived_id("run-control-cancel", &request.id),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        continuation: record.batch.continuation.clone(),
        expected_revision: revision(state, &run.session_id),
        resolver: resolver.clone(),
        kind: crate::ApprovalTerminationKind::Cancel,
    };
    super::approval::apply_termination(state, &termination, now).map_err(|error| match error {
        crate::ApprovalError::UnknownStoreOutcome => Error::UnknownStoreOutcome,
        _ => Error::Busy,
    })?;
    Ok(())
}
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn durable_run_control_contract() {
        crate::run_control_contract::run_contract(|clock| super::MemoryStore::with_clock(clock))
            .await;
        let store = super::MemoryStore::with_clock(std::sync::Arc::new(
            crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH),
        ));
        crate::run_control_contract::run_fault_contract(&store, |point| {
            store
                .control_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
