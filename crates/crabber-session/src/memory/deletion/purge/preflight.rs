use super::{BTreeSet, Failure, Proof, SessionId, State, unknown};
type Runs = BTreeSet<crabber_core::RunId>;
type Calls = BTreeSet<crabber_core::ToolCallId>;
pub(super) fn routes(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(Runs, Calls), Failure> {
    let mut runs = BTreeSet::new();
    for (id, run) in &state.runs {
        if &run.session_id == session {
            proof.measure(&(id, run))?;
            if id != &run.id {
                return Err(unknown("run routing"));
            }
            runs.insert(id.clone());
        }
    }
    let mut calls = BTreeSet::new();
    for (id, call) in &state.calls {
        if runs.contains(&call.run_id) {
            proof.measure(&(id, call))?;
            if id != &call.id {
                return Err(unknown("call routing"));
            }
            calls.insert(id.clone());
        }
    }
    for (scope, record) in state.call_order.scoped_iter() {
        if scope == session || calls.contains(record) {
            proof.measure(&(scope, record))?;
            if scope != session || !calls.contains(record) {
                return Err(unknown("call order scope"));
            }
        }
    }
    Ok((runs, calls))
}
pub(super) fn native(
    state: &State,
    session: &SessionId,
    runs: &Runs,
    calls: &Calls,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for (id, record) in &state.attempts {
        let snapshot = &record.snapshot;
        if &snapshot.session_id == session || runs.contains(&snapshot.run_id) {
            proof.measure(&(
                id,
                snapshot,
                &record.owner,
                &record.token,
                &record.run_token,
                record.lease_until,
            ))?;
            if &snapshot.session_id != session
                || !runs.contains(&snapshot.run_id)
                || id != &snapshot.target.id
            {
                return Err(unknown("attempt routing"));
            }
        }
    }
    for (id, phase) in &state.phases {
        let snapshot = &phase.snapshot;
        if &snapshot.session_id == session
            || runs.contains(&snapshot.run_id)
            || calls.contains(&snapshot.target.call_id)
        {
            proof.measure(&(id, &phase.snapshot, &phase.token, &phase.output))?;
            if &snapshot.session_id != session
                || !runs.contains(&snapshot.run_id)
                || !calls.contains(&snapshot.target.call_id)
                || id != &snapshot.target.id
            {
                return Err(unknown("phase routing"));
            }
        }
    }
    for (scope, message) in state.messages.scoped_iter() {
        if scope == session
            || &message.session_id == session
            || message.run_id.as_ref().is_some_and(|id| runs.contains(id))
        {
            proof.measure(&(scope, message))?;
            if scope != session
                || &message.session_id != session
                || message.run_id.as_ref().is_some_and(|id| !runs.contains(id))
            {
                return Err(unknown("message routing"));
            }
        }
    }
    for event in state.events.iter() {
        if &event.session_id == session || runs.contains(&event.run_id) {
            proof.measure(event)?;
            if &event.session_id != session || !runs.contains(&event.run_id) {
                return Err(unknown("event routing"));
            }
        }
    }
    Ok(())
}
pub(super) fn continuations(
    state: &State,
    session: &SessionId,
    runs: &Runs,
    calls: &Calls,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for domain in [
        crate::continuation_input::Domain::Approval,
        crate::continuation_input::Domain::Tool,
    ] {
        crate::memory::continuation_input::consistent(state, domain, session, None, proof)
            .map_err(crate::public_session::v2::proof::ReadError::public)?;
    }
    for (id, record) in &state.approvals {
        if &record.session_id == session || runs.contains(&record.run_id) {
            proof.measure(&(id, record))?;
            if &record.session_id != session
                || !runs.contains(&record.run_id)
                || id != &record.batch.continuation.id
            {
                return Err(unknown("approval routing"));
            }
        }
    }
    for (id, record) in &state.tool_continuations {
        if &record.session_id == session || runs.contains(&record.run_id) {
            proof.measure(&(id, record))?;
            if &record.session_id != session
                || !runs.contains(&record.run_id)
                || id != &record.batch.continuation.id
            {
                return Err(unknown("continuation routing"));
            }
        }
    }
    for (id, record) in &state.frontend_origins {
        if &record.request.session_id == session
            || calls.contains(id)
            || runs.contains(&record.request.run_id)
        {
            proof.measure(&(id, record))?;
            if &record.request.session_id != session
                || !calls.contains(id)
                || id != &record.request.call.id
            {
                return Err(unknown("frontend routing"));
            }
        }
    }
    proof.measure(&state.native_session_protocols.contains(session))?;
    super::super::super::native_parent::source_protocol::routes(state, session, proof)?;
    for run in runs {
        super::super::super::native_parent::source_protocol::history(state, session, run, proof)?;
    }
    for ((scope, id, sequence), record) in &state.native_run_issuances {
        if scope == session || runs.contains(id) || &record.state.session_id == session {
            proof.measure(&(scope, id, sequence, record))?;
            if scope != session
                || !runs.contains(id)
                || record.state.session_id != *session
                || record.state.run_id != *id
                || record.sequence != *sequence
            {
                return Err(unknown("issuance routing"));
            }
        }
    }
    for (cursor, record) in &state.native_parent_witnesses {
        if &record.source.session_id == session || runs.contains(&record.source.run_id) {
            proof.measure(&(cursor, record))?;
            if &record.source.session_id != session
                || !runs.contains(&record.source.run_id)
                || cursor != &record.source.event_cursor
            {
                return Err(unknown("witness routing"));
            }
        }
    }
    Ok(())
}
pub(super) fn payloads(
    state: &State,
    session: &SessionId,
    runs: &Runs,
    proof: &mut Proof,
) -> Result<(), Failure> {
    // Keyed private payloads are borrowed and charged before removal. Their
    // authenticated semantic content is the readiness verifier's responsibility.
    macro_rules! scoped {
        ($map:expr) => {
            for (key, value) in &$map {
                if &key.0 == session {
                    proof.measure(&(key, value))?;
                }
            }
        };
    }
    routed(state, session, proof)?;
    scoped!(state.extension_values);
    macro_rules! run_scoped {
        ($map:expr) => {
            for (key, value) in &$map {
                if runs.contains(key) {
                    proof.measure(&(key, value))?;
                }
            }
        };
    }
    run_scoped!(state.suspension_proofs);
    run_scoped!(state.abandonments);
    run_scoped!(state.admission_executions);
    run_scoped!(state.execution_bindings);
    for (id, epoch) in &state.epochs {
        if &epoch.session_id == session || runs.contains(&epoch.run_id) {
            proof.measure(&(id, epoch))?;
            if &epoch.session_id != session || !runs.contains(&epoch.run_id) || id != &epoch.id {
                return Err(unknown("epoch routing"));
            }
        }
    }
    for row in &state.inbox {
        if &row.session_id == session {
            proof.measure(&(
                &row.session_id,
                matches!(row.kind, crate::InboxKind::Steer),
                &row.message,
                &row.consumed_by_run,
            ))?;
            if row.message.session_id != *session
                || row
                    .consumed_by_run
                    .as_ref()
                    .is_some_and(|id| !runs.contains(id))
            {
                return Err(unknown("inbox routing"));
            }
        }
    }
    history(state, session, proof)?;
    macro_rules! session_value {
        ($map:expr) => {
            if let Some(value) = $map.get(session) {
                proof.measure(value)?;
            }
        };
    }
    session_value!(state.sessions);
    session_value!(state.management);
    session_value!(state.ownership_origins);
    session_value!(state.tool_barriers);
    session_value!(state.approval_barriers);
    session_value!(state.snapshot_revisions);
    session_value!(state.public_revisions);
    session_value!(state.public_contents);
    Ok(())
}

fn routed(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    macro_rules! check {
        ($map:expr, $value:ident => $route:expr) => {
            for (key, $value) in &$map {
                let route = $route;
                if &key.0 == session || route == session {
                    proof.measure(&(key, $value))?;
                    if &key.0 != session || route != session {
                        return Err(unknown("keyed payload routing"));
                    }
                }
            }
        };
    }
    check!(state.run_controls, value => &value.summary.receipt.target.session_id);
    check!(state.run_control_audits, value => &value.receipt.target.session_id);
    super::super::super::queue::protocol::consistent(state, session, proof)
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    check!(state.queue, value => &value.summary.session_id);
    check!(state.queue_audits, value => &value.receipt.session_id);
    check!(state.receipts, value => &value.session_id);
    check!(state.management_audits, value => &value.receipt.session_id);
    check!(state.fork_audits, value => &value.receipt.target.session_id);
    check!(state.operation_receipts, value => receipt_scope(value));
    Ok(())
}
fn receipt_scope(receipt: &crate::memory::OperationReceipt) -> &SessionId {
    use crate::memory::OperationReceipt as R;
    match receipt {
        R::Fork(value) => &value.target.session_id,
        R::RunControl(value) => &value.target.session_id,
        R::FrontendStaging(value) => &value.session_id,
        R::Queue(value) => &value.session_id,
        R::ToolContinuation(value) => &value.session_id,
        R::Management(value) => &value.session_id,
        R::Approval(value) => &value.session_id,
        R::PublicState(value) => &value.session_id,
        R::Attempt(value) => &value.attempt.session_id,
        R::Phase(value) => &value.phase.session_id,
    }
}

fn history(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    macro_rules! check {
        ($sequence:expr) => {
            for (scope, record) in $sequence.scoped_iter() {
                if scope == session || &record.session_id == session {
                    proof.measure(&(scope, record))?;
                    if scope != session || &record.session_id != session {
                        return Err(unknown("imported history routing"));
                    }
                }
            }
        };
    }
    check!(state.imported_calls);
    check!(state.imported_provenance);
    Ok(())
}

/// Registry ownership comes from validated scoped imported rows, never from a
/// guessed ID prefix. Unroutable reservations belong to no proven erased set.
pub(super) fn imported_ids(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<Calls, Failure> {
    let mut owned = Calls::new();
    for call in state.imported_calls.for_session(session) {
        let registered = state.imported_call_ids.contains(&call.id);
        proof.measure(&(&call.session_id, &call.id, registered))?;
        crate::fork::history::validate_id(&call.id.0).map_err(unknown)?;
        if &call.session_id != session
            || !registered
            || state.calls.contains_key(&call.id)
            || !owned.insert(call.id.clone())
        {
            return Err(unknown("imported reservation ownership"));
        }
    }
    for (scope, call) in state.imported_calls.scoped_iter() {
        if scope != session && owned.contains(&call.id) {
            proof.measure(&(scope, &call.session_id, &call.id))?;
            return Err(unknown("shared imported reservation"));
        }
    }
    Ok(owned)
}
