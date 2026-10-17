//! Physical scope removal, separate from readiness. No public operation calls
//! this until the coordinator has established complete resource coverage.
use super::super::State;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::SessionId;
use std::collections::BTreeSet;

#[path = "purge/preflight.rs"]
mod preflight;
pub(super) fn purge(
    state: &mut State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if !proof.active() {
        return Err(unknown("deletion requires bounded proof"));
    }
    super::owned::verify(state, session, proof)?;
    let (runs, calls) = preflight::routes(state, session, proof)?;
    preflight::native(state, session, &runs, &calls, proof)?;
    preflight::continuations(state, session, &runs, &calls, proof)?;
    preflight::payloads(state, session, &runs, proof)?;
    super::imported::verify(state, session, proof)?;
    let imported = preflight::imported_ids(state, session, proof)?;
    // Ledger is last: it validates all routing before mutation. Every remaining
    // operation is infallible under the caller's unchanged session gate.
    state.session_owners.purge_session(session, proof)?;
    erase(state, session, &runs, &calls, &imported);
    Ok(())
}
fn erase(
    state: &mut State,
    session: &SessionId,
    runs: &BTreeSet<crabber_core::RunId>,
    calls: &BTreeSet<crabber_core::ToolCallId>,
    imported: &BTreeSet<crabber_core::ToolCallId>,
) {
    state.events.remove_session(session);
    state.messages.remove_session(session);
    state.call_order.remove_session(session);
    state.imported_call_ids.retain(|id| !imported.contains(id));
    state.imported_calls.remove_session(session);
    state.imported_provenance.remove_session(session);
    state.runs.retain(|id, _| !runs.contains(id));
    state.native_session_protocols.remove(session);
    state
        .native_run_protocols
        .retain(|(scope, _)| scope != session);
    state
        .native_run_sources
        .retain(|(scope, _, _), _| scope != session);
    state
        .native_run_source_heads
        .retain(|(scope, _), _| scope != session);
    state.run_order.retain(|id| !runs.contains(id));
    state.calls.retain(|id, _| !calls.contains(id));
    state
        .attempts
        .retain(|_, record| &record.snapshot.session_id != session);
    state
        .phases
        .retain(|_, record| &record.snapshot.session_id != session);
    state
        .approvals
        .retain(|_, record| &record.session_id != session);
    state
        .tool_continuations
        .retain(|_, record| &record.session_id != session);
    state.frontend_origins.retain(|id, _| !calls.contains(id));
    state
        .native_run_issuances
        .retain(|(scope, _, _), _| scope != session);
    state
        .native_parent_witnesses
        .retain(|_, record| &record.source.session_id != session);
    macro_rules! remove_scoped {
        ($map:expr) => {
            $map.retain(|key, _| &key.0 != session);
        };
    }
    remove_scoped!(state.run_controls);
    remove_scoped!(state.run_control_audits);
    remove_scoped!(state.queue);
    remove_scoped!(state.queue_audits);
    remove_scoped!(state.receipts);
    remove_scoped!(state.management_audits);
    remove_scoped!(state.fork_audits);
    remove_scoped!(state.operation_receipts);
    remove_scoped!(state.extension_values);
    macro_rules! remove_runs {
        ($map:expr) => {
            $map.retain(|id, _| !runs.contains(id));
        };
    }
    remove_runs!(state.preparation_admissions);
    state
        .preparation_admission_joins
        .retain(|(scope, _), _| scope != session);
    state
        .admission_execution_protocols
        .retain(|id| !runs.contains(id));
    state
        .admission_execution_sources
        .retain(|(id, _), _| !runs.contains(id));
    remove_runs!(state.suspension_proofs);
    remove_runs!(state.abandonments);
    remove_runs!(state.admission_executions);
    remove_runs!(state.execution_bindings);
    state.epochs.retain(|_, epoch| &epoch.session_id != session);
    state.inbox.retain(|row| &row.session_id != session);
    state.tool_barriers.remove(session);
    state.approval_barriers.remove(session);
    state.ownership_origins.remove(session);
    state.ownership_protocol_sessions.remove(session);
    state.continuation_inputs.purge(session);
    state.queue_publications.purge(session);
    state.sessions.remove(session);
    state.management.remove(session);
    state.snapshot_revisions.remove(session);
    state.public_revisions.remove(session);
    state.public_contents.remove(session);
    // Unrelated/orphan call reservations and allocation/authentication high-water marks survive.
}
