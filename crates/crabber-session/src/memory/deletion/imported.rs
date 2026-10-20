//! Copied presentation identities belong to this session only when the original
//! protected fork receipt authenticates their exact history, including IDs.
use super::super::{OperationReceipt, State};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::SessionId;

pub(super) fn verify(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    // Group keys are independently owned routing evidence. A payload claiming
    // this scope inside another group must not disappear from membership.
    for (scope, call) in state.imported_calls.scoped_iter() {
        if scope == session || &call.session_id == session {
            proof.measure(&(scope, call))?;
            if scope != session || &call.session_id != session {
                return Err(unknown("imported call routing"));
            }
        }
    }
    for (scope, origin) in state.imported_provenance.scoped_iter() {
        if scope == session || &origin.session_id == session {
            proof.measure(&(scope, origin))?;
            if scope != session || &origin.session_id != session {
                return Err(unknown("imported provenance routing"));
            }
        }
    }
    let calls = state.imported_calls.for_session(session);
    let provenance = state.imported_provenance.for_session(session);
    let all = state.messages.for_session(session);
    let mut sources = state
        .fork_audits
        .iter()
        .filter(|(key, _)| &key.0 == session);
    let Some((key, audit)) = sources.next() else {
        if !calls.is_empty() || !provenance.is_empty() || all.iter().any(|m| m.run_id.is_none()) {
            return Err(unknown("unanchored imported history"));
        }
        return Ok(());
    };
    proof.measure(&(key, audit))?;
    if sources.next().is_some() {
        return Err(unknown("duplicate fork origin"));
    }
    let Some(OperationReceipt::Fork(receipt)) = state.operation_receipts.get(key) else {
        return Err(unknown("fork receipt"));
    };
    proof.measure(receipt)?;
    audit.validate().map_err(unknown)?;
    let current = state
        .sessions
        .get(session)
        .ok_or_else(|| unknown("fork session"))?;
    proof.measure(current)?;
    if receipt != &audit.receipt
        || &receipt.target.session_id != session
        || key.1 != receipt.operation_id
        || current.id != *session
        || current.workspace_id != receipt.target.workspace_id
        || current.directory != receipt.target.directory
        || current.created_at != receipt.target.created_at
    {
        return Err(unknown("fork origin scope"));
    }
    let messages = all
        .get(..receipt.messages)
        .ok_or_else(|| unknown("fork prefix"))?;
    if calls.len() != receipt.imported_calls || provenance.len() != messages.len() {
        return Err(unknown("fork carrier count"));
    }
    for message in messages {
        proof.measure(message)?;
        if message.session_id != *session || message.run_id.is_some() {
            return Err(unknown("fork message scope"));
        }
    }
    if all[receipt.messages..].iter().any(|m| m.run_id.is_none()) {
        return Err(unknown("unanchored fork suffix"));
    }
    for call in calls {
        proof.measure(call)?;
        if call.session_id != *session {
            return Err(unknown("fork call scope"));
        }
    }
    for origin in provenance {
        proof.measure(origin)?;
        if origin.session_id != *session {
            return Err(unknown("fork provenance scope"));
        }
    }
    if crate::fork::history::imported_digest(messages, calls, provenance).map_err(unknown)?
        != receipt.history_digest
    {
        return Err(unknown("fork history digest"));
    }
    Ok(())
}
