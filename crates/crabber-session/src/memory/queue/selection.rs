//! Private selected sources carried across one continuously locked transaction.
use super::{
    QueueMutation, QueueSummary, Record, State, StoreError, corrupt, head, preflight, protocol,
    target,
};
use crate::public_session::v2::proof::Proof;
use crate::queue::source::{Scope, Source};
use crabber_core::{ControlId, SessionId};
/// Owns the bounded original selected body, rather than trusting a copied digest.
/// `transact_or_replay` keeps the same State lock held while this is constructed,
/// the inherited whole-State clone is made, and the mutation consumes this value.
/// Only selected carriers are bounded; that whole-State clone remains unbounded.
pub(in crate::memory) struct Selected {
    session: SessionId,
    operation: ControlId,
    pub(super) head: Option<QueueSummary>,
    pub(super) original: Option<Record>,
    pub(super) previous: Option<Source>,
}
pub(in crate::memory) fn select(
    state: &State,
    request: &QueueMutation,
    proof: &mut Proof,
) -> Result<Selected, StoreError> {
    preflight(state, &request.session_id, proof)?;
    let current_head = head(state, &request.session_id)?;
    let id = target(&request.action).or_else(|| current_head.as_ref().map(|h| &h.queue_id));
    let original = if let Some(id) = id {
        let scope = Scope {
            session: request.session_id.clone(),
            queue: id.clone(),
        };
        match protocol::record(state, &scope, proof) {
            Ok(record) => Some(record.clone()),
            Err(crate::public_session::v2::proof::ReadError::Domain(StoreError::NotFound))
                if matches!(request.action, crate::QueueAction::Enqueue { .. }) =>
            {
                None
            }
            Err(error) => return Err(protocol::domain(error)),
        }
    } else {
        None
    };
    if matches!(request.action, crate::QueueAction::Admit { .. })
        && let Some(prepared) = original.as_ref().and_then(|r| r.prepared.as_ref())
    {
        crate::memory::admission::selected(
            state,
            &prepared.keyed.request,
            Some(&prepared.keyed),
            proof,
        )?;
    }
    let previous = if let Some(record) = &original {
        let scope = Scope {
            session: request.session_id.clone(),
            queue: record.summary.queue_id.clone(),
        };
        state
            .queue_publications
            .projected(&scope, proof, |id, proof| {
                let audit = state
                    .queue_audits
                    .get(&(request.session_id.clone(), id.clone()))
                    .ok_or_else(corrupt)?;
                proof
                    .measure(&audit.receipt)
                    .map_err(crate::public_session::v2::proof::ReadError::Budget)?;
                Ok(&audit.receipt)
            })
            .map_err(protocol::domain)?
            .cloned()
    } else {
        None
    };
    Ok(Selected {
        session: request.session_id.clone(),
        operation: request.id.clone(),
        head: current_head,
        original,
        previous,
    })
}
pub(super) struct Parts {
    pub head: Option<QueueSummary>,
    pub original: Option<Record>,
    pub previous: Option<Source>,
}
impl Selected {
    pub(in crate::memory) fn record(&self) -> Option<&Record> {
        self.original.as_ref()
    }
    pub(super) fn into_parts(self, request: &QueueMutation) -> Result<Parts, StoreError> {
        if self.session != request.session_id || self.operation != request.id {
            return Err(corrupt());
        }
        if let Some(record) = &self.original {
            let id = target(&request.action).or_else(|| self.head.as_ref().map(|h| &h.queue_id));
            if id != Some(&record.summary.queue_id) {
                return Err(corrupt());
            }
        }
        Ok(Parts {
            head: self.head,
            original: self.original,
            previous: self.previous,
        })
    }
}
