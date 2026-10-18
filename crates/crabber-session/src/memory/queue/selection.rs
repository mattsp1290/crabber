//! Private selected sources carried across one continuously locked transaction.
use super::{
    QueueMutation, QueueSummary, Record, State, StoreError, corrupt, head, preflight, protocol,
    target,
};
use crate::public_session::v2::proof::Proof;
use crate::queue::source::{Scope, Source};
use crabber_core::{ControlId, SessionId};
/// Owns the compact projection and borrows actual protected originals.
/// The caller keeps the same State lock held while this is constructed,
/// the inherited whole-State clone is made, and the mutation consumes this value.
/// Only selected carriers are bounded; that whole-State clone remains unbounded.
pub(in crate::memory) struct Selected<'a> {
    session: SessionId,
    operation: ControlId,
    pub(super) head: Option<QueueSummary>,
    pub(super) original: Option<Record>,
    pub(super) previous: Option<Source>,
    pub(super) quota: Option<(usize, usize)>,
    pub(super) prepared: Option<&'a crate::BoundKeyedAdmitRequest>,
}
pub(in crate::memory) fn select<'a>(
    state: &'a State,
    request: &QueueMutation,
    proof: &mut Proof,
) -> Result<Selected<'a>, StoreError> {
    preflight(state, &request.session_id, proof)?;
    let current_head = head(state, &request.session_id)?;
    let id = target(&request.action).or_else(|| current_head.as_ref().map(|h| &h.queue_id));
    let loaded = if let Some(id) = id {
        let scope = Scope {
            session: request.session_id.clone(),
            queue: id.clone(),
        };
        match protocol::record(state, &scope, proof) {
            Ok(record) => Some(record),
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
    let quota = quota(state, request, loaded.as_ref(), proof)?;
    let prepared = loaded.as_ref().and_then(|record| record.prepared);
    let original = loaded.as_ref().map(|record| record.record.clone());
    if matches!(request.action, crate::QueueAction::Admit { .. })
        && let Some(prepared) = prepared
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
        prepared,
        quota,
    })
}
pub(super) struct Parts<'a> {
    pub head: Option<QueueSummary>,
    pub original: Option<Record>,
    pub previous: Option<Source>,
    pub quota: Option<(usize, usize)>,
    pub prepared: Option<&'a crate::BoundKeyedAdmitRequest>,
}
impl<'a> Selected<'a> {
    pub(in crate::memory) fn record(&self) -> Option<&Record> {
        self.original.as_ref()
    }
    pub(in crate::memory) fn prepared(&self) -> Option<&'a crate::BoundKeyedAdmitRequest> {
        self.prepared
    }
    pub(super) fn into_parts(self, request: &QueueMutation) -> Result<Parts<'a>, StoreError> {
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
            prepared: self.prepared,
            quota: self.quota,
        })
    }
}

/// Count the former full wire shape from actual originals while their State is
/// still locked. Reborrow the selected body instead of loading it from `next`.
fn quota(
    state: &State,
    request: &QueueMutation,
    selected: Option<&protocol::LoadedRecord<'_>>,
    proof: &mut Proof,
) -> Result<Option<(usize, usize)>, StoreError> {
    if !matches!(
        request.action,
        crate::QueueAction::Enqueue { .. } | crate::QueueAction::Prepare { .. }
    ) {
        return Ok(None);
    }
    let mut count = 0usize;
    let mut bytes = crate::queue::encode(request)?.len();
    for ((session, id), record) in &state.queue {
        if session != &request.session_id
            || !matches!(
                record.summary.state,
                crate::QueueState::Queued | crate::QueueState::Claimed
            )
        {
            continue;
        }
        count += 1;
        let length = if record.prepared.is_none() {
            record.former_encoded_len(None)?
        } else if let Some(selected) = selected.filter(|s| std::ptr::eq(s.record, record)) {
            record.former_encoded_len(selected.prepared)?
        } else {
            let scope = Scope {
                session: session.clone(),
                queue: id.clone(),
            };
            let original = protocol::record(state, &scope, proof).map_err(protocol::domain)?;
            record.former_encoded_len(original.prepared)?
        };
        bytes = bytes.checked_add(length).ok_or_else(corrupt)?;
    }
    Ok(Some((count, bytes)))
}
