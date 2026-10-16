//! Exact original Reserve/Begin metadata; no authority or task observation.
use super::Error;
use crate::session_owner::{SessionOwnerOperation, SessionOwnerReceipt, SessionOwnerState};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};
/// Bounded immutable origin declaration; constructors do not authenticate Store
/// state or grant dispatch. Adapters independently load both original audits.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedScopeSource {
    pub session: SessionId,
    pub target: crate::session_owner::SessionOwnerTarget,
    pub reserve: SessionOwnerOperationId,
    pub reserve_digest: InputFingerprint,
    pub begin: SessionOwnerOperationId,
    pub begin_digest: InputFingerprint,
    pub binding: InputFingerprint,
}
impl OwnedScopeSource {
    /// Retain the original metadata returned by fresh Reserve and Begin. This
    /// creates a source declaration only; metadata can never grant dispatch.
    /// # Errors
    /// Refuses another session, owner, binding or a non-active Begin.
    pub fn from_receipts(
        reserve: &SessionOwnerReceipt,
        begin: &SessionOwnerReceipt,
    ) -> Result<Self, Error> {
        reserve.owner.binding.validate()?;
        super::super::identifier(&reserve.operation_id.0, 128, false)?;
        super::super::identifier(&begin.operation_id.0, 128, false)?;
        let command = crate::session_owner::MutateSessionOwner {
            id: begin.operation_id.clone(),
            session_id: begin.session_id.clone(),
            target: begin.owner.target.clone(),
            command: crate::session_owner::SessionOwnerCommand::Begin,
        };
        if command.fingerprint()? != begin.semantic_digest
            || begin.owner.binding.session_id != begin.session_id
            || reserve.owner.target.id != reserve.owner.binding.id
            || reserve.operation != SessionOwnerOperation::Reserve
            || begin.operation != SessionOwnerOperation::Begin
            || reserve.session_id != begin.session_id
            || reserve.owner.binding != begin.owner.binding
            || begin.owner.state != SessionOwnerState::Active
            || begin.owner.target.id != reserve.owner.target.id
        {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            session: begin.session_id.clone(),
            target: begin.owner.target.clone(),
            reserve: reserve.operation_id.clone(),
            reserve_digest: reserve.semantic_digest.clone(),
            begin: begin.operation_id.clone(),
            begin_digest: begin.semantic_digest.clone(),
            binding: super::super::validation::digest(&begin.owner.binding)?,
        })
    }
}
