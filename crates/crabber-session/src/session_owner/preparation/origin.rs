//! Private immutable preparation origin, shared by backend integrity verifiers.
use super::{PreparationKey, PreparationPurpose, ReservePreparation};
use crate::session_owner::{ReserveSessionOwner, SessionOwnerError as Error, digest};
use crabber_core::{InputFingerprint, SessionOwnerOperationId, SessionRevision};
use serde::{Deserialize, Serialize};
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Origin {
    pub operation: SessionOwnerOperationId,
    pub key: PreparationKey,
    pub expected_revision: SessionRevision,
    pub purpose: PreparationPurpose,
    pub intent: InputFingerprint,
}
impl Origin {
    pub fn fingerprint(&self, owner: &ReserveSessionOwner) -> Result<InputFingerprint, Error> {
        crate::session_owner::identifier(&self.operation.0, 128, false)?;
        match &self.purpose {
            PreparationPurpose::OrdinaryAdmission => (),
            PreparationPurpose::ExistingRun { run_id, .. } => {
                crate::session_owner::identifier(&run_id.0, 128, false)?;
            }
            PreparationPurpose::Queue { authority } => {
                crate::session_owner::identifier(&authority.queue_id.0, 128, false)?;
                crate::session_owner::identifier(&authority.session_id.0, 512, false)?;
                crate::session_owner::identifier(&authority.token, 512, false)?;
                if authority.session_id != owner.binding.session_id || authority.generation == 0 {
                    return Err(Error::InvalidRequest);
                }
            }
        }
        let ids = super::identity::ids(&self.key, &owner.binding.session_id)?;
        if self.operation != owner.id
            || self.operation != ids.reserve
            || owner.binding.id != ids.owner
        {
            return Err(Error::InvalidRequest);
        }
        digest(&(
            "crabber.session-preparation.reserve.v1",
            &owner.id,
            &owner.binding,
            self,
        ))
    }
}

pub(crate) fn origin(request: &ReservePreparation) -> Origin {
    Origin {
        operation: request.owner.id.clone(),
        key: request.key.clone(),
        expected_revision: request.expected_revision,
        purpose: request.purpose.clone(),
        intent: request.intent.clone(),
    }
}
