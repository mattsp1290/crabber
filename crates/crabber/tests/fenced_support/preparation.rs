//! Fixture verifier uses the genuine opaque joined proof and pure private rules.
#![allow(dead_code)]
pub use crabber::session::session_owner::preparation::*;
#[path = "../../../crabber-session/src/session_owner/preparation/origin.rs"]
mod origin;
pub(crate) use origin::{Origin, origin};
#[path = "../../../crabber-session/src/session_owner/preparation/identity.rs"]
pub(crate) mod identity;
#[path = "../../../crabber-session/src/session_owner/preparation/proof.rs"]
pub(crate) mod proof;
#[path = "../../../crabber-session/src/session_owner/preparation/source_validation.rs"]
pub(crate) mod source_validation;

#[path = "../../../crabber-session/src/session_owner/preparation/resources/proof.rs"]
pub(crate) mod resource_proof;
#[path = "../../../crabber-session/src/session_owner/preparation/resources/source.rs"]
pub(crate) mod resource_source;

#[path = "../../../crabber-session/src/session_owner/preparation/handoff.rs"]
pub(crate) mod handoff;

#[path = "../../../crabber-session/src/session_owner/preparation/admission.rs"]
pub(crate) mod admission;

#[path = "preparation_recovery.rs"]
pub(crate) mod recovery;

#[path = "../../../crabber-session/src/session_owner/preparation/binding_digest.rs"]
pub(crate) mod binding_digest;
#[path = "preparation_expired.rs"]
pub(crate) mod expired;

pub(crate) mod intent {
    pub(crate) fn execution_binding_digest(
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<crabber_core::InputFingerprint, crate::session_owner::SessionOwnerError> {
        super::AdmissionRecoveryPreparation::fingerprint_binding(binding)
            .map_err(|_| crate::session_owner::SessionOwnerError::InvalidRequest)
    }
}

#[path = "../../../crabber-session/src/session_owner/preparation/inspection.rs"]
pub(crate) mod inspection;

#[path = "preparation_approval_driver.rs"]
pub(crate) mod approval_driver;
#[path = "../../../crabber-session/src/session_owner/preparation/capture.rs"]
mod capture;
#[path = "preparation_driver_completion.rs"]
pub(crate) mod driver_completion;

#[path = "preparation_pending_approval.rs"]
pub(crate) mod pending_approval;

#[path = "preparation_queue_admission.rs"]
pub(crate) mod queue_admission;
#[path = "preparation_tool_driver.rs"]
pub(crate) mod tool_driver;
