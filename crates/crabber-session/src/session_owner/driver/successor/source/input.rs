//! Original input is additional evidence, never replacement native provenance.
use super::super::Error;
use crate::native_parent::RunIssuance;
use crate::session_owner::preparation::admission::Witness;
use crate::{AdmissionExecutionCapsule, ExecutionBindingCapsule, admission_journal::Init};

pub(crate) enum Input<'a> {
    Keyed {
        init: &'a Init,
        initial: &'a RunIssuance,
        capsule: &'a AdmissionExecutionCapsule,
        receipt: &'a crabber_core::AdmissionReceipt,
        key: &'a crabber_core::AdmissionKey,
    },
    Unkeyed(&'a Witness),
}
impl Input<'_> {
    pub(crate) fn verify(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        binding: Option<&ExecutionBindingCapsule>,
    ) -> Result<(), Error> {
        match self {
            Self::Keyed {
                init,
                initial,
                capsule,
                receipt,
                key,
            } => {
                init.verify_capsule(receipt, key, capsule)
                    .map_err(unknown)?;
                init.verify_driver_origin(receipt, initial)
                    .map_err(unknown)?;
                if receipt.session_id != *session || receipt.run_id != *run {
                    return Err(Error::SemanticConflict);
                }
                if let Some(binding) = binding
                    && (capsule.request != binding.request
                        || capsule.runtime_semantics != binding.runtime_semantics
                        || capsule.input_envelope != binding.input_envelope
                        || capsule.config_hash != binding.config_hash.as_str()
                        || capsule.plan_fingerprint != binding.plan_fingerprint.as_str()
                        || capsule.behavior_fingerprint != binding.behavior_fingerprint)
                {
                    return Err(Error::SemanticConflict);
                }
            }
            Self::Unkeyed(witness) => {
                if witness.session_id() != session || witness.run_id() != run {
                    return Err(Error::SemanticConflict);
                }
                witness.unkeyed_binding_digest()?;
                if let Some(binding) = binding {
                    witness.verify_unkeyed_binding(binding)?;
                }
            }
        }
        Ok(())
    }
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
