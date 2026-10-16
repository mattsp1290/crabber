//! Capture a generation's bounded metadata before retaining another copy.
use crabber_extension::{ServiceGenerationDescriptor, ServiceGenerationScope};
use crabber_session::session_owner::{
    SessionOwnerError,
    preparation::PreparationGrant,
    service::{
        SessionServiceCleanupPolicy as Policy, SessionServiceGeneration as Generation,
        SessionServiceGenerationScope as Scope,
    },
};

pub(super) fn capture(
    descriptor: &ServiceGenerationDescriptor,
    grant: &PreparationGrant,
) -> Result<Generation, SessionOwnerError> {
    use crabber_extension::ServiceGenerationCleanupPolicy as CapturedPolicy;
    let binding = &grant.original_reservation().owner.binding;
    if descriptor.id().as_str().len() > 128 {
        return Err(SessionOwnerError::InvalidRequest);
    }
    let scope = match descriptor.scope() {
        ServiceGenerationScope::Shared => Scope::Shared,
        ServiceGenerationScope::Workspace {
            workspace_id,
            directory,
        } if workspace_id == &binding.workspace_id && directory == &binding.directory => {
            Scope::Workspace {
                workspace_id: workspace_id.clone(),
                directory: directory.clone(),
            }
        }
        ServiceGenerationScope::Session {
            session_id,
            workspace_id,
            directory,
        } if session_id == &binding.session_id
            && workspace_id == &binding.workspace_id
            && directory == &binding.directory =>
        {
            Scope::Session {
                session_id: session_id.clone(),
                workspace_id: workspace_id.clone(),
                directory: directory.clone(),
            }
        }
        _ => return Err(SessionOwnerError::InvalidRequest),
    };
    let cleanup_policy = match descriptor.cleanup_policy() {
        CapturedPolicy::LegacySharedMount => Policy::LegacySharedMount,
        CapturedPolicy::SealedFactoryLegacyMount => Policy::SealedFactoryLegacyMount,
        CapturedPolicy::FullySealedFactory => Policy::FullySealedFactory,
    };
    let generation = Generation {
        id: descriptor.id().as_str().into(),
        scope,
        registration_fingerprint: descriptor.registration_fingerprint().clone(),
        cleanup_policy,
    };
    generation.validate_scope(
        &binding.session_id,
        &binding.workspace_id,
        &binding.directory,
    )?;
    Ok(generation)
}
