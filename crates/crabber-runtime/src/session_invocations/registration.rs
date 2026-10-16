//! Bounded native-parent registration and metadata-only retry validation.
use super::{Admission, Error, Invocation, Parent, Request};
use crabber_session::Store;
use crabber_session::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerBinding,
    SessionOwnerCommand as Command, SessionOwnerError, SessionOwnerKind, SessionOwnerOperation,
    SessionOwnerOutcome as Outcome, SessionOwnerParent, SessionOwnerParentAuthority,
    SessionOwnerReceipt, SessionOwnerState, SessionOwnerTermination,
};
use sha2::{Digest, Sha256};
pub(super) struct Started {
    pub scoped: bool,
    pub authority: SessionOwnerAuthority,
    pub source: crabber_session::session_owner::owned_scope::OwnedScopeSource,
}
pub(super) fn validate_request(parent: &Parent, request: &Request) -> Result<(), Error> {
    for (value, bound) in [
        (&request.id.0, 128),
        (&request.reserve_id.0, 128),
        (&request.begin_id.0, 128),
        (&request.join_id.0, 128),
        (&request.component_id, 512),
        (&parent.session.0, 512),
        (&parent.run.0, 128),
    ] {
        if value.is_empty() || value.len() > bound || value.chars().any(char::is_control) {
            return Err(Error::Rejected);
        }
    }
    if parent.workspace.len() > 512
        || parent.directory.len() > 4096
        || parent.workspace.contains('\0')
        || parent.directory.contains('\0')
        || request.reserve_id == request.begin_id
        || request.reserve_id == request.join_id
        || request.begin_id == request.join_id
    {
        return Err(Error::Rejected);
    }
    Ok(())
}
fn reserve(parent: &Parent, request: &Request, scoped: bool) -> Result<ReserveSessionOwner, Error> {
    let behavior_fingerprint = if scoped {
        let encoded = serde_json::to_vec(&("crabber.runtime.detached.registered.v1", request))
            .map_err(|_| Error::Rejected)?;
        crabber_core::InputFingerprint::new(format!("{:x}", Sha256::digest(encoded)))
            .map_err(|_| Error::Rejected)?
    } else {
        request.behavior_fingerprint.clone()
    };
    Ok(ReserveSessionOwner {
        id: request.reserve_id.clone(),
        binding: SessionOwnerBinding {
            session_id: parent.session.clone(),
            workspace_id: parent.workspace.clone(),
            directory: parent.directory.clone(),
            id: request.id.clone(),
            kind: SessionOwnerKind::DetachedInvocation,
            component_id: request.component_id.clone(),
            behavior_fingerprint,
            resource_contract: scoped
                .then(|| {
                    crabber_core::InputFingerprint::new(
                        crabber_session::session_owner::owned_scope::RUNTIME_DETACHED_RESOURCE_CONTRACT_V1,
                    )
                })
                .transpose()
                .map_err(|_| Error::Rejected)?,
            parent: Some(SessionOwnerParent {
                run_id: parent.run.clone(),
                call_id: Some(parent.phase.call_id.clone()),
                phase: Some(parent.phase.clone()),
            }),
        },
        owner: crabber_core::RunId::new().to_string(),
        lease_ms: 30_000,
    })
}
fn valid_reserve(receipt: &SessionOwnerReceipt, request: &ReserveSessionOwner) -> bool {
    request
        .fingerprint()
        .is_ok_and(|digest| receipt.semantic_digest == digest)
        && receipt.operation_id == request.id
        && receipt.session_id == request.binding.session_id
        && receipt.operation == SessionOwnerOperation::Reserve
        && receipt.owner.binding == request.binding
        && receipt.owner.state == SessionOwnerState::ReservedUnstarted
        && receipt.owner.target.id == request.binding.id
        && receipt.owner.target.generation == crabber_core::SessionOwnerGeneration::INITIAL
}
pub(super) async fn replay(
    store: &dyn Store,
    parent: &Parent,
    request: &Request,
) -> Result<Admission, Error> {
    validate_request(parent, request)?;
    let request = reserve(parent, request, store.supports_owned_scopes())?;
    request.fingerprint().map_err(|_| Error::Rejected)?;
    match store
        .lookup_session_owner(&parent.session, &request.id)
        .await
    {
        Ok(Some(receipt)) if valid_reserve(&receipt, &request) => Ok(Admission::Replayed),
        Ok(Some(_)) => Err(Error::Rejected),
        _ => Err(Error::UnknownOutcome),
    }
}
pub(super) async fn start(
    store: &dyn Store,
    parent: &Parent,
    invocation: &Invocation,
) -> Result<Option<Started>, Error> {
    let scoped = store.supports_owned_scopes();
    let request = reserve(parent, &invocation.request, scoped)?;
    request.fingerprint().map_err(|_| Error::Rejected)?;
    let target = crabber_session::session_owner::SessionOwnerTarget {
        id: invocation.request.id.clone(),
        generation: crabber_core::SessionOwnerGeneration::INITIAL,
    };
    if request.id == invocation.request.begin_id
        || request.id == invocation.request.join_id
        || invocation.request.begin_id == invocation.request.join_id
    {
        return Err(Error::Rejected);
    }
    for (id, command) in [
        (invocation.request.begin_id.clone(), Command::Begin),
        (
            invocation.request.join_id.clone(),
            Command::Join {
                termination: SessionOwnerTermination::Returned,
            },
        ),
    ] {
        MutateSessionOwner {
            id,
            session_id: parent.session.clone(),
            target: target.clone(),
            command,
        }
        .fingerprint()
        .map_err(|_| Error::Rejected)?;
    }
    let outcome = store
        .reserve_session_owner(
            request.clone(),
            Some(SessionOwnerParentAuthority::Phase(parent.authority.clone())),
        )
        .await
        .map_err(classify)?;
    let (authority, reserved) = match outcome {
        Outcome::Replayed(receipt) if valid_reserve(&receipt, &request) => return Ok(None),
        Outcome::Applied {
            receipt,
            authority: Some(authority),
        } if valid_reserve(&receipt, &request)
            && authority.session_id() == &parent.session
            && authority.target() == &receipt.owner.target =>
        {
            (authority, receipt)
        }
        _ => return Err(Error::UnknownOutcome),
    };
    let begin = MutateSessionOwner {
        id: invocation.request.begin_id.clone(),
        session_id: parent.session.clone(),
        target: authority.target().clone(),
        command: Command::Begin,
    };
    let digest = begin.fingerprint().map_err(|_| Error::Rejected)?;
    match store
        .mutate_session_owner(begin.clone(), Some(authority.clone()))
        .await
        .map_err(|_| Error::UnknownOutcome)?
    {
        Outcome::Applied {
            receipt,
            authority: None,
        } if receipt.operation_id == begin.id
            && receipt.semantic_digest == digest
            && receipt.session_id == parent.session
            && receipt.operation == SessionOwnerOperation::Begin
            && receipt.owner.binding == request.binding
            && receipt.owner.target == begin.target
            && receipt.owner.state == SessionOwnerState::Active =>
        {
            Ok(Some(Started {
                scoped,
                authority,
                source:
                    crabber_session::session_owner::owned_scope::OwnedScopeSource::from_receipts(
                        &reserved, &receipt,
                    )
                    .map_err(|_| Error::UnknownOutcome)?,
            }))
        }
        // Replayed Begin is never permission to invoke the supplied task factory.
        _ => Err(Error::UnknownOutcome),
    }
}
fn classify(error: SessionOwnerError) -> Error {
    match error {
        SessionOwnerError::UnknownStoreOutcome => Error::UnknownOutcome,
        SessionOwnerError::Unsupported => Error::Unsupported,
        _ => Error::Rejected,
    }
}
