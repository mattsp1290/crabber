//! A public lifecycle summary derived only from an actually published owner audit.
use super::{
    SessionOwnerError as Error, SessionOwnerKind, SessionOwnerOperation, audit::RevisionState,
};
use crabber_core::{
    EventKind, EventRecord, PublicAttemptIdentity, PublicInvocationEvent, PublicInvocationIdentity,
    PublicInvocationStatus, PublicSourceIdentity,
};

pub(crate) fn event(
    audit: &super::audit::Audit,
    now: time::OffsetDateTime,
) -> Result<Option<EventRecord>, Error> {
    let receipt = &audit.receipt;
    let owner = &receipt.owner;
    if owner.binding.kind != SessionOwnerKind::DetachedInvocation {
        return Ok(None);
    }
    let (kind, status) = match receipt.operation {
        SessionOwnerOperation::Begin => (
            EventKind::InvocationStarted,
            PublicInvocationStatus::Started,
        ),
        SessionOwnerOperation::Join => (
            EventKind::InvocationFinished,
            PublicInvocationStatus::Finished,
        ),
        SessionOwnerOperation::MarkUncertain => {
            (EventKind::InvocationError, PublicInvocationStatus::Error)
        }
        _ => return Ok(None),
    };
    let parent = owner
        .binding
        .parent
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let Some(native) = audit.record.parent.as_ref() else {
        return Ok(None);
    };
    if native.source.run_id != parent.run_id
        || native.source.session_id != receipt.session_id
        || native.source.phase != parent.phase
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let RevisionState::Present(revision) =
        audit.publication.ok_or(Error::UnknownStoreOutcome)?.after
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    let source = PublicSourceIdentity {
        workspace_id: owner.binding.workspace_id.clone(),
        session_id: receipt.session_id.clone(),
        run_id: parent.run_id.clone(),
        attempt: PublicAttemptIdentity {
            id: native.source.attempt.id.clone(),
            generation: native.source.attempt.generation,
        },
        call_id: parent.call_id.clone(),
        invocation: Some(PublicInvocationIdentity {
            id: crabber_core::InvocationId::from(owner.target.id.0.clone()),
            generation: owner.target.generation,
            parent_invocation: parent
                .invocation
                .as_ref()
                .map(|target| crabber_core::InvocationId::from(target.id.0.clone())),
            parent_generation: parent.invocation.as_ref().map(|target| target.generation),
            parent_call_id: parent.call_id.clone(),
        }),
    };
    let payload = PublicInvocationEvent {
        version: 1,
        source,
        revision,
        status,
    };
    Ok(Some(EventRecord {
        cursor: None,
        session_id: receipt.session_id.clone(),
        run_id: parent.run_id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::to_value(payload).map_err(|_| Error::UnknownStoreOutcome)?,
        correlation: Some(receipt.operation_id.0.clone()),
        live_only: false,
        created_at: now,
    }))
}
