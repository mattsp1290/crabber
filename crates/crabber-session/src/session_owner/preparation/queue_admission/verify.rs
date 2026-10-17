//! Pure original Queue admission evidence. This never reads a mutable Queue head.
use super::super::{
    JoinedPreparation, PreparationCommitError, PreparationKey, PreparationPurpose,
    PreparedOperation,
};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::SessionOwnerError as Error;
use crate::{BoundKeyedAdmitRequest, QueueAction, QueueMutation, QueueReceipt, QueueState};

pub(crate) fn body<'a>(
    joined: &'a JoinedPreparation,
    proof: &mut Proof,
) -> Result<(&'a QueueMutation, &'a BoundKeyedAdmitRequest), PreparationCommitError> {
    let grant = joined.observed_grant();
    let reservation = grant.original_reservation();
    let PreparedOperation::Queue {
        request,
        binding: Some(bound),
    } = joined.observed_operation()
    else {
        return Err(Error::Unsupported.into());
    };
    if reservation.resource_contract.is_none()
        || joined.observed_registration().is_none()
        || !matches!(reservation.purpose, PreparationPurpose::Queue { .. })
        || !matches!(request.action, QueueAction::Admit { .. })
        || !matches!(&reservation.key, PreparationKey::Queue(id) if id == &request.id)
    {
        return Err(Error::Unsupported.into());
    }
    let root = grant.owner_authority();
    let child = grant
        .retained_resources_authority()
        .ok_or(Error::Unsupported)?;
    proof
        .measure(&(
            reservation,
            joined.observed_registration(),
            request,
            bound,
            root.session_id(),
            root.target(),
            child.session_id(),
            child.target(),
        ))
        .map_err(|_| Error::Capacity)?;
    joined.verify_original_intent()?;
    Ok((request, bound))
}

/// The backend has authenticated each full audit against the protected prefix.
pub(crate) fn prepared(
    joined: &JoinedPreparation,
    enqueue: &crate::queue::Audit,
    prepare: &crate::queue::Audit,
    admit: &crate::queue::Audit,
) -> Result<BoundKeyedAdmitRequest, PreparationCommitError> {
    let PreparedOperation::Queue {
        request,
        binding: Some(restored),
    } = joined.observed_operation()
    else {
        return Err(Error::Unsupported.into());
    };
    if let QueueAction::Prepare { prepared, .. } = &prepare.request.action
        && (prepared.binding != restored.binding
            || prepared.keyed.execution != restored.keyed.execution
            || prepared.keyed.options != restored.keyed.options)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    recorded(request, enqueue, prepare, admit)
}
fn recorded(
    request: &QueueMutation,
    enqueue: &crate::queue::Audit,
    prepare: &crate::queue::Audit,
    admit: &crate::queue::Audit,
) -> Result<BoundKeyedAdmitRequest, PreparationCommitError> {
    let QueueAction::Enqueue { queue_id, input } = &enqueue.request.action else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let QueueAction::Prepare { prepared, .. } = &prepare.request.action else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let QueueAction::Admit {
        authority,
        owner,
        lease,
    } = &request.action
    else {
        return Err(Error::Unsupported.into());
    };
    if admit.request.fingerprint()? != request.fingerprint()?
        || admit.request.id != request.id
        || admit.request.session_id != request.session_id
        || admit.authority.is_some()
        || admit.receipt.summary.state != QueueState::Admitted
        || !admit.receipt.summary.prepared
        || admit.receipt.summary.admission.is_none()
        || admit.receipt.summary.queue_id != authority.queue_id
        || admit.receipt.summary.claim_generation != authority.generation
        || enqueue.request.session_id != request.session_id
        || *queue_id != authority.queue_id
        || prepare.request.session_id != request.session_id
        || prepare.receipt.summary.queue_id != *queue_id
        || enqueue.receipt.summary.sequence != admit.receipt.summary.sequence
        || prepare.receipt.summary.sequence != admit.receipt.summary.sequence
        || !(enqueue.receipt.revision < prepare.receipt.revision
            && prepare.receipt.revision < admit.receipt.revision)
        || input.admission != prepared.keyed.options
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let record = crate::queue::Record {
        enqueue_operation: enqueue.request.id.clone(),
        prepare_operation: Some(prepare.request.id.clone()),
        summary: admit.receipt.summary.clone(),
        input: *input.clone(),
        claim: None,
        prepared: Some(*prepared.clone()),
    };
    record.verify_prepared(prepared)?;
    let mut actual = *prepared.clone();
    actual.keyed.request.owner.clone_from(owner);
    actual.keyed.request.lease = *lease;
    actual.binding.validate_keyed(&actual.keyed)?;
    let receipt = admit
        .receipt
        .summary
        .admission
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if receipt.session_id != request.session_id
        || receipt.user_message_id != actual.keyed.request.user_message.id
        || receipt.fingerprint != actual.keyed.options.fingerprint
        || receipt.semantic_digest != actual.keyed.semantic_digest()?
        || receipt.semantic_digest_version
            != if actual.keyed.request.input_envelope.is_some() {
                2
            } else {
                1
            }
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(actual)
}

pub(crate) struct AdmissionSources<'a> {
    pub session: &'a crabber_core::Session,
    pub initial: &'a crate::native_parent::RunIssuance,
    pub witness: &'a super::super::admission::Witness,
    pub audits: super::super::admission::SourceAudits<'a>,
    pub init: &'a crate::admission_journal::Init,
}
pub(crate) fn verify(
    joined: &JoinedPreparation,
    actual: &BoundKeyedAdmitRequest,
    receipt: &QueueReceipt,
    source: &AdmissionSources<'_>,
) -> Result<(), PreparationCommitError> {
    let admission = receipt
        .summary
        .admission
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    source.witness.verify_queue(
        joined.observed_grant(),
        source.session,
        source.initial,
        &source.audits,
    )?;
    source
        .init
        .verify_driver_origin(admission, source.initial)?;
    if source.init.driver_key() != &actual.keyed.options.key
        || source.witness.run_id() != &admission.run_id
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}

pub(crate) fn select<'a>(
    audits: impl IntoIterator<Item = &'a crate::queue::Audit>,
) -> Result<
    (
        &'a crate::queue::Audit,
        &'a crate::queue::Audit,
        &'a crate::queue::Audit,
    ),
    PreparationCommitError,
> {
    let (mut enqueue, mut prepare, mut admit) = (None, None, None);
    for audit in audits {
        let slot = match audit.request.action {
            QueueAction::Enqueue { .. } => &mut enqueue,
            QueueAction::Prepare { .. } => &mut prepare,
            QueueAction::Admit { .. } => &mut admit,
            _ => continue,
        };
        if slot.replace(audit).is_some() {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    Ok((
        enqueue.ok_or(Error::UnknownStoreOutcome)?,
        prepare.ok_or(Error::UnknownStoreOutcome)?,
        admit.ok_or(Error::UnknownStoreOutcome)?,
    ))
}

/// Actual final Queue publication revision, never witness arithmetic or today's head.
pub(crate) fn publication_revision<'a>(
    prefix: impl IntoIterator<Item = &'a crate::queue::Audit>,
    source: &AdmissionSources<'_>,
) -> Result<crabber_core::SessionRevision, PreparationCommitError> {
    source
        .witness
        .verify_sources(source.session, source.initial, &source.audits)?;
    let origin = source
        .audits
        .reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let (PreparationKey::Queue(id), PreparationPurpose::Queue { authority }) =
        (&origin.key, &origin.purpose)
    else {
        return Err(Error::Unsupported.into());
    };
    let (enqueue, prepare, admit) = select(prefix)?;
    let QueueAction::Admit {
        authority: actual_authority,
        ..
    } = &admit.request.action
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let actual = recorded(&admit.request, enqueue, prepare, admit)?;
    let receipt = admit
        .receipt
        .summary
        .admission
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if admit.request.id != *id
        || admit.request.session_id != source.session.id
        || admit.request.expected_revision != origin.expected_revision
        || actual_authority != authority
        || origin.intent != super::super::queue_admission_intent(&actual)?
        || origin.resource_contract.is_none()
        || source.init.driver_key() != &actual.keyed.options.key
        || source.witness.run_id() != &receipt.run_id
        || admit.receipt.revision <= source.witness.admitted_revision()
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    source.init.verify_driver_origin(receipt, source.initial)?;
    Ok(admit.receipt.revision)
}
