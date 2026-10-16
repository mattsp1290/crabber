//! Exact original Reserve/Begin sources, independently authenticated by adapters.
use super::{Error, OwnedScopeActivation, OwnedScopeSource};
use crate::session_owner::{
    SessionOwnerOperation, SessionOwnerState, audit::Audit, record::Record,
};
pub(crate) fn matches_record(source: &OwnedScopeSource, record: &Record) -> Result<(), Error> {
    if source.session != record.snapshot.binding.session_id
        || source.target != record.snapshot.target
        || source.reserve != record.origin
        || source.binding != crate::session_owner::validation::digest(&record.snapshot.binding)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn verify(
    source: &OwnedScopeSource,
    record: &Record,
    reserve: &Audit,
    begin: &Audit,
) -> Result<(), Error> {
    matches_record(source, record)?;
    crate::session_owner::audit::verify_current(&begin.record, reserve, begin)?;
    if OwnedScopeSource::from_receipts(&reserve.receipt, &begin.receipt)? != *source
        || record.origin != source.reserve
        || record.snapshot.binding != begin.record.snapshot.binding
        || record.snapshot.target != source.target
        || record.parent != begin.record.parent
        || record.resource != begin.record.resource
        || record.service != begin.record.service
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn context(record: &Record) -> Option<&OwnedScopeActivation> {
    record
        .owned_scope
        .as_ref()
        .or_else(|| record.owned_join.as_ref().map(|joined| &joined.activation))
}
pub(crate) fn current(
    record: &Record,
    reserve: &Audit,
    begin: &Audit,
    activated: Option<&Audit>,
) -> Result<(), Error> {
    let scope = context(record).ok_or(Error::UnknownStoreOutcome)?;
    verify(&scope.source, record, reserve, begin)?;
    if record.owned_scope.is_none() {
        if !record
            .owned_join
            .as_ref()
            .is_some_and(|joined| joined.disposition == super::OwnedMainDisposition::NoDispatch)
            || activated.is_some()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        return Ok(());
    }
    let activated = activated.ok_or(Error::UnknownStoreOutcome)?;
    activated.validate()?;
    if activated.receipt.operation != SessionOwnerOperation::ActivateScope
        || activated.receipt.operation_id != scope.id
        || activated.receipt.semantic_digest != scope.fingerprint()?
        || activated.record.owned_scope.as_ref() != Some(scope)
        || activated.record.snapshot.target != record.snapshot.target
        || activated.record.snapshot.binding != record.snapshot.binding
        || activated.record.sequence <= begin.record.sequence
        || activated.record.sequence > record.sequence
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn join_candidate(
    record: &Record,
    proof: &super::JoinedOwnedScope,
    reserve: &Audit,
    begin: &Audit,
    activated: Option<&Audit>,
) -> Result<(), Error> {
    let activation = proof.activation();
    verify(&activation.source, record, reserve, begin)?;
    match &record.owned_scope {
        Some(scope) if scope == activation => current(record, reserve, begin, activated),
        None if proof.inspection().disposition == super::OwnedMainDisposition::NoDispatch
            && activated.is_none() =>
        {
            Ok(())
        }
        _ => Err(Error::SemanticConflict),
    }
}

pub(crate) fn activate(
    record: &mut Record,
    request: &OwnedScopeActivation,
    authority: &crate::session_owner::SessionOwnerAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    record.validate()?;
    if request.source.session != record.snapshot.binding.session_id
        || request.source.target != record.snapshot.target
        || !authority.authenticates(
            &request.source.session,
            &request.source.target,
            &record.token,
        )
        || record.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    if record.snapshot.state != SessionOwnerState::Active || record.owned_scope.is_some() {
        return Err(Error::InvalidTransition);
    }
    if record.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    request.fingerprint()?;
    record.owned_scope = Some(request.clone());
    record.publish_change(now, true)
}

pub(crate) fn activation_key(
    session: &crabber_core::SessionId,
    target: &crate::session_owner::SessionOwnerTarget,
) -> Result<crabber_core::SessionOwnerOperationId, Error> {
    let digest = crate::session_owner::validation::digest(&(
        "crabber.owned-scope.activation-key.v1",
        session,
        target,
    ))?;
    Ok(crabber_core::SessionOwnerOperationId(format!(
        "owned-scope-{}",
        digest.as_str()
    )))
}
