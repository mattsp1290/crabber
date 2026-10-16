//! Positive resource classification follows authenticated owner-history reads.
use crate::PublicRegisteredOwner;
use crate::native_parent::Witness;
use crate::session_owner::{SessionOwnerKind, SessionOwnerState, owned_scope, record::Record};
#[cfg(test)]
#[path = "coverage/contract.rs"]
pub(crate) mod contract;

pub(crate) struct CheckedOwners {
    pub live: Vec<PublicRegisteredOwner>,
    pub unclassified: bool,
}

/// This predicate grants no coverage on its own. The adapter must first check
/// original Reserve/Begin/activation/join audits and then authenticate this exact
/// parent against its independent native Begin witness, event, receipt and issuer.
pub(crate) fn detached_parent(record: &Record) -> Option<&Witness> {
    let binding = &record.snapshot.binding;
    let joined = record.owned_join.as_ref()?;
    if binding.kind != SessionOwnerKind::DetachedInvocation
        || binding.resource_contract.as_ref()?.as_str()
            != owned_scope::RUNTIME_DETACHED_RESOURCE_CONTRACT_V1
        || record.snapshot.state != SessionOwnerState::Joined
        || record.owned_scope.as_ref() != Some(&joined.activation)
        || !matches!(
            joined.disposition,
            owned_scope::OwnedMainDisposition::Returned
                | owned_scope::OwnedMainDisposition::NoDispatch
        )
        || record.preparation.is_some()
        || record.resource.is_some()
        || record.retained_resources.is_some()
    {
        return None;
    }
    let parent = record.parent.as_ref()?;
    (parent.version == 2
        && parent.run_issuance_digest.is_some()
        && parent.source.kind == crate::native_parent::Kind::Phase)
        .then_some(parent)
}

/// A selected outer-worker join candidate, not a complete Run inventory. The
/// adapter must still authenticate the original `DriverLink` and all its native
/// admission sources after checking owner history under the same caller Proof.
pub(crate) fn execution_driver(
    record: &Record,
) -> Result<
    Option<&crate::session_owner::driver::DriverLink>,
    crate::session_owner::SessionOwnerError,
> {
    let Some(driver) = record.driver.as_deref() else {
        return Ok(None);
    };
    record.validate()?;
    let Some(joined) = &record.owned_join else {
        return Ok(None);
    };
    if record.snapshot.state != SessionOwnerState::Joined
        || !matches!(
            joined.disposition,
            owned_scope::OwnedMainDisposition::Returned
                | owned_scope::OwnedMainDisposition::NoDispatch
        )
    {
        return Ok(None);
    }
    Ok(Some(driver))
}
