//! Immutable nested binding association, independent of dispatch permission.
use crate::session_owner::{SessionOwnerError as Error, SessionOwnerKind, record::Record};
pub(crate) fn original<'a>(
    child: &Record,
    parent: &'a Record,
) -> Result<&'a crate::native_parent::Witness, Error> {
    relation(&child.snapshot.binding, parent)?;
    let witness = parent.parent.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    if child.parent.as_ref() != Some(witness) {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(witness)
}
pub(super) fn relation(
    binding: &crate::session_owner::SessionOwnerBinding,
    parent: &Record,
) -> Result<(), Error> {
    let expected = binding.parent.as_ref().ok_or(Error::InvalidRequest)?;
    let actual = parent
        .snapshot
        .binding
        .parent
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if binding.kind != SessionOwnerKind::DetachedInvocation
        || parent.snapshot.binding.kind != SessionOwnerKind::DetachedInvocation
        || expected.invocation.as_ref() != Some(&parent.snapshot.target)
        || binding.id == parent.snapshot.binding.id
        || expected.phase.is_none()
        || binding.session_id != parent.snapshot.binding.session_id
        || binding.workspace_id != parent.snapshot.binding.workspace_id
        || binding.directory != parent.snapshot.binding.directory
        || expected.run_id != actual.run_id
        || expected.phase != actual.phase
        || expected.call_id != actual.call_id
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
