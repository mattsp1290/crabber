//! Authenticate actual independent originals before a nested handoff or public write.
use crate::memory::State;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{SessionOwnerError as Error, record::Record};
pub(in crate::memory) fn verify(
    state: &State,
    record: &Record,
    proof: &mut Proof,
) -> Result<(), Error> {
    if record.snapshot.binding.kind != crate::session_owner::SessionOwnerKind::DetachedInvocation {
        return Err(Error::Unsupported);
    }
    let mut current = record;
    while let Some(target) = current
        .snapshot
        .binding
        .parent
        .as_ref()
        .and_then(|parent| parent.invocation.as_ref())
    {
        let parent = state
            .session_owners
            .public_source_record_at(&current.snapshot.binding.session_id, &target.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        crate::session_owner::invocation_parent::original(current, parent)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        current = parent;
    }
    let witness = current.parent.as_ref().ok_or(Error::Unsupported)?;
    if !crate::memory::native_parent::source_protocol::history(
        state,
        &witness.source.session_id,
        &witness.source.run_id,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::Unsupported);
    }
    crate::memory::deletion::owners::verify_parent(state, witness, proof)
        .map_err(|_| Error::UnknownStoreOutcome)
}
