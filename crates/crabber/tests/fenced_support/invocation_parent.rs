//! Reuse immutable nested attribution; fresh native handoff stays unavailable.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerError, record::Record,
};

#[path = "../../../crabber-session/src/session_owner/invocation_parent/original.rs"]
mod canonical;
pub(crate) use canonical::original;

pub(crate) fn fresh<'a>(
    _: &ReserveSessionOwner,
    _: &'a Record,
    _: &SessionOwnerAuthority,
    _: time::OffsetDateTime,
    _: &mut Proof,
) -> Result<&'a crate::native_parent::Witness, SessionOwnerError> {
    Err(SessionOwnerError::Unsupported)
}
