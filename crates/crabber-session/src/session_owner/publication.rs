//! Independent SQL transition sources; legacy owners never gain positive coverage.
use super::{SessionOwnerError as Error, audit::Audit};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Coverage {
    Legacy,
    Verified,
}
pub(crate) fn digest(audit: &Audit) -> Result<crabber_core::InputFingerprint, Error> {
    audit.validate()?;
    let inner = super::digest(audit)?;
    super::digest(&("crabber.session-owner-publication.v1", inner))
}

pub(crate) mod scope;
