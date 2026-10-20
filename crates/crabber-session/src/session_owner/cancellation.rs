//! A cancellation intent and actual signal acknowledgement never prove a join.
use super::{SessionOwnerError as Error, SessionOwnerResolver, SessionOwnerTarget};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerOperationId, SessionRevision};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestSessionOwnerCancel {
    pub id: SessionOwnerOperationId,
    pub session_id: SessionId,
    pub target: SessionOwnerTarget,
    /// None requires an absent Session; Some(R) requires an existing Session at R.
    pub expected_revision: Option<SessionRevision>,
    /// The embedding host authenticates its caller; storage records this assertion.
    pub claim: SessionOwnerResolver,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserveSessionOwnerCancel {
    pub id: SessionOwnerOperationId,
    pub session_id: SessionId,
    pub target: SessionOwnerTarget,
    pub request_id: SessionOwnerOperationId,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerCancellation {
    pub request_id: SessionOwnerOperationId,
    pub requested_at: OffsetDateTime,
    pub observed: Option<SessionOwnerCancelObservation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOwnerCancelObservation {
    pub operation_id: SessionOwnerOperationId,
    pub observed_at: OffsetDateTime,
}
fn scope(session: &SessionId, target: &SessionOwnerTarget) -> Result<(), Error> {
    super::identifier(&session.0, 512, false)?;
    super::identifier(&target.id.0, 128, false)
}
impl RequestSessionOwnerCancel {
    /// # Errors
    /// Rejects malformed identities or an unbounded host assertion.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::identifier(&self.id.0, 128, false)?;
        scope(&self.session_id, &self.target)?;
        super::resolver(&self.claim)?;
        super::digest(&("crabber.session-owner.cancel.v1", self))
    }
}
impl ObserveSessionOwnerCancel {
    /// # Errors
    /// Rejects malformed scope or an acknowledgement reusing its request key.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::identifier(&self.id.0, 128, false)?;
        super::identifier(&self.request_id.0, 128, false)?;
        scope(&self.session_id, &self.target)?;
        if self.id == self.request_id {
            return Err(Error::InvalidRequest);
        }
        super::digest(&("crabber.session-owner.observe-cancel.v1", self))
    }
}
macro_rules! redacted {
    ($($ty:ty),* $(,)?) => {$(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(concat!(stringify!($ty), "([redacted])"))
        }
    })*};
}
redacted!(RequestSessionOwnerCancel, ObserveSessionOwnerCancel);

#[path = "cancellation/record.rs"]
mod record;
pub(crate) use record::Cancellation;
#[path = "cancellation/proof.rs"]
pub(crate) mod proof;

#[cfg(test)]
#[path = "cancellation/compatibility.rs"]
mod compatibility;
