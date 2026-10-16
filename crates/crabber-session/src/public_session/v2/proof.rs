//! One read context follows every actual carrier materialization, including repeats.
use super::PublicSessionLimitV2 as Limit;
use crate::PublicSessionError;
use serde::Serialize;
#[derive(Debug)]
pub(crate) enum Failure {
    Limited(Limit),
    Error(PublicSessionError),
}
pub(crate) fn unknown(_: impl std::fmt::Debug) -> Failure {
    Failure::Error(PublicSessionError::UnknownStoreOutcome)
}
/// One always-present shared proof context. Its Budget also serves the existing
/// qualified approval and cross-domain readers without resetting the allowance.
pub(crate) struct Proof(crate::public_session::budget::Budget);
impl Proof {
    pub fn bounded(cap: usize) -> Self {
        Self(crate::public_session::budget::Budget::new(
            cap,
            crate::PublicSessionLimit::ProofBytes,
        ))
    }
    pub fn charge(&mut self, bytes: usize) -> Result<(), Failure> {
        self.0.charge(bytes).map_err(Into::into)
    }
    pub fn measure(&mut self, carrier: &impl Serialize) -> Result<(), Failure> {
        self.0.measure(carrier).map_err(Into::into)
    }
    pub fn used(&self) -> usize {
        self.0.used
    }
    pub fn budget(&mut self) -> &mut crate::public_session::budget::Budget {
        &mut self.0
    }
}
impl From<crate::public_session::budget::Failure> for Failure {
    fn from(value: crate::public_session::budget::Failure) -> Self {
        use crate::public_session::{PublicSessionLimit as Old, budget::Failure as F};
        match value {
            F::Error(error) => Self::Error(error),
            F::Limited(limit) => Self::Limited(match limit {
                Old::HeaderBytes => Limit::HeaderBytes,
                Old::ProofBytes => Limit::ProofBytes,
                Old::Interactions => Limit::ApprovalInteractions,
                Old::History(limit) => Limit::History(limit),
            }),
        }
    }
}
pub(crate) fn owner(
    snapshot: &crate::session_owner::SessionOwnerSnapshot,
    safe: &mut crate::public_session::budget::Budget,
) -> Result<crate::PublicRegisteredOwner, Failure> {
    #[derive(Serialize)]
    struct View<'a> {
        target: &'a crate::session_owner::SessionOwnerTarget,
        kind: crate::session_owner::SessionOwnerKind,
        component_id: &'a str,
        parent: &'a Option<crate::session_owner::SessionOwnerParent>,
        state: crate::session_owner::SessionOwnerState,
        termination: Option<crate::session_owner::SessionOwnerTermination>,
        created_at: time::OffsetDateTime,
        updated_at: time::OffsetDateTime,
    }
    safe.measure(&View {
        target: &snapshot.target,
        kind: snapshot.binding.kind,
        component_id: &snapshot.binding.component_id,
        parent: &snapshot.binding.parent,
        state: snapshot.state,
        termination: snapshot.termination,
        created_at: snapshot.created_at,
        updated_at: snapshot.updated_at,
    })?;
    Ok(snapshot.into())
}
