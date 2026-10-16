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
pub(crate) struct Proof(crate::public_session::budget::Budget, bool);
impl Proof {
    pub fn legacy() -> Self {
        Self(
            crate::public_session::budget::Budget::new(
                usize::MAX,
                crate::PublicSessionLimit::ProofBytes,
            ),
            false,
        )
    }
    pub fn active(&self) -> bool {
        self.1
    }
    pub fn bounded(cap: usize) -> Self {
        Self(
            crate::public_session::budget::Budget::new(cap, crate::PublicSessionLimit::ProofBytes),
            true,
        )
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

/// Preserve each legacy domain's exact error while carrying aggregate limits
/// through nested proof readers. Budget failures cannot masquerade as corruption.
#[derive(Debug)]
pub(crate) enum ReadError<E> {
    Domain(E),
    Budget(Failure),
}
impl<E> From<E> for ReadError<E> {
    fn from(error: E) -> Self {
        Self::Domain(error)
    }
}
impl<E> ReadError<E> {
    pub fn into_domain_or(self, limited: E) -> E {
        match self {
            Self::Domain(error) => error,
            Self::Budget(_) => limited,
        }
    }
    pub fn map_domain<F>(self, map: impl FnOnce(E) -> F) -> ReadError<F> {
        match self {
            Self::Domain(error) => ReadError::Domain(map(error)),
            Self::Budget(error) => ReadError::Budget(error),
        }
    }
    pub fn public(self) -> Failure {
        match self {
            Self::Domain(_) => unknown("domain proof"),
            Self::Budget(error) => error,
        }
    }
}
pub(crate) type ToolReadError = ReadError<crate::ToolContinuationError>;
impl From<ReadError<crate::StoreError>> for ToolReadError {
    fn from(error: ReadError<crate::StoreError>) -> Self {
        match error {
            ReadError::Domain(_) => Self::Domain(crate::ToolContinuationError::UnknownStoreOutcome),
            ReadError::Budget(error) => Self::Budget(error),
        }
    }
}
