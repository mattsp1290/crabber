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
        if self.active() {
            self.0.charge(bytes).map_err(Into::into)
        } else {
            Ok(())
        }
    }
    pub fn measure(&mut self, carrier: &impl Serialize) -> Result<(), Failure> {
        if self.active() {
            self.0.measure(carrier).map_err(Into::into)
        } else {
            Ok(())
        }
    }
    pub fn used(&self) -> usize {
        self.0.used
    }
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub fn budget(&mut self) -> &mut crate::public_session::budget::Budget {
        &mut self.0
    }
}
impl From<crate::public_session::budget::Failure> for Failure {
    fn from(value: crate::public_session::budget::Failure) -> Self {
        use crate::PublicSessionLimit as Old;
        use crate::public_session::budget::Failure as F;
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
impl From<ReadError<crate::AttemptError>> for ToolReadError {
    fn from(error: ReadError<crate::AttemptError>) -> Self {
        match error {
            ReadError::Domain(_) => Self::Domain(crate::ToolContinuationError::UnknownStoreOutcome),
            ReadError::Budget(error) => Self::Budget(error),
        }
    }
}
pub(crate) type ApprovalReadError = ReadError<crate::ApprovalError>;
impl From<ReadError<crate::StoreError>> for ApprovalReadError {
    fn from(error: ReadError<crate::StoreError>) -> Self {
        match error {
            ReadError::Domain(_) => Self::Domain(crate::ApprovalError::UnknownStoreOutcome),
            ReadError::Budget(error) => Self::Budget(error),
        }
    }
}
impl From<ReadError<crate::AttemptError>> for ApprovalReadError {
    fn from(error: ReadError<crate::AttemptError>) -> Self {
        match error {
            ReadError::Domain(_) => Self::Domain(crate::ApprovalError::UnknownStoreOutcome),
            ReadError::Budget(error) => Self::Budget(error),
        }
    }
}
impl From<ApprovalReadError> for ToolReadError {
    fn from(error: ApprovalReadError) -> Self {
        error.map_domain(|_| crate::ToolContinuationError::UnknownStoreOutcome)
    }
}
impl From<ReadError<crate::RunControlError>> for ToolReadError {
    fn from(error: ReadError<crate::RunControlError>) -> Self {
        error.map_domain(|_| crate::ToolContinuationError::UnknownStoreOutcome)
    }
}

pub(crate) fn validate_request(
    request: &crate::PublicHeaderRequestV2,
) -> Result<(), PublicSessionError> {
    // Validate untrusted routing text before constructing the legacy view.
    if request.session_id.0.is_empty()
        || request.session_id.0.len() > 512
        || request.session_id.0.chars().any(char::is_control)
        || request.run_id.as_ref().is_some_and(|id| {
            id.0.is_empty() || id.0.len() > 128 || id.0.chars().any(char::is_control)
        })
    {
        return Err(PublicSessionError::InvalidRequest);
    }
    let legacy = crate::PublicHeaderRequest {
        session_id: request.session_id.clone(),
        run_id: request.run_id.clone(),
        expected_revision: request.expected_revision,
        max_bytes: request.limits.header_bytes,
        max_proof_bytes: request.limits.proof_bytes,
        max_interactions: request.limits.approval_interactions,
    };
    crate::public_session::budget::validate_request(&legacy)?;
    if request.limits.tool_requirements > 64
        || request.limits.queue_entries > 64
        || request.limits.registered_owners > 1024
        || request.limits.run_controls > 64
    {
        return Err(PublicSessionError::InvalidRequest);
    }
    Ok(())
}
