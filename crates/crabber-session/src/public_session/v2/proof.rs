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
/// Legacy readers use Unbounded. Aggregate readers always share one Bounded context.
pub(crate) enum Proof {
    Unbounded,
    Bounded { remaining: usize, used: usize },
}
impl Proof {
    pub fn bounded(cap: usize) -> Self {
        Self::Bounded {
            remaining: cap,
            used: 0,
        }
    }
    pub fn charge(&mut self, bytes: usize) -> Result<(), Failure> {
        if let Self::Bounded { remaining, used } = self {
            if bytes > *remaining {
                return Err(Failure::Limited(Limit::ProofBytes));
            }
            *remaining -= bytes;
            *used += bytes;
        }
        Ok(())
    }
    pub fn measure(&mut self, carrier: &impl Serialize) -> Result<(), Failure> {
        if let Self::Bounded { remaining, .. } = self {
            let bytes = crate::snapshot::encoded_bytes(carrier, *remaining)
                .ok_or(Failure::Limited(Limit::ProofBytes))?;
            self.charge(bytes)?;
        }
        Ok(())
    }
    pub fn used(&self) -> usize {
        match self {
            Self::Unbounded => 0,
            Self::Bounded { used, .. } => *used,
        }
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
