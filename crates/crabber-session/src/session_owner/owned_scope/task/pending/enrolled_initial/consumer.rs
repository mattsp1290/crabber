//! Closed same-original consumer over the concrete library source.
mod proof;
mod resources;
#[cfg(test)]
pub(crate) use proof::{ConsumerProofCaps, ConsumerProofUsage};
mod calls;
pub(super) use calls::{Calls, Step};
mod run;
mod transport;
pub(super) use resources::InitialResources;
pub(crate) use resources::{DrainedInitialResources, InitialResourceAssociation};
pub(super) use run::run;
pub(crate) use transport::NativeConsumerParts;
pub use transport::{PendingInitialConsumerNativeCall, PendingInitialConsumerNativeReply};

use super::{Arc, Error, PreparationCommitError, RetainedInitial, core};
use crate::native_enrollment::pending_initial::{Correlation, EnrolledInitialData};
use crate::session_owner::preparation::JoinedPreparation;
struct ConsumerLease {
    retained: Arc<RetainedInitial>,
    _call: core::CallAdmission,
    cleanup: Option<Arc<FreshBegun>>,
}
impl ConsumerLease {
    fn verify(
        &self,
        original: &JoinedPreparation,
        birth: &EnrolledInitialData,
        expected: &Correlation,
        input: &crate::native_enrollment::initial_consumer::ConsumerInput<'_>,
    ) -> Result<(), PreparationCommitError> {
        let state = self.retained.lock();
        if state.task != tokio::task::try_id()
            || state.joined
            || state
                .ingress
                .as_ref()
                .is_none_or(|ingress| ingress.is_closed())
            || state.identity.as_deref() != Some(&expected.identity)
            || !std::ptr::eq(original, &raw const self.retained.source.original)
            || state
                .enrolled
                .as_deref()
                .is_none_or(|actual| !std::ptr::eq(actual, birth))
            || self.retained.consumer.is_none()
        {
            return Err(Error::InvalidTransition.into());
        }
        if let Some(cleanup) = &self.cleanup {
            if self
                .retained
                .calls
                .begun
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .is_none_or(|actual| !Arc::ptr_eq(actual, cleanup))
            {
                return Err(Error::InvalidTransition.into());
            }
            cleanup.verify(input)?;
        }
        Ok(())
    }
}

pub(in crate::session_owner::owned_scope::task::pending) struct FreshBegun {
    authority: Arc<crate::AttemptAuthority>,
    receipt: crate::AttemptReceipt,
}
impl FreshBegun {
    fn verify(
        &self,
        input: &crate::native_enrollment::initial_consumer::ConsumerInput<'_>,
    ) -> Result<(), PreparationCommitError> {
        let crate::native_enrollment::initial_consumer::ConsumerInput::Conclude {
            request,
            authority,
            ..
        } = input
        else {
            return Err(Error::InvalidTransition.into());
        };
        if !matches!(
            request.boundary,
            crate::RunBoundary::Terminal {
                status: crabber_core::RunStatus::Interrupted,
                ..
            }
        ) || self.receipt.attempt.state != crate::AttemptState::Started
            || request.finish.command.target() != &self.receipt.attempt.target
            || !authority.matches_store(
                &self.authority.snapshot,
                &self.authority.token,
                &self.authority.run_token,
            )
            || authority.tool_ack_commitment != self.authority.tool_ack_commitment
        {
            return Err(Error::InvalidTransition.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
