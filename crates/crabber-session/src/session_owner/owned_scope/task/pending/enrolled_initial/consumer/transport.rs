//! One opaque call borrowed from the same dispatched original and its resources.
use super::{ConsumerLease, InitialResourceAssociation};
use crate::native_enrollment::{
    caller::initial::SelectedOriginal,
    initial_consumer::{ConsumerInput, ConsumerResult},
    pending_initial::{Correlation, EnrolledInitialData},
};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{JoinedPreparation, PreparationCommitError};
use crabber_core::{ControlId, SessionContext};

/// Non-Clone private ingress. Adapters can forward the genuine borrowed call;
/// neither a public fence nor metadata can construct it.
pub struct PendingInitialConsumerNativeCall<'a> {
    pub(super) original: &'a JoinedPreparation,
    pub(super) context: &'a SessionContext,
    pub(super) birth: &'a EnrolledInitialData,
    pub(super) expected: &'a Correlation,
    pub(super) call_id: &'a ControlId,
    pub(super) input: &'a ConsumerInput<'a>,
    pub(super) selected: &'a SelectedOriginal<'a>,
    pub(super) association: &'a InitialResourceAssociation<'a>,
    pub(super) proof: &'a mut Proof,
    pub(super) lease: &'a ConsumerLease,
}
/// Closed native borrowed view. Authentication still occurs independently under
/// the original native transaction; local ingress is not Store eligibility.
pub(crate) struct NativeConsumerParts<'a> {
    pub(crate) original: &'a JoinedPreparation,
    pub(crate) context: &'a SessionContext,
    pub(crate) birth: &'a EnrolledInitialData,
    pub(crate) expected: &'a Correlation,
    pub(crate) call_id: &'a ControlId,
    pub(crate) input: &'a ConsumerInput<'a>,
    pub(crate) selected: &'a SelectedOriginal<'a>,
    pub(crate) association: &'a InitialResourceAssociation<'a>,
    pub(crate) proof: &'a mut Proof,
    _lease: &'a ConsumerLease,
}
impl<'a> PendingInitialConsumerNativeCall<'a> {
    #[cfg(test)]
    pub(crate) fn operation_kind(&self) -> crate::native_enrollment::initial_consumer::CallKind {
        self.input.kind()
    }

    pub(crate) fn into_native(self) -> Result<NativeConsumerParts<'a>, PreparationCommitError> {
        self.lease
            .verify(self.original, self.birth, self.expected, self.input)?;
        Ok(NativeConsumerParts {
            original: self.original,
            context: self.context,
            birth: self.birth,
            expected: self.expected,
            call_id: self.call_id,
            input: self.input,
            selected: self.selected,
            association: self.association,
            proof: self.proof,
            _lease: self.lease,
        })
    }
}
/// Actual native response. It contains no dispatch permit or public capability
/// accessor, and cannot be cloned or reconstructed by a Store adapter.
pub struct PendingInitialConsumerNativeReply {
    result: ConsumerResult,
}
impl PendingInitialConsumerNativeReply {
    pub(crate) fn from_result(result: ConsumerResult) -> Self {
        Self { result }
    }
    pub(super) fn verify_correlated(
        &self,
        expected: &Correlation,
        call_id: &ControlId,
        input: &ConsumerInput<'_>,
        birth: &EnrolledInitialData,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        self.result
            .verify_returned(expected, call_id, input, &birth.assignment.run_id, proof)?;
        if !matches!(self.result, ConsumerResult::Applied { .. }) {
            return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into());
        }
        Ok(())
    }
    pub(super) fn into_result(self) -> ConsumerResult {
        self.result
    }
}
