//! Retained actual outputs and one original call's Proof through literal disposal.
use super::super::{
    Arc, Error, NATIVE_BOUND, PreparationCommitError, Proof, RetainedInitial, observe, protocol,
};
use super::{
    ConsumerLease, InitialResources, PendingInitialConsumerNativeCall,
    PendingInitialConsumerNativeReply,
};
use crate::native_enrollment::{
    caller::initial::with_initial_original_async,
    initial_consumer::{CallResult, ConsumerInput, ConsumerOutput, ConsumerResult},
};
use std::sync::Mutex;

#[derive(Default)]
pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) struct Calls {
    pub(super) driver: Mutex<Option<Arc<crate::session_owner::driver::ExecutionDriverGrant>>>,
    pub(super) authority: Mutex<Option<Arc<crate::AttemptAuthority>>>,
    pub(super) begun: Mutex<Option<Arc<super::FreshBegun>>>,
    #[cfg(test)]
    pub(super) budget: Mutex<super::proof::Harness>,
    staged: Mutex<Option<Result<PendingInitialConsumerNativeReply, PreparationCommitError>>>,
    originals: Mutex<Vec<Arc<CallResult>>>,
}
pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) struct Step {
    retained: Arc<RetainedInitial>,
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) proof:
        super::proof::OperationProof,
    expected: protocol::Correlation,
}
impl Step {
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn new(
        retained: &Arc<RetainedInitial>,
        kind: crate::native_enrollment::initial_consumer::CallKind,
    ) -> Result<Self, PreparationCommitError> {
        let mut proof = super::proof::OperationProof::new(retained, kind);
        let identity = retained.identity()?;
        let expected =
            protocol::prefix::expected(&retained.source.original, &identity, &mut proof)?;
        Ok(Self {
            retained: retained.clone(),
            proof,
            expected,
        })
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) async fn invoke(
        self,
        input: &ConsumerInput<'_>,
    ) -> Result<
        (
            ConsumerOutput,
            Arc<CallResult>,
            super::proof::OperationProof,
        ),
        PreparationCommitError,
    > {
        self.invoke_with(input, None).await
    }
    pub(super) async fn invoke_cleanup(
        self,
        input: &ConsumerInput<'_>,
        begun: Arc<super::FreshBegun>,
    ) -> Result<
        (
            ConsumerOutput,
            Arc<CallResult>,
            super::proof::OperationProof,
        ),
        PreparationCommitError,
    > {
        self.invoke_with(input, Some(begun)).await
    }
    fn call_lease(
        &self,
        input: &ConsumerInput<'_>,
        cleanup: Option<Arc<super::FreshBegun>>,
    ) -> Result<ConsumerLease, PreparationCommitError> {
        let control = self
            .retained
            .lock()
            .control
            .clone()
            .ok_or(Error::InvalidTransition)?;
        let call = match &cleanup {
            Some(begun) => {
                begun.verify(input)?;
                control.enter_enrolled_cleanup(begun)?
            }
            None => control
                .enter_consumer_call()?
                .ok_or(Error::InvalidTransition)?,
        };
        Ok(ConsumerLease {
            retained: self.retained.clone(),
            _call: call,
            cleanup,
        })
    }
    async fn invoke_with(
        mut self,
        input: &ConsumerInput<'_>,
        cleanup: Option<Arc<super::FreshBegun>>,
    ) -> Result<
        (
            ConsumerOutput,
            Arc<CallResult>,
            super::proof::OperationProof,
        ),
        PreparationCommitError,
    > {
        let retained = &self.retained;
        let resources = retained.consumer.as_ref().ok_or(Error::Unsupported)?;
        let lease = self.call_lease(input, cleanup)?;
        let birth = retained
            .lock()
            .enrolled
            .clone()
            .ok_or(Error::InvalidTransition)?;
        let call_id = crabber_core::ControlId::new();
        resources.register_call(&call_id, &mut self.proof)?;
        let expected = &self.expected;
        let birth = &birth;
        let call_id = &call_id;
        let lease = &lease;
        let result = with_initial_original_async(
            &retained.source.original,
            &mut self.proof,
            |selected, proof| {
                Box::pin(async move {
                    let association = resources.association()?;
                    let call = PendingInitialConsumerNativeCall {
                        original: &retained.source.original,
                        context: &retained.source.context,
                        birth,
                        expected,
                        call_id,
                        input,
                        selected,
                        association: &association,
                        proof: &mut *proof,
                        lease,
                    };
                    let observed = observe::original(
                        || retained.store.pending_initial_consumer_native(call),
                        |result| {
                            *retained
                                .calls
                                .staged
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
                        },
                        Some(tokio::time::Instant::now() + NATIVE_BOUND),
                        || {
                            retained.status(
                                super::super::PendingInitialProducerStatus::ConsumerDisposalPending,
                            );
                        },
                        || {
                            resources.fail();
                            retained.fail();
                        },
                    )
                    .await;
                    if let Err(failure) = observed {
                        // A deadline has no accepted ACK, but the literal future's
                        // Drop completed successfully. Native reconciliation stays
                        // behind its real gate after actual main/resource joins.
                        // Panic/disposal failures retain sticky physical uncertainty.
                        if failure != observe::Failure::Deadline {
                            resources.fail();
                            retained.lock().observation_failed = true;
                        }
                        retained.fail();
                        return Err(Error::UnknownStoreOutcome.into());
                    }
                    retained
                        .calls
                        .take_verified(retained, expected, call_id, input, birth, proof)
                })
            },
        )
        .await?;
        let ConsumerResult::Applied { output, record } = result else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        #[cfg(test)]
        self.proof.acknowledged();
        let record = Arc::new(record);
        retained
            .calls
            .originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(record.clone());
        Ok((output, record, self.proof))
    }
}
impl Calls {
    fn take_verified(
        &self,
        retained: &RetainedInitial,
        expected: &protocol::Correlation,
        call_id: &crabber_core::ControlId,
        input: &ConsumerInput<'_>,
        birth: &protocol::EnrolledInitialData,
        proof: &mut Proof,
    ) -> Result<ConsumerResult, PreparationCommitError> {
        let mut staged = self
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let verification = if let Ok(reply) = staged.as_ref().ok_or(Error::UnknownStoreOutcome)? {
            reply.verify_correlated(expected, call_id, input, birth, proof)
        } else {
            let Some(Err(error)) = staged.take() else {
                return Err(Error::UnknownStoreOutcome.into());
            };
            return Err(error);
        };
        if let Err(error) = verification {
            // Leave the SAME opaque reply (and actual private grant) in its
            // original slot until joined terminal reconciliation.
            retained.fail();
            return Err(error);
        }
        let Some(Ok(reply)) = staged.take() else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        Ok(reply.into_result())
    }
    #[cfg(test)]
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn has_staged_reply(
        &self,
    ) -> bool {
        self.staged
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(Result::is_ok)
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) async fn dispose_staged(
        &self,
        resources: &Arc<InitialResources>,
    ) {
        let staged = self
            .staged
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Ok(capability) = resources.capability() {
            crabber_extension::with_callback_invocations(capability, async {
                super::super::super::retain::discard(staged);
            })
            .await;
        } else {
            super::super::super::retain::discard(staged);
        }
    }
}
