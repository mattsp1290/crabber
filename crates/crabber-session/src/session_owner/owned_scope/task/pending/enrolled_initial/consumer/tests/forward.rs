//! Real forwarding only; faults retain the actual borrowed opaque call.
use super::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
#[derive(Clone, Copy)]
pub(super) enum Fault {
    Pass,
    HoldConsumer(CallKind),
    Literal(CallKind),
    Pending(CallKind),
    LostAck(CallKind),
    LostSealAckThenHold,
}
#[derive(Debug)]
pub(super) struct RetirementObservation {
    pub disposition: OwnedMainDisposition,
    pub activation: bool,
    pub grant: bool,
}
pub(super) struct Forward {
    pub inner: Arc<dyn Store>,
    pub fault: Fault,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
    pub calls: Mutex<Vec<(CallKind, Option<tokio::task::Id>)>>,
    pub retirement: Mutex<Vec<RetirementObservation>>,
    pub seals: Mutex<Vec<crate::native_enrollment::pending_initial::Identity>>,
}
impl Forward {
    pub fn new(inner: Arc<dyn Store>, fault: Fault) -> Arc<Self> {
        Arc::new(Self {
            inner,
            fault,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            calls: Mutex::default(),
            retirement: Mutex::default(),
            seals: Mutex::default(),
        })
    }
}
struct Pending<'a> {
    _call: PendingInitialConsumerNativeCall<'a>,
}
impl Future for Pending<'_> {
    type Output = Result<PendingInitialConsumerNativeReply, PreparationCommitError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
struct Literal<'a> {
    _call: PendingInitialConsumerNativeCall<'a>,
}
impl Future for Literal<'_> {
    type Output = Result<PendingInitialConsumerNativeReply, PreparationCommitError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("literal consumer poll failure")
    }
}
impl Drop for Literal<'_> {
    fn drop(&mut self) {
        panic!("literal consumer Drop failure");
    }
}
#[async_trait::async_trait]
impl Store for Forward {
    async fn admit_run(
        &self,
        request: crate::AdmitRequest,
    ) -> Result<crate::AdmitOutcome, crate::StoreError> {
        self.inner.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crate::ExecutionStore>, crate::StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crate::StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<crabber_core::Run>, crate::StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crate::StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<crabber_core::Run>, crate::StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crate::StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::StoreError> {
        self.inner.get_extension_state(extension, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crate::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crate::StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }

    async fn pending_initial_native(
        &self,
        call: PendingInitialNativeCall<'_>,
    ) -> Result<PendingInitialNativeReply, PreparationCommitError> {
        if let Some(witness) = call.retirement {
            let view = witness.verify(
                &call.source.original,
                &call.source.context,
                call.expected,
                &mut *call.proof,
            )?;
            if let Some(activation) = view.activation {
                let joined = witness.joined_scope(activation, &mut *call.proof)?;
                assert_eq!(joined.activation().nonce, activation.nonce);
                assert_eq!(joined.inspection().disposition, view.disposition);
                let other = crate::session_owner::owned_scope::OwnedScopeActivation::new(
                    view.grant.unwrap().scope_source()?,
                )?;
                assert!(witness.joined_scope(&other, &mut *call.proof).is_err());
            }
            self.retirement.lock().unwrap().push(RetirementObservation {
                disposition: view.disposition,
                activation: view.activation.is_some(),
                grant: view.grant.is_some(),
            });
        } else {
            assert!(
                !call.is_seal(),
                "scripted Seal omitted its actual joined/drained witness"
            );
        }
        if call.is_seal() && matches!(self.fault, Fault::LostSealAckThenHold) {
            let index = {
                let mut seals = self.seals.lock().unwrap();
                let index = seals.len();
                seals.push(call.expected.identity.clone());
                index
            };
            if index == 0 {
                // The actual native Seal commits before its original reply is
                // lost. This adapter cannot fabricate or clone that reply.
                let _reply = self.inner.pending_initial_native(call).await?;
                return Err(Error::UnknownStoreOutcome.into());
            }
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.pending_initial_native(call).await
    }
    fn pending_initial_consumer_native<'life0, 'life1, 'async_trait>(
        &'life0 self,
        call: PendingInitialConsumerNativeCall<'life1>,
    ) -> BoxFuture<'async_trait, Result<PendingInitialConsumerNativeReply, PreparationCommitError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let kind = call.operation_kind();
        self.calls
            .lock()
            .unwrap()
            .push((kind, tokio::task::try_id()));
        if matches!(self.fault,Fault::Literal(selected) if selected==kind) {
            return Box::pin(Literal { _call: call });
        }
        if matches!(self.fault,Fault::Pending(selected) if selected==kind) {
            return Box::pin(Pending { _call: call });
        }
        let original = self.inner.pending_initial_consumer_native(call);
        if matches!(self.fault,Fault::LostAck(selected) if selected==kind) {
            return Box::pin(async move {
                let _reply = original.await?;
                Err(Error::UnknownStoreOutcome.into())
            });
        }
        if matches!(self.fault,Fault::HoldConsumer(selected) if selected==kind) {
            return Box::pin(async move {
                let reply = original.await;
                if reply.is_ok() {
                    self.entered.notify_one();
                    self.release.notified().await;
                }
                reply
            });
        }
        original
    }
}
