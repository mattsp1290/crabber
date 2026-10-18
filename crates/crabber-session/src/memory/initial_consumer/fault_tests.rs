//! Real transaction failures and lost ACKs, never synthetic committed receipts.
use super::tests::{source,store,MAX};
use super::{selected,Proof};
use crate::{MemoryStore,Store};
use crate::native_enrollment::initial_consumer::{CallKind,Original};
use crate::session_owner::owned_scope::{initial_consumer_fixture,PendingDriverTasks,PendingInitialProducer,
    PendingInitialNativeCall,PendingInitialNativeReply,
    PendingInitialConsumerNativeCall,PendingInitialConsumerNativeReply};
use crate::session_owner::preparation::PreparationCommitError;
use std::sync::{Arc,atomic::{AtomicBool,AtomicUsize,Ordering}};
struct Fault {
    store:Arc<MemoryStore>,kind:CallKind,boundary:u8,observed:AtomicBool,seal:Option<Arc<SealHold>>,
}
pub(super) struct SealHold {
    calls:AtomicUsize,
    pub(super) entered:tokio::sync::Notify,
    pub(super) release:tokio::sync::Notify,
}
pub(super) fn held_seal(store:Arc<MemoryStore>)->(Arc<dyn Store>,Arc<SealHold>){
    let seal=Arc::new(SealHold{calls:AtomicUsize::new(0),entered:tokio::sync::Notify::new(),release:tokio::sync::Notify::new()});
    let adapter=Arc::new(Fault{store,kind:CallKind::Register,boundary:0,observed:AtomicBool::new(false),seal:Some(seal.clone())});
    (adapter,seal)
}
#[async_trait::async_trait]
impl Store for Fault {
    async fn admit_run(
        &self,
        request: crate::AdmitRequest,
    ) -> Result<crate::AdmitOutcome, crate::StoreError> {
        self.store.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crate::ExecutionStore>, crate::StoreError> {
        self.store.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crate::StoreError> {
        self.store.get_session(id).await
    }
    async fn get_run(&self, id: &crabber_core::RunId) -> Result<Option<crabber_core::Run>, crate::StoreError> {
        self.store.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.store.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.store.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crate::StoreError> {
        self.store.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<crabber_core::Run>, crate::StoreError> {
        self.store.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &crabber_core::RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
        self.store.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &crabber_core::RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crate::StoreError> {
        self.store.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::StoreError> {
        self.store.get_extension_state(extension, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crate::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crate::StoreError> {
        self.store.enqueue_inbox(session, kind, message).await
    }

    async fn pending_initial_native(&self,call:PendingInitialNativeCall<'_>)->Result<PendingInitialNativeReply,PreparationCommitError>{
        if let Some(seal)=&self.seal {
            // This genuine fixture executes Open, Complete, then joined Seal.
            // Counting only injects refusal/holding; it authenticates no call.
            match seal.calls.fetch_add(1,Ordering::SeqCst) {
                2=>return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
                3=>{seal.entered.notify_one();seal.release.notified().await;}
                _=>{}
            }
        }
        Store::pending_initial_native(&*self.store,call).await
    }
    async fn pending_initial_consumer_native(&self,call:PendingInitialConsumerNativeCall<'_>)->Result<PendingInitialConsumerNativeReply,PreparationCommitError>{
        let fault=call.operation_kind()==self.kind&&!self.observed.swap(true,Ordering::SeqCst);
        if fault{self.store.pending_initial_test_state.fault.store(self.boundary,Ordering::SeqCst);}
        let result=Store::pending_initial_consumer_native(&*self.store,call).await;
        if fault{self.store.pending_initial_test_state.fault.store(0,Ordering::SeqCst);}
        result
    }
}
#[tokio::test]
async fn native_call_first_lost_ack_is_closed_and_rollback_has_no_member(){
    for keyed in [false,true]{for boundary in [1,2]{
        let store=store();let source=source();
        let (context,original)=initial_consumer_fixture::prepared(&*store,keyed,&source).await;
        let adapter=Arc::new(Fault{store:store.clone(),kind:CallKind::Register,boundary,observed:AtomicBool::new(false),seal:None});
        let owner=PendingDriverTasks::default();
        let mut task=PendingInitialProducer::register_scripted(&owner,adapter.clone(),context,original,source).unwrap();
        task.start().unwrap();let observed=task.join().await.unwrap();
        assert!(observed.error().is_some(),"actual native failure was masked: {:?}",observed.status());
        assert!(adapter.observed.load(Ordering::SeqCst));
        owner.wait_for_supervision().await;
        let state=store.state.lock().unwrap();
        let origin=state.initial_consumer_cohorts.values().next().unwrap();
        let view=selected(&state,origin.correlation(),&mut Proof::bounded(MAX)).unwrap();
        assert!(view.close.is_some(),"joined/drained partial member was not paired closed");
        let committed=view.results.iter().filter(|r|matches!(r.original,Original::Register(_))).count();
        assert_eq!(committed,usize::from(boundary==2));
        assert!(view.results.iter().all(|r|!matches!(r.original,Original::Activate(_)|Original::ReserveInitial(_)|Original::BeginInitial(_)|Original::AssistantMessage(_)|Original::Conclude(_))));
    }}
}

struct RefuseOpen {store:Arc<MemoryStore>,attempted:AtomicBool}
#[async_trait::async_trait]
impl Store for RefuseOpen {
    async fn admit_run(
        &self,
        request: crate::AdmitRequest,
    ) -> Result<crate::AdmitOutcome, crate::StoreError> {
        self.store.admit_run(request).await
    }
    async fn execution(
        &self,
        fence: crabber_core::RunFence,
    ) -> Result<Box<dyn crate::ExecutionStore>, crate::StoreError> {
        self.store.execution(fence).await
    }
    async fn get_session(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Option<crabber_core::Session>, crate::StoreError> {
        self.store.get_session(id).await
    }
    async fn get_run(&self, id: &crabber_core::RunId) -> Result<Option<crabber_core::Run>, crate::StoreError> {
        self.store.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &crabber_core::SessionId,
        epoch: Option<crabber_core::EpochId>,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.store.list_messages(id, epoch).await
    }
    async fn list_all_messages(
        &self,
        id: &crabber_core::SessionId,
    ) -> Result<Vec<crabber_core::Message>, crate::StoreError> {
        self.store.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &crabber_core::SessionId,
        after: Option<crabber_core::EventCursor>,
        limit: usize,
    ) -> Result<Vec<crabber_core::EventRecord>, crate::StoreError> {
        self.store.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<crabber_core::Run>, crate::StoreError> {
        self.store.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &crabber_core::RunId,
    ) -> Result<Vec<crabber_core::ToolCallRecord>, crate::StoreError> {
        self.store.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(
        &self,
        run: &crabber_core::RunId,
        owner: &str,
    ) -> Result<crabber_core::RunFence, crate::StoreError> {
        self.store.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension: &str,
        session: &crabber_core::SessionId,
    ) -> Result<std::collections::BTreeMap<String, String>, crate::StoreError> {
        self.store.get_extension_state(extension, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &crabber_core::SessionId,
        kind: crate::InboxKind,
        message: crabber_core::Message,
    ) -> Result<(), crate::StoreError> {
        self.store.enqueue_inbox(session, kind, message).await
    }

    async fn pending_initial_native(&self,call:PendingInitialNativeCall<'_>)->Result<PendingInitialNativeReply,PreparationCommitError>{
        if !self.attempted.swap(true,Ordering::SeqCst){return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into());}
        Store::pending_initial_native(&*self.store,call).await
    }
    async fn pending_initial_consumer_native(&self,call:PendingInitialConsumerNativeCall<'_>)->Result<PendingInitialConsumerNativeReply,PreparationCommitError>{
        Store::pending_initial_consumer_native(&*self.store,call).await
    }
}
#[tokio::test]
async fn actual_armed_open_refusal_seals_no_opened_member_without_fake_registration(){
    for keyed in [false,true]{
        let store=store();let source=source();
        let (context,original)=initial_consumer_fixture::prepared(&*store,keyed,&source).await;
        let adapter=Arc::new(RefuseOpen{store:store.clone(),attempted:AtomicBool::new(false)});
        let owner=PendingDriverTasks::default();
        let mut task=PendingInitialProducer::register_scripted(&owner,adapter.clone(),context,original,source).unwrap();
        task.start().unwrap();let observed=task.join().await.unwrap();
        assert!(adapter.attempted.load(Ordering::SeqCst));
        assert!(observed.error().is_some());
        assert!(matches!(observed.status(),crate::session_owner::owned_scope::PendingInitialProducerStatus::ClosedNoPublication),"actual pairedclose: {:?}, error={:?}",observed.status(),observed.error());
        owner.wait_for_supervision().await;
        let state=store.state.lock().unwrap();assert!(state.runs.is_empty());
        let origin=state.initial_consumer_cohorts.values().next().unwrap();
        assert!(matches!(origin.as_ref(),crate::native_enrollment::initial_consumer::Origin::NoOpenedMember(_)));
        let view=selected(&state,origin.correlation(),&mut Proof::bounded(MAX)).unwrap();
        assert!(view.calls.is_empty()&&view.results.is_empty()&&view.assignment.is_none());
        let birth=state.pending_initial.selected(&origin.correlation().key,&origin.correlation().identity,&mut Proof::bounded(MAX)).unwrap();
        assert!(birth.epoch.unwrap().kind==crate::native_enrollment::pending_initial::EpochKind::SealedBeforeOpen);
        assert!(birth.registered.is_none()&&birth.assignment.is_none());
        view.verify_birth(birth,&mut Proof::bounded(MAX)).unwrap();
    }
}
