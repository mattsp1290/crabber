//! Exact actual operation Proofs: native commit and returned ACK are separate.
use super::tests::{source,store,MAX};
use super::{State,selected,Proof};
use crate::session_owner::owned_scope::{initial_consumer_fixture,ConsumerProofCaps,ConsumerProofUsage,
    PendingDriverTasks,PendingInitialProducer};
use crate::session_owner::preparation::JoinedPreparation;
use crate::native_enrollment::initial_consumer::CallKind;
use crabber_core::SessionContext;
use std::sync::{Arc,Mutex};
struct Fixture{state:State,context:SessionContext,original:JoinedPreparation}
struct Observed{
    usage:ConsumerProofUsage,native:Option<usize>,members:usize,failed:bool,closed:bool,
}
impl Fixture {
    async fn new(keyed:bool)->Self{
        let store=store();let source=source();
        let (context,original)=initial_consumer_fixture::prepared(&*store,keyed,&source).await;
        let state=store.state.lock().unwrap().clone();Self{state,context,original}
    }
    async fn run(&self,kind:CallKind,cap:usize)->Observed{
        let store=store();*store.state.lock().unwrap()=self.state.clone();
        let usage=Arc::new(Mutex::new(Vec::new()));let owner=PendingDriverTasks::default();
        let mut task=PendingInitialProducer::register_scripted_with_caps(&owner,store.clone(),self.context.clone(),
            self.original.clone(),source(),ConsumerProofCaps(vec![(kind,cap)]),usage.clone()).unwrap();
        task.start().unwrap();let observed=task.join().await.unwrap();
        let failed=observed.error().is_some();
        assert!(!observed.observation_failed(),"actual literal observation failed: {:?}",observed.error());
        owner.wait_for_supervision().await;
        let usage=usage.lock().unwrap().iter().copied().find(|u|u.kind==kind).unwrap();
        let native=store.pending_initial_test_state.consumer_precommit_usage.lock().unwrap().iter()
            .find_map(|(actual,used)|(*actual==kind).then_some(*used));
        let state=store.state.lock().unwrap();let origin=state.initial_consumer_cohorts.values().next().unwrap();
        let view=selected(&state,origin.correlation(),&mut Proof::bounded(MAX)).unwrap();
        let members=view.results.iter().filter(|r|r.original.kind()==kind).count();
        Observed{usage,native,members,failed,closed:view.close.is_some()}
    }
}
#[tokio::test]
async fn actual_consumer_native_n_commits_and_n_minus_one_rolls_back(){
    for keyed in [false,true]{for kind in [CallKind::Register,CallKind::BeginInitial,CallKind::Conclude]{
        let fixture=Fixture::new(keyed).await;let baseline=fixture.run(kind,MAX).await;
        let label=serde_json::to_string(&kind).unwrap();
        assert!(!baseline.failed&&baseline.usage.acknowledged&&baseline.closed,"baseline {label}, keyed={keyed}");
        let native=baseline.native.unwrap();assert!(native>0&&native<baseline.usage.used);
        let exact=fixture.run(kind,native).await;
        assert!(exact.failed&&!exact.usage.acknowledged&&exact.closed);
        assert_eq!(exact.native,Some(native),"native exact {label}, keyed={keyed}");assert_eq!(exact.members,1);
        let short=fixture.run(kind,native-1).await;
        assert!(short.failed&&!short.usage.acknowledged&&short.closed);
        assert_eq!(short.native,None,"native short {label}, keyed={keyed}");assert_eq!(short.members,0);
    }}
}
#[tokio::test]
async fn actual_consumer_returned_ack_n_and_n_minus_one_preserve_native_outcome(){
    for keyed in [false,true]{for kind in [CallKind::Register,CallKind::BeginInitial,CallKind::Conclude]{
        let fixture=Fixture::new(keyed).await;let baseline=fixture.run(kind,MAX).await;
        let label=serde_json::to_string(&kind).unwrap();
        let whole=baseline.usage.used;assert!(whole>baseline.native.unwrap());
        let exact=fixture.run(kind,whole).await;
        assert!(!exact.failed&&exact.usage.acknowledged&&exact.closed);assert_eq!(exact.usage.used,whole,"whole exact {label}, keyed={keyed}");
        let short=fixture.run(kind,whole-1).await;
        assert!(short.failed&&!short.usage.acknowledged&&short.closed);
        // ACK exhaustion occurs after the actual native commit. It cannot
        // relabel the protected member as absent or permit another grant.
        assert_eq!(short.members,1);assert!(short.native.is_some());
    }}
}

struct ReleaseSeal(Arc<super::fault_tests::SealHold>);
impl Drop for ReleaseSeal{fn drop(&mut self){self.0.release.notify_one();}}
#[tokio::test]
async fn actual_ack_refusal_retains_original_reply_until_paired_close_ack(){
    for keyed in [false,true]{
        let fixture=Fixture::new(keyed).await;
        let baseline=fixture.run(CallKind::Register,MAX).await;
        let cap=baseline.usage.used-1;
        let store=store();*store.state.lock().unwrap()=fixture.state.clone();
        let (adapter,hold)=super::fault_tests::held_seal(store.clone());
        let _release=ReleaseSeal(hold.clone());
        let owner=PendingDriverTasks::default();let usage=Arc::new(Mutex::new(Vec::new()));
        let mut task=PendingInitialProducer::register_scripted_with_caps(&owner,adapter,fixture.context.clone(),
            fixture.original.clone(),source(),ConsumerProofCaps(vec![(CallKind::Register,cap)]),usage.clone()).unwrap();
        let control=task.control();task.start().unwrap();let observed=task.join().await.unwrap();
        assert!(observed.error().is_some()&&!observed.observation_failed());
        assert!(control.original_join_observed());assert!(control.consumer_reply_staged());
        let mut retry=observed.take_retry().expect("real joined Seal refusal retains retry");
        let mut attempt=Box::pin(retry.retry());
        tokio::time::timeout(std::time::Duration::from_secs(2),async{
            tokio::select!{
                ()=hold.entered.notified()=>{},
                result=&mut attempt=>panic!("held real Seal returned early: {}",result.is_ok()),
            }
        }).await.unwrap();
        assert!(control.consumer_reply_staged(),"same opaque actual grant survives held paired close");
        {
            let state=store.state.lock().unwrap();let origin=state.initial_consumer_cohorts.values().next().unwrap();
            let view=selected(&state,origin.correlation(),&mut Proof::bounded(MAX)).unwrap();
            assert!(view.close.is_none());assert_eq!(view.results.len(),1);
        }
        hold.release.notify_one();
        let closed=tokio::time::timeout(std::time::Duration::from_secs(2),attempt).await.unwrap().unwrap();
        assert!(matches!(closed,crate::session_owner::owned_scope::PendingInitialProducerStatus::ClosedAssignment));
        assert!(!control.consumer_reply_staged(),"actual opaque reply moves only after terminal paired ACK");
        owner.wait_for_supervision().await;
        let usage=usage.lock().unwrap();let register=usage.iter().find(|v|v.kind==CallKind::Register).unwrap();
        assert!(!register.acknowledged);
    }
}
