//! Real native grants never authorize an old projection after protected reclaim.
use super::super::super::MemoryStore;
use crate::{PublicSourceAuthority,PublicSourceMutation,PublicStateError,Store};
use crabber_core::{Clock,ControlId,ManualClock,PublicSourceChange};
use std::sync::Arc;
use time::OffsetDateTime;

#[tokio::test]
async fn restored_old_run_projection_cannot_reuse_native_source_authority() {
    let clock=Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store=MemoryStore::with_clock(clock.clone());
    let (admitted,_execution,authority,_claim)=crate::phase_contract::fixture(&store).await;
    let original=store.state.lock().unwrap().runs.get(&admitted.run.id).unwrap().clone();
    let revision=store.session_revision(&admitted.session.id).await.unwrap();
    let request=PublicSourceMutation{id:ControlId::new(),expected_revision:revision,change:PublicSourceChange::StateSnapshot{state:serde_json::json!({"original":null})}};
    let receipt=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await.unwrap();
    clock.set(OffsetDateTime::UNIX_EPOCH+time::Duration::seconds(31));
    store.claim_expired_run(&admitted.run.id,"replacement").await.unwrap();
    // Preserve genuine replacement issuer/cohort/history; change only the
    // mutable old Run projection and clock back to its still-live interval.
    let replacement=store.state.lock().unwrap().runs.insert(admitted.run.id.clone(),original).unwrap();
    clock.set(OffsetDateTime::UNIX_EPOCH+time::Duration::seconds(1));
    let replay=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await;
    let before=store.public_state(&admitted.session.id).await.unwrap();
    let mut fresh=request;fresh.id=ControlId::new();fresh.expected_revision=before.revision;
    let refused=store.mutate_bound_public_source(fresh,PublicSourceAuthority::Attempt(authority)).await;
    let after=store.public_state(&admitted.session.id).await.unwrap();
    store.state.lock().unwrap().runs.insert(admitted.run.id,replacement);
    assert_eq!(replay.unwrap(),receipt);
    assert_eq!(refused.unwrap_err(),PublicStateError::StaleOwner);
    assert_eq!(after,before);
    assert_eq!(clock.now(),OffsetDateTime::UNIX_EPOCH+time::Duration::seconds(1));
}

#[tokio::test]
async fn native_public_source_contract() {
    let clock=Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store=MemoryStore::with_clock(clock);
    crate::public_state_contract::source::run(&store).await;
}
#[tokio::test]
async fn native_public_source_fault_contract() {
    let clock=Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store=MemoryStore::with_clock(clock);
    crate::public_state_contract::source::faults(&store,|point|store.public_state_fault.store(point as u8,std::sync::atomic::Ordering::SeqCst)).await;
}

#[tokio::test]
async fn restored_started_attempt_cannot_override_actual_terminal_source() {
    let store=MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    let (admitted,execution,authority,_)=crate::phase_contract::fixture(&store).await;
    let target=authority.snapshot.target.clone();
    let original=store.state.lock().unwrap().attempts.get(&target.id).unwrap().clone();
    let request=PublicSourceMutation{id:ControlId::new(),expected_revision:store.session_revision(&admitted.session.id).await.unwrap(),change:PublicSourceChange::StateSnapshot{state:serde_json::json!({"acknowledged":true})}};
    let receipt=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await.unwrap();
    execution.mutate_attempt(crate::AttemptMutation{id:ControlId::new(),expected_revision:store.session_revision(&admitted.session.id).await.unwrap(),command:crate::AttemptCommand::Finish{target:target.clone(),outcome:crate::AttemptFinish::Suspended}},Some(authority.clone())).await.unwrap();
    let terminal=store.state.lock().unwrap().attempts.insert(target.id.clone(),original).unwrap();
    let before=store.public_state(&admitted.session.id).await.unwrap();
    let replay=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await;
    let fresh=PublicSourceMutation{id:ControlId::new(),expected_revision:before.revision,..request};
    let refused=store.mutate_bound_public_source(fresh,PublicSourceAuthority::Attempt(authority)).await;
    let after=store.public_state(&admitted.session.id).await.unwrap();
    store.state.lock().unwrap().attempts.insert(target.id,terminal);
    assert_eq!(replay.unwrap(),receipt);
    assert_eq!(refused.unwrap_err(),PublicStateError::StaleOwner);
    assert_eq!(after,before);
    assert_eq!(store.get_run(&admitted.run.id).await.unwrap().unwrap().status,crabber_core::RunStatus::Running);
}

async fn started_phase(store:&MemoryStore)->(crate::AdmitOutcome,Box<dyn crate::ExecutionStore>,crate::PhaseAuthority,crate::PhaseTarget) {
    let (admitted,execution,owner,claim)=crate::phase_contract::fixture(store).await;
    let (claimed,grant)=crate::phase_contract::applied(execution.mutate_phase(claim,Some(crate::PhaseWriteAuthority::Attempt(owner))).await.unwrap());
    let grant=grant.unwrap();let target=claimed.phase.target;
    execution.start_tool_phase(crate::PhaseStart{begin:crate::PhaseMutation{id:ControlId::new(),expected_revision:claimed.revision,command:crate::PhaseCommand::Begin{target:target.clone()}},turn_id:None,created_at:admitted.run.created_at},Some(grant.clone())).await.unwrap();
    (admitted,execution,grant,target)
}
#[tokio::test]
async fn restored_started_phase_cannot_override_actual_completed_source() {
    let store=MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    let (admitted,execution,authority,target)=started_phase(&store).await;
    let original=store.state.lock().unwrap().phases.get(&target.id).unwrap().clone();
    let request=PublicSourceMutation{id:ControlId::new(),expected_revision:store.session_revision(&admitted.session.id).await.unwrap(),change:PublicSourceChange::StateSnapshot{state:serde_json::json!({"phase":true})}};
    let receipt=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Phase(authority.clone())).await.unwrap();
    execution.mutate_phase(crate::PhaseMutation{id:ControlId::new(),expected_revision:store.session_revision(&admitted.session.id).await.unwrap(),command:crate::PhaseCommand::Complete{target:target.clone(),output:crate::phase_contract::output()}},Some(crate::PhaseWriteAuthority::Phase(authority.clone()))).await.unwrap();
    let terminal=store.state.lock().unwrap().phases.insert(target.id.clone(),original).unwrap();
    let before=store.public_state(&admitted.session.id).await.unwrap();
    let replay=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Phase(authority.clone())).await;
    let refused=store.mutate_bound_public_source(PublicSourceMutation{id:ControlId::new(),expected_revision:before.revision,..request},PublicSourceAuthority::Phase(authority)).await;
    let after=store.public_state(&admitted.session.id).await.unwrap();
    store.state.lock().unwrap().phases.insert(target.id,terminal);
    assert_eq!(replay.unwrap(),receipt);assert_eq!(refused.unwrap_err(),PublicStateError::StaleOwner);assert_eq!(after,before);
}
async fn reserve_invocation(store:&MemoryStore,admitted:&crate::AdmitOutcome,target:&crate::PhaseTarget,parent:crate::session_owner::SessionOwnerParentAuthority,nested:Option<crate::session_owner::SessionOwnerTarget>)->(crate::session_owner::SessionOwnerAuthority,crabber_core::SessionOwnerOperationId) {
    use crate::session_owner::*;
    let mut request=crate::session_owner_contract::reservation(admitted.session.id.clone());
    request.binding.workspace_id.clone_from(&admitted.session.workspace_id);request.binding.directory.clone_from(&admitted.session.directory);
    request.binding.kind=SessionOwnerKind::DetachedInvocation;
    request.binding.parent=Some(SessionOwnerParent{run_id:admitted.run.id.clone(),phase:Some(target.clone()),call_id:Some(target.call_id.clone()),invocation:nested});
    let id=request.id.clone();
    let SessionOwnerOutcome::Applied{authority:Some(authority),..}=store.reserve_session_owner(request,Some(parent)).await.unwrap() else {panic!("actual native Reserve required");};
    store.mutate_session_owner(MutateSessionOwner{id:crabber_core::SessionOwnerOperationId::new(),session_id:admitted.session.id.clone(),target:authority.target().clone(),command:SessionOwnerCommand::Begin},Some(authority.clone())).await.unwrap();
    (authority,id)
}
#[tokio::test]
async fn nested_source_requires_its_original_immediate_parent_even_after_parent_join() {
    use crate::session_owner::*;
    let store=MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
    let (admitted,_execution,phase,target)=started_phase(&store).await;
    let (parent,original)=reserve_invocation(&store,&admitted,&target,SessionOwnerParentAuthority::Phase(phase),None).await;
    let (child,_)=reserve_invocation(&store,&admitted,&target,SessionOwnerParentAuthority::Invocation(parent.clone()),Some(parent.target().clone())).await;
    store.mutate_session_owner(MutateSessionOwner{id:crabber_core::SessionOwnerOperationId::new(),session_id:admitted.session.id.clone(),target:parent.target().clone(),command:SessionOwnerCommand::Join{termination:SessionOwnerTermination::Returned}},Some(parent.clone())).await.unwrap();
    let request=PublicSourceMutation{id:ControlId::new(),expected_revision:store.session_revision(&admitted.session.id).await.unwrap(),change:PublicSourceChange::StateSnapshot{state:serde_json::json!({"nested":true})}};
    let committed=store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Invocation(child.clone())).await.unwrap();
    let saved=store.state.lock().unwrap().clone();
    store.state.lock().unwrap().session_owners.erase_historical_audit_for_test(&admitted.session.id,&original);
    let before=store.public_state(&admitted.session.id).await.unwrap();
    let failed=store.mutate_bound_public_source(PublicSourceMutation{id:ControlId::new(),expected_revision:before.revision,..request},PublicSourceAuthority::Invocation(child)).await;
    let after=store.public_state(&admitted.session.id).await.unwrap();
    *store.state.lock().unwrap()=saved;
    assert_eq!(failed.unwrap_err(),PublicStateError::UnknownStoreOutcome);assert_eq!(after,before);
    let publication=store.lookup_public_source_publication(&admitted.session.id,&committed.operation_id).await.unwrap().unwrap();
    assert_eq!(publication.receipt,committed);assert_eq!(publication.events.len(),1);
    let source:crabber_core::PublicSourceEvent=serde_json::from_value(publication.events[0].payload.clone()).unwrap();
    assert_eq!(source.source.invocation.unwrap().parent_invocation.unwrap().0,parent.target().id.0);
}

#[tokio::test]
async fn source_whole_native_precommit_uses_one_exact_budget_before_atomic_publish() {
    use std::sync::atomic::Ordering;
    for delta in [false,true] {
        let store=MemoryStore::with_clock(Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH)));
        let (admitted,_execution,authority,_)=crate::phase_contract::fixture(&store).await;
        let session=&admitted.session.id;
        store.mutate_bound_public_source(PublicSourceMutation{id:ControlId::new(),expected_revision:store.session_revision(session).await.unwrap(),change:PublicSourceChange::StateSnapshot{state:serde_json::json!({"start":null})}},PublicSourceAuthority::Attempt(authority.clone())).await.unwrap();
        let change=if delta {PublicSourceChange::StateDelta{delta:vec![serde_json::from_value(serde_json::json!({"op":"add","path":"/end","value":true,"extra":null})).unwrap()]}} else {PublicSourceChange::StateSnapshot{state:serde_json::json!({"end":null})}};
        let request=PublicSourceMutation{id:ControlId::new(),expected_revision:store.session_revision(session).await.unwrap(),change};
        let before=store.public_state(session).await.unwrap();let events=store.list_events(session,None,128).await.unwrap();
        store.public_state_fault.store(1,Ordering::SeqCst);
        assert_eq!(store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await.unwrap_err(),PublicStateError::UnknownStoreOutcome);
        let used=*store.pending_initial_test_state.public_source_usage.lock().unwrap().last().unwrap();
        assert!(used>0);assert_eq!(store.public_state(session).await.unwrap(),before);assert_eq!(store.list_events(session,None,128).await.unwrap(),events);
        *store.pending_initial_test_state.public_source_cap.lock().unwrap()=Some(used-1);
        store.public_state_fault.store(0,Ordering::SeqCst);
        assert_eq!(store.mutate_bound_public_source(request.clone(),PublicSourceAuthority::Attempt(authority.clone())).await.unwrap_err(),PublicStateError::UnknownStoreOutcome);
        assert_eq!(store.public_state(session).await.unwrap(),before);assert_eq!(store.list_events(session,None,128).await.unwrap(),events);assert!(store.lookup_public_state_mutation(session,&request.id).await.unwrap().is_none());
        *store.pending_initial_test_state.public_source_cap.lock().unwrap()=Some(used);
        let receipt=store.mutate_bound_public_source(request,PublicSourceAuthority::Attempt(authority)).await.unwrap();
        assert_eq!(receipt.revision,before.revision.next().unwrap());assert_eq!(*store.pending_initial_test_state.public_source_usage.lock().unwrap().last().unwrap(),used);
        assert_eq!(store.list_events(session,None,128).await.unwrap().len(),events.len()+if delta {2}else{1});
    }
}
