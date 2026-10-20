//! The actual closed task call, all native mutations and the protected consumer
//! append run under one original State lock and one caller Proof.
use super::{selected, publish};
use crate::native_enrollment::initial_consumer as protocol;
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::initial_consumer::{ConsumerInput, ConsumerOutput, ConsumerResult, Original, CallDecision, VerifiedConsumerCall};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{SessionOwnerError as Error, preparation::PreparationCommitError};
use crate::session_owner::owned_scope::{PendingInitialConsumerNativeCall,PendingInitialConsumerNativeReply};
use crabber_core::RunFence;
#[path="publishers.rs"]
mod publishers;
#[path="members.rs"]
mod members;
impl MemoryStore {
    pub(in crate::memory) fn apply_pending_initial_consumer(&self,call:PendingInitialConsumerNativeCall<'_>)->Result<PendingInitialConsumerNativeReply,PreparationCommitError>{
        let parts: crate::session_owner::owned_scope::NativeConsumerParts<'_>=call.into_native()?;
        let proof=parts.proof;
        parts.selected.verify(parts.original,proof).map_err(|error|protocol::failure(&error))?;
        parts.association.verify(parts.original,parts.context,parts.expected,proof)?;
        let mut state=self.state.lock().map_err(|_|Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state,parts.context.session_id()).map_err(crate::deletion::owner_error)?;
        crate::memory::pending_initial::native::verify_context(parts.original,parts.expected,parts.context)?;
        let prefix=crate::memory::pending_initial::native::historical(&state,parts.original,parts.selected,proof)?;
        if prefix.correlation(&parts.expected.identity,proof)?!=*parts.expected{return Err(Error::SemanticConflict.into());}
        crate::memory::pending_initial::routes(&state,parts.context.session_id(),proof).map_err(|error|protocol::failure(&error))?;
        let birth=state.pending_initial.selected(&parts.expected.key,&parts.expected.identity,proof).map_err(|error|protocol::failure(&error))?;
        let consumer=selected(&state,parts.expected,proof).map_err(|error|protocol::failure(&error))?;
        parts.birth.verify_returned(parts.selected,proof)?;
        if birth.assignment.ok_or(Error::UnknownStoreOutcome)?.digest().map_err(|error|protocol::failure(&error))?!=parts.birth.assignment.digest().map_err(|error|protocol::failure(&error))?{
            return Err(Error::UnknownStoreOutcome.into());
        }
        members::verify(&state,&consumer,proof).map_err(|error|protocol::failure(&error))?;
        let decision=protocol::prepare_call(&consumer,birth,parts.expected,parts.call_id,parts.input,proof)?;
        let verified=match decision {
            CallDecision::Replayed(record)=>{
                proof.measure(record).map_err(|error|protocol::failure(&error))?;
                return Ok(PendingInitialConsumerNativeReply::from_result(ConsumerResult::Replayed(record.clone())));
            }
            CallDecision::Fresh(verified)=>verified,
        };
        let admitted=admitted(parts.birth)?;
        let fence=&admitted.fence;
        let run=state.runs.get(&fence.run_id).ok_or(Error::NotFound)?;
        proof.measure(&(run,fence)).map_err(|error|protocol::failure(&error))?;
        crate::attempt::verify_run(run,fence,self.clock.now()).map_err(|_|Error::StaleOwner)?;
        let current=crate::memory::native_parent::source_protocol::current(&state,&run.session_id,&run.id,proof)
            .map_err(|error|protocol::failure(&error))?.ok_or(Error::UnknownStoreOutcome)?;
        current.verify(run).map_err(|_|Error::UnknownStoreOutcome)?;
        let prepared=publishers::preflight(&state,fence,parts.input,&verified,self.clock.now(),proof)?;
        if let ConsumerInput::Conclude{cleanup,..}=parts.input{cleanup.verify(parts.association,proof)?;}
        let mut next=state.clone();
        let (output,original)=self.publish_consumer_native(&mut next,fence,(parts.input,prepared),&verified,proof)?;
        protocol::verify_native_output(&output,&original,proof)?;
        let delta=protocol::capture_actual(*verified,original,proof)?;
        let record=delta.result.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        proof.measure(record).map_err(|error|protocol::failure(&error))?;
        let result=ConsumerResult::Applied{output,record:record.clone()};
        publish(&mut next,delta,proof).map_err(|error|protocol::failure(&error))?;
        let reply=PendingInitialConsumerNativeReply::from_result(result);
        #[cfg(test)] before(self,parts.input.kind(),proof)?;
        *state=next;drop(state);
        #[cfg(test)] after(self)?;
        Ok(reply)
    }
    fn publish_consumer_native(&self,state:&mut State,fence:&RunFence,operation:(&ConsumerInput<'_>,publishers::PreparedNative),call:&VerifiedConsumerCall<'_,'_>,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
        let (input,prepared)=operation;
        match (input,prepared) {
            (ConsumerInput::Register(request),publishers::PreparedNative::Register(source))=>publishers::register(self,state,request,*source,proof),
            (ConsumerInput::Activate{request,grant},publishers::PreparedNative::Activate(record))=>publishers::activate(self,state,request,grant,*record,proof),
            (ConsumerInput::KeyedBegin{fence:request},publishers::PreparedNative::Checked)=>{
                if *request!=fence{return Err(Error::StaleOwner.into());}
                publishers::begin_keyed(state,fence,self.clock.now(),proof)
            }
            (ConsumerInput::ReserveInitial(_) | ConsumerInput::BeginInitial{..} | ConsumerInput::Conclude{..},publishers::PreparedNative::Checked)=>publishers::attempt(self,state,fence,input,call,proof),
            (ConsumerInput::AssistantMessage(message),publishers::PreparedNative::Checked)=>publishers::message(state,fence,message,proof),
            (ConsumerInput::Lifecycle{kind,event},publishers::PreparedNative::Checked)=>publishers::lifecycle(state,fence,*kind,event,proof),
            _=>Err(Error::UnknownStoreOutcome.into()),
        }
    }
}
fn admitted(data:&crate::native_enrollment::pending_initial::EnrolledInitialData)->Result<&crate::AdmitOutcome,PreparationCommitError>{
    use crate::session_owner::preparation::PreparationCompletion;
    match &data.completion {
        PreparationCompletion::Admitted(result)=>Ok(result),
        PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started{admitted,..})=>Ok(admitted),
        _=>Err(Error::Unsupported.into()),
    }
}
#[cfg(test)] fn before(store:&MemoryStore,kind:protocol::CallKind,proof:&Proof)->Result<(),PreparationCommitError>{
    store.pending_initial_test_state.consumer_precommit_usage.lock().unwrap().push((kind,proof.used()));
    if store.pending_initial_test_state.fault.load(std::sync::atomic::Ordering::SeqCst)==1{return Err(Error::UnknownStoreOutcome.into());}Ok(())
}
#[cfg(test)] fn after(store:&MemoryStore)->Result<(),PreparationCommitError>{
    if store.pending_initial_test_state.fault.load(std::sync::atomic::Ordering::SeqCst)==2{return Err(Error::UnknownStoreOutcome.into());}Ok(())
}

pub(in crate::memory) fn verify_members(state:&State,view:&protocol::Selection<'_>,proof:&mut Proof)->Result<(),PreparationCommitError>{
    members::verify(state,view,proof).map_err(|error|protocol::failure(&error))
}
