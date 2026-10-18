//! Actual operation inputs and outputs. Only the native root can enter this
//! boundary; metadata rows cannot manufacture the opaque task/resource lease.
use super::{InitialLifecycleKind, CallKind, Correlation, Proof, InputFingerprint, failure, digest, CallResult, Original, AttemptOriginal};
use crate::session_owner::{SessionOwnerError as Error, preparation::PreparationCommitError};
use crate::session_owner::owned_scope::{DrainedInitialResources, OwnedScopeActivation};
use crate::session_owner::driver::{ExecutionDriverGrant, ExecutionDriverOutcome, RegisterExecutionDriver};
use crate::{AttemptAuthority, AttemptConclusion, AttemptMutation, AttemptMutationOutcome};
use crabber_core::{ControlId, EventCursor, EventRecord, Message, RunFence, RunId, SessionRevision};

pub(crate) enum ConsumerInput<'a> {
    Register(&'a RegisterExecutionDriver),
    Activate {request:&'a OwnedScopeActivation, grant:&'a ExecutionDriverGrant},
    KeyedBegin {fence:&'a RunFence},
    ReserveInitial(&'a AttemptMutation),
    BeginInitial {request:&'a AttemptMutation, authority:&'a AttemptAuthority},
    AssistantMessage(&'a Message),
    Lifecycle {kind:InitialLifecycleKind,event:&'a EventRecord},
    Conclude {request:&'a AttemptConclusion,authority:&'a AttemptAuthority,cleanup:&'a DrainedInitialResources<'a>},
}
impl ConsumerInput<'_> {
    pub(crate) fn kind(&self)->CallKind{
        match self {
            Self::Register(_)=>CallKind::Register,Self::Activate{..}=>CallKind::Activate,
            Self::KeyedBegin{..}=>CallKind::KeyedBegin,Self::ReserveInitial(_)=>CallKind::ReserveInitial,
            Self::BeginInitial{..}=>CallKind::BeginInitial,Self::AssistantMessage(_)=>CallKind::AssistantMessage,
            Self::Lifecycle{kind,..}=>kind.call_kind(),Self::Conclude{..}=>CallKind::Conclude,
        }
    }
    pub(crate) fn fingerprint(&self,expected:&Correlation,run:&RunId,proof:&mut Proof)->Result<InputFingerprint,PreparationCommitError>{
        let session=&expected.key.session_id;
        let result=match self {
            Self::Register(v)=>{
                proof.measure(&(v.context.session_id(),v.context.workspace_id(),v.context.directory(),&v.fence)).map_err(|error|failure(&error))?;
                if v.context.session_id()!=session||&v.fence.run_id!=run{return Err(Error::IdentityMismatch.into());}
                v.fingerprint()?
            }
            Self::Activate{request,grant}=>{
                proof.measure(&(*request,grant.receipt(),grant.source_identity())).map_err(|error|failure(&error))?;
                crate::native_enrollment::caller::measure_owner_authority(grant.authority(),proof).map_err(|error|failure(&error))?;
                if request.session_id()!=session||&grant.receipt().run_id!=run{return Err(Error::IdentityMismatch.into());}
                request.fingerprint()?
            }
            Self::KeyedBegin{fence}=>{
                proof.measure(fence).map_err(|error|failure(&error))?;
                if &fence.run_id!=run{return Err(Error::IdentityMismatch.into());}
                digest(&("crabber.initial-consumer.keyed-begin.v1",fence)).map_err(|error|failure(&error))?
            }
            Self::ReserveInitial(request)=>{
                proof.measure(request).map_err(|error|failure(&error))?;
                if !matches!(request.command,crate::AttemptCommand::ReserveBound{..})||request.command.target().generation!=crabber_core::AttemptGeneration::INITIAL{return Err(Error::Unsupported.into());}
                request.fingerprint(session,run).map_err(|_|Error::InvalidRequest)?
            }
            Self::BeginInitial{request,authority}=>{
                proof.measure(request).map_err(|error|failure(&error))?;
                crate::native_enrollment::caller::measure_authority(Some(authority),proof).map_err(|error|failure(&error))?;
                if !matches!(request.command,crate::AttemptCommand::Begin{..})||request.command.target().generation!=crabber_core::AttemptGeneration::INITIAL{return Err(Error::Unsupported.into());}
                request.fingerprint(session,run).map_err(|_|Error::InvalidRequest)?
            }
            Self::AssistantMessage(message)=>{
                proof.measure(message).map_err(|error|failure(&error))?;
                if &message.session_id!=session||message.run_id.as_ref()!=Some(run)||message.role!=crabber_core::Role::Assistant{return Err(Error::InvalidRequest.into());}
                digest(&("crabber.initial-consumer.message-input.v1",message)).map_err(|error|failure(&error))?
            }
            Self::Lifecycle{kind,event}=>{
                proof.measure(event).map_err(|error|failure(&error))?;
                if &event.session_id!=session||&event.run_id!=run||event.kind!=kind.event_kind()||event.cursor.is_some(){return Err(Error::InvalidRequest.into());}
                digest(&("crabber.initial-consumer.lifecycle-input.v1",kind,event)).map_err(|error|failure(&error))?
            }
            Self::Conclude{request,authority,..}=>{
                proof.measure(request).map_err(|error|failure(&error))?;
                crate::native_enrollment::caller::measure_authority(Some(authority),proof).map_err(|error|failure(&error))?;
                if !matches!(request.boundary,crate::RunBoundary::Terminal{..}){return Err(Error::Unsupported.into());}
                request.fingerprint(session,run).map_err(|_|Error::InvalidRequest)?
            }
        };Ok(result)
    }
}
pub(crate) enum ConsumerOutput {
    Registered(Box<ExecutionDriverOutcome>), Activated(Box<crate::session_owner::SessionOwnerOutcome>),
    KeyedBegan{revision:SessionRevision}, Attempt(Box<AttemptMutationOutcome>),
    Message{revision:SessionRevision}, Lifecycle{cursor:EventCursor,revision:SessionRevision},
}
pub(crate) enum ConsumerResult {
    Applied {output:ConsumerOutput,record:CallResult},
    Replayed(CallResult),
}
impl ConsumerResult {
    pub(crate) fn verify_returned(&self,expected:&Correlation,call_id:&ControlId,input:&ConsumerInput<'_>,run:&RunId,proof:&mut Proof)->Result<(),PreparationCommitError>{
        let record=match self{Self::Applied{record,..}|Self::Replayed(record)=>record};
        proof.measure(record).map_err(|error|failure(&error))?;
        if record.correlation!=*expected||&record.call!=call_id||record.original.kind()!=input.kind()
            ||record.original.input_digest().map_err(|error|failure(&error))?!=input.fingerprint(expected,run,proof)?{
            return Err(Error::SemanticConflict.into());
        }
        record.original.verify(&expected.key.session_id,run,proof).map_err(|error|failure(&error))?;
        if let Self::Applied{output,..}=self{verify_native_output(output,&record.original,proof)?;}
        Ok(())
    }
}
pub(crate) fn verify_native_output(output:&ConsumerOutput,original:&Original,proof:&mut Proof)->Result<(),PreparationCommitError>{
    match(output,original){
        (ConsumerOutput::Registered(output),Original::Register(v))=>{
            let ExecutionDriverOutcome::Applied{receipt,grant}=output.as_ref() else{return Err(Error::UnknownStoreOutcome.into());};
            let request=grant.original_request();
            proof.measure(&(receipt,grant.receipt(),grant.source_identity(),request.context.session_id(),
                request.context.workspace_id(),request.context.directory(),&request.fence)).map_err(|error|failure(&error))?;
            crate::native_enrollment::caller::measure_owner_authority(grant.authority(),proof).map_err(|error|failure(&error))?;
            if receipt!=&v.receipt||grant.receipt()!=receipt||request.fingerprint()?!=v.request.fingerprint().map_err(|error|failure(&error))?||*grant.source_identity()!=v.source.identity()?
                ||!grant.authority().authenticates(&v.receipt.session_id,&v.begin.record.snapshot.target,&v.begin.record.token){return Err(Error::UnknownStoreOutcome.into());}
        }
        (ConsumerOutput::Activated(output),Original::Activate(v))=>{
            let crate::session_owner::SessionOwnerOutcome::Applied{receipt,authority:None}=output.as_ref() else{return Err(Error::UnknownStoreOutcome.into());};
            proof.measure(receipt).map_err(|error|failure(&error))?;if receipt!=&v.receipt{return Err(Error::UnknownStoreOutcome.into());}
        }
        (ConsumerOutput::Attempt(output),Original::ReserveInitial(v)|Original::BeginInitial(v))=>{
            let AttemptMutationOutcome::Applied{receipt,authority}=output.as_ref() else{return Err(Error::UnknownStoreOutcome.into());};
            verify_attempt_output(receipt,authority.as_ref(),v,proof)?;
        }
        (ConsumerOutput::Attempt(output),Original::Conclude(v))=>{
            let AttemptMutationOutcome::Applied{receipt,authority}=output.as_ref() else{return Err(Error::UnknownStoreOutcome.into());};
            verify_attempt_output(receipt,authority.as_ref(),&v.attempt,proof)?;
        }
        (ConsumerOutput::Message{revision},Original::AssistantMessage(v)) if *revision==v.revision=>proof.measure(revision).map_err(|error|failure(&error))?,
        (ConsumerOutput::Lifecycle{cursor,revision},Original::Lifecycle(v)) if *cursor==v.cursor&&*revision==v.revision=>proof.measure(&(cursor,revision)).map_err(|error|failure(&error))?,
        (ConsumerOutput::KeyedBegan{revision},Original::KeyedBegin(v)) if *revision==v.revision=>proof.measure(revision).map_err(|error|failure(&error))?,
        _=>return Err(Error::UnknownStoreOutcome.into()),
    }Ok(())
}
fn verify_attempt_output(receipt:&crate::AttemptReceipt,authority:Option<&AttemptAuthority>,v:&AttemptOriginal,proof:&mut Proof)->Result<(),PreparationCommitError>{
    proof.measure(receipt).map_err(|error|failure(&error))?;
    crate::native_enrollment::caller::measure_authority(authority,proof).map_err(|error|failure(&error))?;
    let reserve=matches!(v.request.command,crate::AttemptCommand::ReserveBound{..});
    if receipt!=&v.receipt||reserve!=authority.is_some()||authority.is_some_and(|a|!v.after.matches(a)){
        return Err(Error::UnknownStoreOutcome.into());
    }Ok(())
}
