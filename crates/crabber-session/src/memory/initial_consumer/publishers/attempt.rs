use super::{protocol, Proof, PreparationCommitError, MemoryStore, State, RunFence, ConsumerInput, VerifiedConsumerCall, ConsumerOutput, Original, Error, copied};
use crate::{AttemptMutationOutcome,AttemptAuthority};
use protocol::{AttemptOriginal,StoredAttempt,ConclusionOriginal};
#[derive(serde::Serialize)]
struct AttemptView<'a>{
    snapshot:&'a crate::AttemptSnapshot,owner:&'a str,token:&'a str,run_token:&'a str,
    lease_until:time::OffsetDateTime,
}
fn stored(record:&crate::attempt::AttemptRecord,proof:&mut Proof)->Result<StoredAttempt,PreparationCommitError>{
    let view=AttemptView{snapshot:&record.snapshot,owner:&record.owner,token:&record.token,
        run_token:&record.run_token,lease_until:record.lease_until};
    proof.measure(&view).map_err(|error|protocol::failure(&error))?;
    Ok(StoredAttempt{snapshot:record.snapshot.clone(),owner:record.owner.clone(),token:record.token.clone(),
        run_token:record.run_token.clone(),lease_until:record.lease_until})
}
pub(in crate::memory::initial_consumer) fn attempt(store:&MemoryStore,state:&mut State,fence:&RunFence,input:&ConsumerInput<'_>,call:&VerifiedConsumerCall<'_,'_>,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    let (request,authority,conclusion):( _,Option<&AttemptAuthority>,_)=match input{
        ConsumerInput::ReserveInitial(request)=>(*request,None,None),
        ConsumerInput::BeginInitial{request,authority}=>(*request,Some(*authority),None),
        ConsumerInput::Conclude{request,authority,..}=>(&request.finish,Some(*authority),Some(*request)),
        _=>return Err(Error::Unsupported.into()),
    };
    let before=state.attempts.get(&request.command.target().id).map(|r|stored(r,proof)).transpose()?;
    let before_run=conclusion.map(|_|copied(state.runs.get(&fence.run_id).ok_or(Error::NotFound)?,proof)).transpose()?;
    proof.measure(fence).map_err(|error|protocol::failure(&error))?;
    let execution=crate::memory::MemoryExecution{store:store.clone(),fence:fence.clone()};
    let (outcome,publication)=execution.apply_attempt_output(state,copied(request,proof)?,(authority,conclusion,false),crate::memory::attempt::Dispatch::Consumer(call),proof)
        .map_err(|_|Error::UnknownStoreOutcome)?;
    let AttemptMutationOutcome::Applied{receipt,..}=&outcome else{return Err(Error::UnknownStoreOutcome.into());};
    let after=stored(state.attempts.get(&request.command.target().id).ok_or(Error::UnknownStoreOutcome)?,proof)?;
    let event=state.events.get(receipt.event_cursor).ok_or(Error::UnknownStoreOutcome)?;
    let actual=AttemptOriginal{request:copied(request,proof)?,before,after,receipt:copied(receipt,proof)?,event:copied(event,proof)?,
        publication:publication.and_then(|p|p.updated).map(|p|{
            Ok::<_,PreparationCommitError>(protocol::NativePublication{issuer:p.issuer,source:p.source.ok_or(Error::UnknownStoreOutcome)?})
        }).transpose()?};
    let original=if let Some(request)=conclusion{
        let settlement=crate::attempt::terminal::settlement(&actual.receipt,&actual.event).map_err(|_|Error::UnknownStoreOutcome)?;
        let cursor=settlement.cursor.ok_or(Error::UnknownStoreOutcome)?;
        let retained=state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)?;
        if retained!=&settlement{return Err(Error::UnknownStoreOutcome.into());}
        let original=ConclusionOriginal{request:copied(request,proof)?,attempt:actual,
            before_run:before_run.ok_or(Error::UnknownStoreOutcome)?,after_run:copied(state.runs.get(&fence.run_id).ok_or(Error::UnknownStoreOutcome)?,proof)?,
            settlement:copied(retained,proof)?};
        proof.charge(std::mem::size_of::<ConclusionOriginal>()).map_err(|error|protocol::failure(&error))?;
        Original::Conclude(Box::new(original))
    }else{
        proof.charge(std::mem::size_of::<AttemptOriginal>()).map_err(|error|protocol::failure(&error))?;
        if matches!(input,ConsumerInput::ReserveInitial(_)){Original::ReserveInitial(Box::new(actual))}else{Original::BeginInitial(Box::new(actual))}
    };
    proof.charge(std::mem::size_of::<crate::AttemptMutationOutcome>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::Attempt(Box::new(outcome)),original))
}
