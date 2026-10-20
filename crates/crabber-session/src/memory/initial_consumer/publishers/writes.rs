use super::{State, RunFence, Proof, ConsumerOutput, Original, PreparationCommitError, copied, Error, protocol};
use crabber_core::{EventRecord,Message};
pub(in crate::memory::initial_consumer) fn message(state:&mut State,fence:&RunFence,message:&Message,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    let run=copied(state.runs.get(&fence.run_id).ok_or(Error::NotFound)?,proof)?;
    let actual=copied(message,proof)?;
    crate::memory::insert_message(state,&run,actual).map_err(|_|Error::InvalidRequest)?;
    let crate::session_owner::audit::RevisionState::Present(revision)=crate::memory::session_owner::revision(state,&run.session_id) else{return Err(Error::UnknownStoreOutcome.into());};
    let original=protocol::MessageOriginal{message:copied(message,proof)?,revision};
    proof.charge(std::mem::size_of::<protocol::MessageOriginal>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::Message{revision},Original::AssistantMessage(Box::new(original))))
}
pub(in crate::memory::initial_consumer) fn lifecycle(state:&mut State,fence:&RunFence,kind:protocol::InitialLifecycleKind,event:&EventRecord,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    let run=copied(state.runs.get(&fence.run_id).ok_or(Error::NotFound)?,proof)?;
    if event.kind!=kind.event_kind()||event.cursor.is_some(){return Err(Error::InvalidRequest.into());}
    let cursor=crate::memory::insert_event_captured(state,&run,copied(event,proof)?).map_err(|_|Error::InvalidRequest)?;
    let actual=state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)?;
    let crate::session_owner::audit::RevisionState::Present(revision)=crate::memory::session_owner::revision(state,&run.session_id) else{return Err(Error::UnknownStoreOutcome.into());};
    let original=protocol::LifecycleOriginal{kind,input:copied(event,proof)?,event:copied(actual,proof)?,cursor,revision};
    proof.charge(std::mem::size_of::<protocol::LifecycleOriginal>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::Lifecycle{cursor,revision},Original::Lifecycle(Box::new(original))))
}
