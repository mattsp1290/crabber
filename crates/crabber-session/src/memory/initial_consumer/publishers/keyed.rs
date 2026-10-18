use super::{State, RunFence, Proof, ConsumerOutput, Original, PreparationCommitError, Error, protocol, copied};
use crate::{AdmissionExecutionState,admission_journal::{Kind,Source}};
pub(in crate::memory::initial_consumer) fn begin_keyed(state:&mut State,fence:&RunFence,now:time::OffsetDateTime,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    crate::memory::admission_journal::verify_with_proof(state,&fence.run_id,proof).map_err(|_|Error::UnknownStoreOutcome)?;
    let run=state.runs.get(&fence.run_id).ok_or(Error::NotFound)?;
    proof.measure(run).map_err(|error|protocol::failure(&error))?;
    crate::attempt::verify_run(run,fence,now).map_err(|_|Error::StaleOwner)?;
    let record=state.admission_executions.get(&run.id).ok_or(Error::Unsupported)?;
    if record.state!=AdmissionExecutionState::Unstarted{return Err(Error::InvalidTransition.into());}
    proof.measure(record).map_err(|error|protocol::failure(&error))?;
    let count=record.capsule.input_envelope.as_ref().map_or(1,|i|i.messages().len());
    let mut messages=Vec::new();
    for message in state.messages.iter().filter(|m|m.run_id.as_ref()==Some(&run.id)&&m.session_id==run.session_id).take(count){
        messages.push(copied(message,proof)?);
    }
    record.validate_messages(run,&messages).map_err(|_|Error::UnknownStoreOutcome)?;
    if state.receipts.get(&(run.session_id.clone(),record.key.clone()))!=Some(&record.receipt){return Err(Error::SemanticConflict.into());}
    let (init,begin)=crate::memory::admission_journal::begin_captured_with_proof(state,&fence.run_id,proof).map_err(|_|Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let record=state.admission_executions.get_mut(&fence.run_id).ok_or(Error::UnknownStoreOutcome)?;
    record.state=AdmissionExecutionState::Started;
    let record=copied(record,proof)?;
    let revision=crate::memory::session_owner::revision(state,&record.receipt.session_id);
    let crate::session_owner::audit::RevisionState::Present(revision)=revision else{return Err(Error::UnknownStoreOutcome.into());};
    let original=protocol::KeyedBeginOriginal{fence:copied(fence,proof)?,init,begin,
        previous_state:AdmissionExecutionState::Unstarted,record,revision};
    // Both captured originals are the actual existing Init and newly appended
    // Begin; there is no fabricated Run publication for this state-only writer.
    if !matches!(state.admission_execution_sources.get(&(fence.run_id.clone(),Kind::Begin)),Some(Source::Begin(_))){return Err(Error::UnknownStoreOutcome.into());}
    proof.charge(std::mem::size_of::<protocol::KeyedBeginOriginal>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::KeyedBegan{revision},Original::KeyedBegin(Box::new(original))))
}
