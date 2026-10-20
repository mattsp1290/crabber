use super::{protocol, MemoryStore, State, Proof, ConsumerOutput, Original, PreparationCommitError, Error, copied, copied_str};
use crate::session_owner::driver::{ExecutionDriverGrant,ExecutionDriverOutcome,RegisterExecutionDriver};
use crate::session_owner::owned_scope::OwnedScopeActivation;
use protocol::{DriverRequest,RegisterOriginal,ActivateOriginal};
pub(in crate::memory::initial_consumer) fn register(store:&MemoryStore,state:&mut State,request:&RegisterExecutionDriver,source:crate::session_owner::driver::DriverLink,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    proof.measure(&(request.context.session_id(),request.context.workspace_id(),request.context.directory(),&request.fence,&source)).map_err(|error|protocol::failure(&error))?;
    let current=crate::memory::session_owner::revision(state,request.context.session_id());
    let outcome=state.session_owners.register_driver(request,source,current,store.clock.now(),proof)?;
    crate::memory::bump_public_revision(state,request.context.session_id()).map_err(|_|Error::UnknownStoreOutcome)?;
    crate::memory::bump_public_revision(state,request.context.session_id()).map_err(|_|Error::UnknownStoreOutcome)?;
    let ExecutionDriverOutcome::Applied{receipt,..}=&outcome else{return Err(Error::UnknownStoreOutcome.into());};
    let reserve=state.session_owners.source_audit(&receipt.session_id,&receipt.reserve.operation_id).ok_or(Error::UnknownStoreOutcome)?;
    let begin=state.session_owners.source_audit(&receipt.session_id,&receipt.begin.operation_id).ok_or(Error::UnknownStoreOutcome)?;
    let source=reserve.record.driver.as_deref().ok_or(Error::UnknownStoreOutcome)?;
    let request=DriverRequest{session:copied(request.context.session_id(),proof)?,
        workspace:copied_str(request.context.workspace_id(),proof)?,directory:copied_str(request.context.directory(),proof)?,fence:copied(&request.fence,proof)?};
    let original=RegisterOriginal{request,receipt:copied(receipt,proof)?,source:copied(source,proof)?,reserve:copied(reserve,proof)?,begin:copied(begin,proof)?};
    proof.charge(std::mem::size_of::<RegisterOriginal>()).map_err(|error|protocol::failure(&error))?;
    proof.charge(std::mem::size_of::<ExecutionDriverOutcome>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::Registered(Box::new(outcome)),Original::Register(Box::new(original))))
}
pub(in crate::memory::initial_consumer) fn activate(store:&MemoryStore,state:&mut State,request:&OwnedScopeActivation,grant:&ExecutionDriverGrant,record:crate::session_owner::record::Record,proof:&mut Proof)->Result<(ConsumerOutput,Original),PreparationCommitError>{
    let current=crate::memory::session_owner::revision(state,request.session_id());
    let receipt=state.session_owners.activate_scope(record,request,grant.authority(),current,store.clock.now())?;
    crate::memory::bump_public_revision(state,request.session_id()).map_err(|_|Error::UnknownStoreOutcome)?;
    let audit=state.session_owners.source_audit(request.session_id(),&receipt.operation_id).ok_or(Error::UnknownStoreOutcome)?;
    let original=ActivateOriginal{request:copied(request,proof)?,receipt:copied(&receipt,proof)?,audit:copied(audit,proof)?};
    proof.charge(std::mem::size_of::<ActivateOriginal>()).map_err(|error|protocol::failure(&error))?;
    proof.charge(std::mem::size_of::<crate::session_owner::SessionOwnerOutcome>()).map_err(|error|protocol::failure(&error))?;
    Ok((ConsumerOutput::Activated(Box::new(crate::session_owner::SessionOwnerOutcome::Applied{receipt,authority:None})),Original::Activate(Box::new(original))))
}
