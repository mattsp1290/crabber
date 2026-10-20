//! State-local entry points reuse the actual canonical native writers.
use crate::memory::{MemoryStore, State};
use crate::native_enrollment::initial_consumer::{self as protocol, ConsumerInput, ConsumerOutput, Original, VerifiedConsumerCall};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{SessionOwnerError as Error,preparation::PreparationCommitError};
use crabber_core::RunFence;
#[path="publishers/driver.rs"] mod driver;
#[path="publishers/attempt.rs"] mod attempt;
#[path="publishers/keyed.rs"] mod keyed;
#[path="publishers/writes.rs"] mod writes;
pub(super) use driver::{register,activate};
pub(super) use attempt::attempt;
pub(super) use keyed::begin_keyed;
pub(super) use writes::{message,lifecycle};
pub(super) enum PreparedNative {
    Register(Box<crate::session_owner::driver::DriverLink>),
    Activate(Box<crate::session_owner::record::Record>),
    Checked,
}
pub(super) fn preflight(state:&State,fence:&RunFence,input:&ConsumerInput<'_>,call:&VerifiedConsumerCall<'_,'_>,now:time::OffsetDateTime,proof:&mut Proof)->Result<PreparedNative,PreparationCommitError>{
    match input {
        ConsumerInput::Register(request)=>{
            if &request.fence!=fence{return Err(Error::StaleOwner.into());}
            if state.session_owners.driver_receipt(request,proof)?.is_some(){return Err(Error::UnknownStoreOutcome.into());}
            let source=crate::memory::session_owner::drivers::fresh_consumer(state,request,now,proof,None,call)?;
            proof.charge(std::mem::size_of::<crate::session_owner::driver::DriverLink>()).map_err(|error|protocol::failure(&error))?;
            return Ok(PreparedNative::Register(Box::new(source)));
        }
        ConsumerInput::Activate{request,grant}=>{
            if &grant.original_request().fence!=fence{return Err(Error::StaleOwner.into());}
            if state.session_owners.driver_activation_receipt_bounded(request,proof)?.is_some(){return Err(Error::UnknownStoreOutcome.into());}
            let record=state.session_owners.driver_activation_record_bounded(request,proof)?;
            if record.cancellation.is_some(){return Err(Error::CancellationPending.into());}
            let revision=state.session_owners.source_audit(request.session_id(),&request.source.begin)
                .and_then(|a|a.publication).ok_or(Error::UnknownStoreOutcome)?.after;
            let source=record.driver.as_deref().ok_or(Error::UnknownStoreOutcome)?;
            if &source.identity()?!=grant.source_identity()||grant.authority().target()!=&record.snapshot.target
                ||&crate::memory::session_owner::drivers::fresh_consumer(state,grant.original_request(),now,proof,Some(revision),call)?!=source {
                return Err(Error::StaleOwner.into());
            }
            crate::session_owner::owned_scope::source::validate_activation(record,request,grant.authority(),now)?;
            let record=copied(record,proof)?;
            proof.charge(std::mem::size_of::<crate::session_owner::record::Record>()).map_err(|error|protocol::failure(&error))?;
            return Ok(PreparedNative::Activate(Box::new(record)));
        }
        ConsumerInput::KeyedBegin{..}=>crate::memory::admission_journal::verify_with_proof(state,&fence.run_id,proof).map_err(|_|Error::UnknownStoreOutcome)?,
        ConsumerInput::ReserveInitial(_)|ConsumerInput::BeginInitial{..}|ConsumerInput::Conclude{..}=>{
            crate::memory::attempt::preflight_consumer(state,fence,input,call,proof).map_err(|_|Error::UnknownStoreOutcome)?;
        }
        ConsumerInput::AssistantMessage(message)=>{proof.measure(message).map_err(|error|protocol::failure(&error))?;}
        ConsumerInput::Lifecycle{event,..}=>{proof.measure(event).map_err(|error|protocol::failure(&error))?;}
    }
    Ok(PreparedNative::Checked)
}
fn copied<T:serde::Serialize+Clone>(value:&T,proof:&mut Proof)->Result<T,PreparationCommitError>{
    proof.measure(value).map_err(|error|protocol::failure(&error))?;Ok(value.clone())
}

fn copied_str(value:&str,proof:&mut Proof)->Result<String,PreparationCommitError>{proof.measure(&value).map_err(|error|protocol::failure(&error))?;Ok(value.to_owned())}
