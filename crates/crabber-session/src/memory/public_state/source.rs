//! One Memory gate for native source validation, patch and public publication.
use super::{Error, MemoryStore, OperationReceipt, PublicStateReceipt, public_state, revision};
use crate::{PublicSourceAuthority as Authority, PublicSourceMutation};
use crate::public_session::v2::proof::Proof;
use crabber_core::{PublicSourceChange, RunFence};

impl MemoryStore {
    pub(in crate::memory) fn write_public_source(
        &self, mutation:PublicSourceMutation, authority:&Authority, fence:Option<&RunFence>,
    )->Result<PublicStateReceipt,Error> {
        mutation.validate()?;
        let result=self.transact(|state| {
            #[cfg(not(test))]
            let cap=32<<20;
            #[cfg(test)]
            let cap=self.pending_initial_test_state.public_source_cap.lock().unwrap().unwrap_or(32<<20);
            let mut proof=Proof::bounded(cap);
            let session=authority.session();
            super::super::deletion::ensure_live(state,session).map_err(|_|Error::NotFound)?;
            let owner=match authority {
                Authority::Invocation(owner)=>Some(state.session_owners.public_source_record(owner,&mut proof).map_err(|_|Error::UnknownStoreOutcome)?),
                Authority::Attempt(_)|Authority::Phase(_)=>None,
            };
            if let Some(record)=owner {super::super::session_owner::invocation::verify(state,record,&mut proof).map_err(|_|Error::UnknownStoreOutcome)?;}
            let run_id=if let Some(record)=owner {
                &record.snapshot.binding.parent.as_ref().ok_or(Error::UnknownStoreOutcome)?.run_id
            } else { authority.run().ok_or(Error::StaleOwner)? };
            let run=state.runs.get(run_id).ok_or(Error::NotFound)?;
            proof.measure(run).map_err(|_|Error::InvalidRequest)?;
            super::super::credential_access::check(state,run,&mut proof).map_err(public_state::unknown)?;
            let workspace=&state.sessions.get(session).ok_or(Error::NotFound)?.workspace_id;
            let source=authority.identity(workspace,run,owner,&mut proof)?;
            if fence.is_some_and(|fence|fence.run_id!=*run_id) {return Err(Error::StaleOwner);}
            let key=(session.clone(),mutation.id.clone());
            if let Some(saved)=state.operation_receipts.get(&key) {
                let OperationReceipt::PublicState(receipt)=saved else {return Err(Error::SemanticConflict);};
                let event=state.events.get(receipt.event_cursor).ok_or(Error::UnknownStoreOutcome)?;
                if event.payload.get("version").and_then(serde_json::Value::as_u64)!=Some(2) {return Err(Error::SemanticConflict);}
                public_state::verify_source_event_with_proof(receipt,event,&mut proof)?;
                if let Some(cursor)=public_state::before_event(event)? {
                    let before=state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)?;
                    public_state::verify_before_event_with_proof(event,before,&mut proof)?;
                }
                let event=state.events.get(receipt.event_cursor).ok_or(Error::UnknownStoreOutcome)?;
                public_state::source_matches(event,&source,&mutation,&mut proof)?;
                proof.measure(receipt).map_err(|_|Error::UnknownStoreOutcome)?;
                return Ok(receipt.clone());
            }
            if !matches!(authority,Authority::Invocation(_)) {
                super::super::native_parent::current_issuance_with_proof(state,run,&mut proof).map_err(|_|Error::StaleOwner)?;
            }
            if let Some(fence)=fence {public_state::verify_owner(run,fence,self.clock.now())?;}
            let attempt=authority.attempt().and_then(|target|state.attempts.get(&target.id));
            if let Some(selected)=attempt {
                let latest=state.attempts.values().filter(|record|record.snapshot.run_id==run.id)
                    .max_by_key(|record|record.snapshot.target.generation);
                if latest.is_none_or(|latest|latest.snapshot.target!=selected.snapshot.target) {return Err(Error::StaleOwner);}
            }
            let phase=authority.phase().and_then(|target|state.phases.get(&target.id));
            if let Some(attempt)=attempt {crate::native_enrollment::caller::measure_record(attempt,&mut proof).map_err(|_|Error::UnknownStoreOutcome)?;}
            if let Some(phase)=phase {crate::native_enrollment::caller::measure_phase_record(phase,&mut proof).map_err(|_|Error::UnknownStoreOutcome)?;}
            authority.verify_current(run,attempt,phase,owner,self.clock.now(),&mut proof)?;
            if let Some(attempt)=attempt {super::super::native_parent::verify_public_source(state,run,attempt,phase,&mut proof).map_err(|_|Error::StaleOwner)?;}

            let session=session.clone();
            let run_id=run_id.clone();
            let current=revision(state,&session);
            if current!=mutation.expected_revision {return Err(Error::RevisionConflict);}
            let next=current.next().map_err(|_|Error::RevisionExhausted)?;
            let empty=crabber_core::PublicStateContents::default();
            let contents=state.public_contents.get(&session).unwrap_or(&empty);
            proof.measure(contents).map_err(|_|Error::InvalidRequest)?;
            proof.measure(&mutation.change).map_err(|_|Error::InvalidRequest)?;
            let changed=mutation.change.apply(contents).map_err(|_|Error::InvalidRequest)?;
            proof.measure(&changed).map_err(|_|Error::InvalidRequest)?;
            proof.measure(&source).map_err(|_|Error::InvalidRequest)?;
            let (mut event,mut digest)=public_state::source_event(source.clone(),&mutation,next,changed.clone(),self.clock.now(),&mut proof)?;
            if matches!(&mutation.change,PublicSourceChange::StateDelta{..}|PublicSourceChange::ActivityDelta{..}) {
                proof.measure(contents).map_err(|_|Error::InvalidRequest)?;
                let contents=contents.clone();
                proof.measure(contents.state()).map_err(|_|Error::InvalidRequest)?;
                let snapshot=PublicSourceMutation{id:mutation.id.clone(),expected_revision:current,
                    change:PublicSourceChange::StateSnapshot{state:contents.state().clone()}};
                proof.measure(&contents).map_err(|_|Error::InvalidRequest)?;
                let (before,_)=public_state::source_event(source,&snapshot,current,contents.clone(),self.clock.now(),&mut proof)?;
                let cursor=append(state,before)?;
                digest=public_state::attach_before(&mut event,contents,cursor,&mut proof)?;
            }
            let cursor=append(state,event)?;
            state.public_contents.insert(session.clone(),changed);
            super::bump_public_revision(state,&session).map_err(public_state::unknown)?;
            let receipt=PublicStateReceipt{session_id:session.clone(),run_id:run_id.clone(),operation_id:mutation.id,
                semantic_digest:digest,revision:next,event_cursor:cursor};
            proof.measure(&receipt).map_err(|_|Error::UnknownStoreOutcome)?;
            #[cfg(test)]
            {
                self.pending_initial_test_state.public_source_usage.lock().unwrap().push(proof.used());
                if self.public_state_fault.load(std::sync::atomic::Ordering::SeqCst)==1 {return Err(Error::UnknownStoreOutcome);}
            }
            state.operation_receipts.insert(key,OperationReceipt::PublicState(receipt.clone()));
            Ok(receipt)
        });
        #[cfg(test)]
        if self.public_state_fault.load(std::sync::atomic::Ordering::SeqCst)==2 && result.is_ok() {return Err(Error::UnknownStoreOutcome);}
        result
    }
}

fn append(state:&mut super::State,mut event:crabber_core::EventRecord)->Result<crabber_core::EventCursor,Error> {
    let cursor=state.events.next_cursor().map_err(|_|Error::UnknownStoreOutcome)?;
    event.cursor=Some(cursor);
    state.events.append(event).map_err(|_|Error::UnknownStoreOutcome)?;
    Ok(cursor)
}

impl MemoryStore {
    pub(in crate::memory) fn read_public_source_publication(&self,session:&crabber_core::SessionId,id:&crabber_core::ControlId)->Result<Option<crate::PublicSourcePublication>,Error> {
        public_state::validate_id(id)?;
        let state=self.state.lock().unwrap();
        let Some(saved)=state.operation_receipts.get(&(session.clone(),id.clone())) else {return Ok(None);};
        let OperationReceipt::PublicState(receipt)=saved else {return Err(Error::SemanticConflict);};
        let mut proof=Proof::bounded(32<<20);
        let event=state.events.get(receipt.event_cursor).ok_or(Error::UnknownStoreOutcome)?;
        if event.payload.get("version").and_then(serde_json::Value::as_u64)!=Some(2) {return Err(Error::SemanticConflict);}
        public_state::verify_source_event_with_proof(receipt,event,&mut proof)?;
        proof.charge(2*std::mem::size_of::<crabber_core::EventRecord>()).map_err(|_|Error::UnknownStoreOutcome)?;
        let mut events=Vec::with_capacity(2);
        if let Some(cursor)=public_state::before_event(event)? {
            let before=state.events.get(cursor).ok_or(Error::UnknownStoreOutcome)?;
            public_state::verify_before_event_with_proof(event,before,&mut proof)?;
            proof.measure(before).map_err(|_|Error::UnknownStoreOutcome)?;
            events.push(before.clone());
        }
        proof.measure(event).and_then(|()|proof.measure(receipt)).map_err(|_|Error::UnknownStoreOutcome)?;
        events.push(event.clone());
        Ok(Some(crate::PublicSourcePublication{receipt:receipt.clone(),events}))
    }
}
