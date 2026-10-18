//! Original Queue metadata association under one borrowed State lock.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{JoinedPreparation, PreparationCommitError, inspection, queue_admission},
};
use crate::{QueueReceipt, memory::MemoryStore};
impl MemoryStore {
    pub(in crate::memory) fn inspect_queue_admission(
        &self,
        original: &JoinedPreparation,
    ) -> Result<Option<QueueReceipt>, PreparationCommitError> {
        self.inspect_queue_admission_with_proof(
            original,
            &mut Proof::bounded(inspection::MAX_PROOF),
        )
    }
    pub(in crate::memory) fn inspect_queue_admission_with_proof(
        &self,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Option<QueueReceipt>, PreparationCommitError> {
        let (request, _) = queue_admission::original(original, proof)?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state, &request.session_id)
            .map_err(crate::deletion::owner_error)?;
        let Some(prefix) = crate::memory::queue::protocol::admission_prefix(
            &state,
            &request.session_id,
            &request.id,
            proof,
        )
        .map_err(|error| match error {
            crate::public_session::v2::proof::ReadError::Budget(
                crate::public_session::v2::proof::Failure::Limited(_),
            ) => Error::Capacity,
            crate::public_session::v2::proof::ReadError::Domain(
                crate::StoreError::HostUnsupported,
            ) => Error::Unsupported,
            _ => Error::UnknownStoreOutcome,
        })?
        else {
            let join = original.observed_grant().reconciliation()?.join_id;
            proof
                .measure(&(&request.session_id, &join))
                .map_err(|_| Error::Capacity)?;
            return if state
                .session_owners
                .source_audit(&request.session_id, &join)
                .is_some()
                || state
                    .preparation_admission_joins
                    .contains_key(&(request.session_id.clone(), join))
            {
                Err(Error::UnknownStoreOutcome.into())
            } else {
                Ok(None)
            };
        };
        let (enqueue, prepare, admit) = queue_admission::select(prefix)?;
        let actual = queue_admission::prepared(original, enqueue, prepare, admit)?;
        let receipt = &admit.receipt;
        let run = &receipt
            .summary
            .admission
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .run_id;
        let (session, initial, witness, sources) =
            super::super::drivers::sources_read(&state, run, proof)
                .map_err(|error| inspection::read_error(&error))?;
        let protected = crate::memory::native_parent::source_protocol::original(
            &state,
            &request.session_id,
            run,
            1,
            proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
        if protected != initial
            || !state
                .native_run_protocols
                .contains(&(request.session_id.clone(), run.clone()))
        {
            return Err(Error::Unsupported.into());
        }
        let init = super::super::drivers::initial_journal_read(&state, run, initial, proof)
            .map_err(|error| inspection::read_error(&error))?;
        queue_admission::verify(
            original,
            actual,
            receipt,
            &queue_admission::AdmissionSources {
                session,
                initial,
                witness,
                audits: sources,
                init,
            },
        )?;
        Ok(Some(receipt.clone()))
    }
}

#[cfg(test)]
#[path = "queue_inspection_tests.rs"]
mod tests;
