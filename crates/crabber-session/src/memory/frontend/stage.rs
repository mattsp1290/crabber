use super::{
    Error, FrontendCallStaging, FrontendOriginRecord, FrontendStagingReceipt, MemoryExecution,
    OperationReceipt, evidence, receipt,
};
use crate::{AttemptAuthority, AttemptCleanup, AttemptState};
impl MemoryExecution {
    pub(in super::super) fn stage_frontend(
        &self,
        request: &FrontendCallStaging,
        authority: &AttemptAuthority,
    ) -> Result<FrontendStagingReceipt, Error> {
        if request.run_id != self.fence.run_id {
            return Err(Error::NotFound);
        }
        let digest = request.fingerprint()?;
        let fault = super::super::tool_continuation::fault(&self.store);
        let result = self.store.transact(|state| {
            super::super::credential_access::check_origin_bounded(
                state,
                &request.session_id,
                &request.run_id,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
            if let Some(saved) = receipt(state, &request.session_id, &request.id)? {
                if saved.run_id != request.run_id || saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok((saved, false));
            }
            let run = state
                .runs
                .get(&request.run_id)
                .filter(|run| run.session_id == request.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if state
                .public_revisions
                .get(&run.session_id)
                .copied()
                .unwrap_or(crabber_core::SessionRevision::INITIAL)
                != request.expected_revision
            {
                return Err(Error::RevisionConflict);
            }
            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(|_| Error::StaleOwner)?;
            let parent = state
                .attempts
                .values()
                .filter(|parent| parent.snapshot.run_id == run.id)
                .max_by_key(|parent| parent.snapshot.target.generation)
                .ok_or(Error::NotFound)?;
            parent.validate().map_err(|_| Error::UnknownStoreOutcome)?;
            if parent.snapshot.target != request.parent
                || parent.snapshot.session_id != run.session_id
                || parent.snapshot.state != AttemptState::Started
                || parent.snapshot.cleanup != AttemptCleanup::Pending
                || parent.run_token != run.claim_token
                || parent.lease_until <= now
                || !authority.matches_store(&parent.snapshot, &parent.token, &parent.run_token)
            {
                return Err(Error::StaleOwner);
            }
            if state.calls.contains_key(&request.call.id)
                || state.imported_call_ids.contains(&request.call.id)
                || state.frontend_origins.contains_key(&request.call.id)
                || state
                    .phases
                    .values()
                    .any(|phase| phase.snapshot.target.call_id == request.call.id)
            {
                return Err(Error::SemanticConflict);
            }
            super::super::native_parent::verify_attempt(state, &run, parent)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            evidence::binding(state, &run, request)?;
            let cursor = state
                .events
                .next_cursor()
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let mut event = request.materialize()?;
            event.cursor = Some(cursor);
            state
                .events
                .append(event)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            super::super::bump_public_revision(state, &run.session_id)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let saved = crate::frontend_continuation::staging_receipt(request, cursor)?;
            state
                .calls
                .insert(request.call.id.clone(), request.call.clone());
            state
                .call_order
                .push(run.session_id.clone(), request.call.id.clone());
            state.frontend_origins.insert(
                request.call.id.clone(),
                FrontendOriginRecord {
                    request: request.clone(),
                    receipt: saved.clone(),
                },
            );
            state.operation_receipts.insert(
                (run.session_id.clone(), request.id.clone()),
                OperationReceipt::FrontendStaging(saved.clone()),
            );
            *state.snapshot_revisions.entry(run.session_id).or_default() += 1;
            super::super::tool_continuation::before(fault)?;
            Ok((saved, true))
        });
        super::super::tool_continuation::after(result, fault)
    }
}
