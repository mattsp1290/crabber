//! Exact original generic claim reconciliation, independent of current Run.
use crate::memory::{MemoryStore, OperationReceipt, State};
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunOutcome, JoinedPreparation, PreparationCommitError,
    PreparationCompletion, PreparedOperation,
    expired::{history, replay::Native},
};
impl MemoryStore {
    pub(in crate::memory) fn replay_expired(
        state: &State,
        joined: &JoinedPreparation,
    ) -> Result<Option<PreparationCompletion>, PreparationCommitError> {
        let PreparedOperation::ExpiredRunResume(prepared) = joined.observed_operation() else {
            return Ok(None);
        };
        let source = &prepared.source;
        if let Some(receipt) = Self::lookup_expired_metadata(
            state,
            &source.request().session_id,
            &source.request().expected_fence.run_id,
            source.expected_issuance(),
        )? {
            super::super::verify_replay(state, joined.observed_grant())?;
            return Ok(Some(PreparationCompletion::ExpiredRun(
                ExpiredRunOutcome::Replayed(receipt),
            )));
        }
        Ok(None)
    }
    pub(in crate::memory) fn lookup_expired_source(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        issuance: u64,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        history::identities(session, run, issuance)?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        crate::memory::deletion::ensure_live(&state, session)
            .map_err(crate::deletion::owner_error)?;
        Self::lookup_expired_metadata(&state, session, run, issuance)
    }
    fn lookup_expired_metadata(
        state: &State,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        issuance: u64,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        let ids = history::identities(session, run, issuance)?;
        let key = |seq| (session.clone(), run.clone(), seq);
        let Some(next) = state.native_run_issuances.get(&key(issuance + 1)) else {
            return Ok(None);
        };
        if next.completion_source.as_ref().is_none_or(|source| {
            source.native_operation != crate::native_parent::CompletionOperation::ExpiredRunResume
                || source.root_join_id != ids.join
        }) {
            return Ok(None);
        }
        let previous = state
            .native_run_issuances
            .get(&key(issuance))
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(previous, 2048)?;
        bounded(next, 2048)?;
        let mut audits = Vec::with_capacity(5);
        for id in [&ids.reserve, &ids.begin, &ids.join] {
            let audit = state
                .session_owners
                .source_audit(session, id)
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(audit, 32768)?;
            audits.push(audit.clone());
        }
        for id in history::audit_ids(&audits[0], &audits[1], &audits[2])? {
            let audit = state
                .session_owners
                .source_audit(session, &id)
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(audit, 32768)?;
            audits.push(audit.clone());
        }
        let target = history::target(&audits)?;
        let native = match target.origin {
            ExpiredRunOrigin::Suspended(ref id) => {
                let Some(OperationReceipt::Attempt(receipt)) = state
                    .operation_receipts
                    .get(&(session.clone().clone(), id.clone()))
                else {
                    return Err(Error::UnknownStoreOutcome);
                };
                let event = state
                    .events
                    .get(receipt.event_cursor)
                    .ok_or(Error::UnknownStoreOutcome)?;
                bounded(receipt, 8192)?;
                bounded(event, 65536)?;
                Native::Suspended(receipt, event)
            }
            ExpiredRunOrigin::KeyedInitial(ref admission_key) => {
                use crate::admission_journal::{Kind, Source};
                if !state.admission_execution_protocols.contains(&run.clone()) {
                    return Err(Error::UnknownStoreOutcome);
                }
                let get = |kind| {
                    state
                        .admission_execution_sources
                        .get(&(run.clone().clone(), kind))
                        .ok_or(Error::UnknownStoreOutcome)
                };
                let initial_source = get(Kind::Init)?;
                let begin_source = get(Kind::Begin)?;
                bounded(initial_source, 8192)?;
                bounded(begin_source, 8192)?;
                let (Source::Init(init), Source::Begin(begin)) = (initial_source, begin_source)
                else {
                    return Err(Error::UnknownStoreOutcome);
                };
                let receipt = state
                    .receipts
                    .get(&(session.clone().clone(), admission_key.clone()))
                    .ok_or(Error::UnknownStoreOutcome)?;
                let initial = state
                    .native_run_issuances
                    .get(&key(1))
                    .ok_or(Error::UnknownStoreOutcome)?;
                let activated_issuer = state
                    .native_run_issuances
                    .get(&key(begin.issuance.sequence))
                    .ok_or(Error::UnknownStoreOutcome)?;
                bounded(receipt, 8192)?;
                bounded(initial, 2048)?;
                bounded(activated_issuer, 2048)?;
                Native::Initial {
                    init,
                    begin,
                    receipt,
                    initial,
                    begun: activated_issuer,
                }
            }
        };
        history::verify(previous, next, &audits, native)
    }
}
fn bounded(value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
