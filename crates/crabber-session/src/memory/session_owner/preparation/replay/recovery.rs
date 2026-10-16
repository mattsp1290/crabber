//! Exact successor issuer plus original sources, never current state or a grant.
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    AdmissionRecoveryOutcome, JoinedPreparation, PreparationCommitError, PreparationCompletion,
    PreparedOperation,
    recovery::replay::{self, NativeSources},
};
impl crate::memory::MemoryStore {
    pub(super) fn replay_recovery(
        state: &crate::memory::State,
        joined: &JoinedPreparation,
    ) -> Result<Option<PreparationCompletion>, PreparationCommitError> {
        let PreparedOperation::AdmissionRecovery(recovery) = joined.observed_operation() else {
            return Ok(None);
        };
        let source = &recovery.source;
        let request = source.request();
        let key = |sequence| {
            (
                request.session_id.clone(),
                request.expected_fence.run_id.clone(),
                sequence,
            )
        };
        let Some(next) = state
            .native_run_issuances
            .get(&key(source.expected_issuance() + 1))
        else {
            return Ok(None);
        };
        let previous = state
            .native_run_issuances
            .get(&key(source.expected_issuance()))
            .ok_or(Error::UnknownStoreOutcome)?;
        let initial = state
            .native_run_issuances
            .get(&key(1))
            .ok_or(Error::UnknownStoreOutcome)?;
        let init = state
            .admission_execution_sources
            .get(&(
                request.expected_fence.run_id.clone(),
                crate::admission_journal::Kind::Init,
            ))
            .ok_or(Error::UnknownStoreOutcome)?;
        let receipt = state
            .receipts
            .get(&(request.session_id.clone(), request.key.clone()))
            .ok_or(Error::UnknownStoreOutcome)?;
        for issuer in [previous, next, initial] {
            bounded(issuer, 2048)?;
        }
        bounded(init, 8192)?;
        bounded(receipt, 8192)?;
        let crate::admission_journal::Source::Init(init) = init else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        let mut audits = Vec::with_capacity(5);
        for id in replay::audit_ids(joined.observed_grant())? {
            let audit = state
                .session_owners
                .source_audit(&request.session_id, &id)
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(audit, 32768)?;
            audits.push(audit.clone());
        }
        let receipt = replay::verify(
            joined.observed_grant(),
            recovery,
            &audits,
            &NativeSources {
                previous,
                next,
                initial,
                init,
                receipt,
            },
        )?;
        Ok(Some(PreparationCompletion::AdmissionRecovery(
            AdmissionRecoveryOutcome::Replayed(receipt),
        )))
    }
}
fn bounded(value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
