//! Exact owned unkeyed replay carries metadata, never a replacement Run fence.
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation, admission::SourceAudits,
};
impl crate::memory::MemoryStore {
    pub(super) fn replay_admission(
        state: &crate::memory::State,
        joined: &JoinedPreparation,
    ) -> Result<Option<PreparationCompletion>, Error> {
        if !matches!(joined.observed_operation(), PreparedOperation::Admit(_)) {
            return Ok(None);
        }
        let grant = joined.observed_grant();
        let metadata = grant.reconciliation()?;
        let key = (metadata.session_id.clone(), metadata.join_id);
        let Some(run) = state.preparation_admission_joins.get(&key) else {
            return Ok(None);
        };
        let witness = state
            .preparation_admissions
            .get(run)
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(witness, 8192)?;
        witness.validate()?;
        if witness.run_id() != run || witness.session_id() != &key.0 {
            return Err(Error::UnknownStoreOutcome);
        }
        let session = state
            .sessions
            .get(&key.0)
            .ok_or(Error::UnknownStoreOutcome)?;
        let initial = state
            .native_run_issuances
            .get(&(key.0.clone(), run.clone(), 1))
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(session, 16384)?;
        bounded(initial, 2048)?;
        let ids = witness.audit_ids()?;
        let mut audits = Vec::with_capacity(5);
        for id in &ids {
            let audit = state
                .session_owners
                .source_audit(&key.0, id)
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(audit, 32768)?;
            audits.push(audit);
        }
        let sources = SourceAudits {
            reserve: audits[0],
            begin: audits[1],
            join: audits[2],
            child_reserve: audits[3],
            child_begin: audits[4],
        };
        witness
            .replay(grant, session, initial, &sources)
            .map(PreparationCompletion::AdmissionReplayed)
            .map(Some)
    }
}
fn bounded(value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
