//! Child reserve and Begin share the root's mutation gate, before factory work.
use super::{Error, MemoryStore, before, bump_public_revision, fault, revision};
use crate::session_owner::preparation::{PreparationGrant, PreparationResourceOutcome};
impl MemoryStore {
    pub(in crate::memory) fn retain_preparation_resources(
        &self,
        grant: &PreparationGrant,
    ) -> Result<PreparationResourceOutcome, Error> {
        let session = &grant.original_reservation().owner.binding.session_id;
        let outcome = self.transact(|state| {
            if let Some(receipt) = state.session_owners.preparation_resource_receipt(grant)? {
                return Ok(PreparationResourceOutcome::Replayed(Box::new(receipt)));
            }
            super::validate_preparation_source(
                state,
                grant.original_reservation(),
                self.clock.now(),
            )?;
            let before_revision = revision(state, session);
            let outcome = state.session_owners.retain_preparation_resources(
                grant,
                before_revision,
                self.clock.now(),
            )?;
            if matches!(outcome, PreparationResourceOutcome::Applied { .. }) {
                if state.sessions.contains_key(session) {
                    for _ in 0..2 {
                        bump_public_revision(state, session)
                            .map_err(|_| Error::UnknownStoreOutcome)?;
                    }
                }
                #[cfg(test)]
                crate::session_owner_contract::process::pause("resources-before");
                before(fault(self))?;
            }
            Ok(outcome)
        })?;
        if matches!(outcome, PreparationResourceOutcome::Applied { .. }) {
            #[cfg(test)]
            crate::session_owner_contract::process::pause("resources-after");
            if fault(self) == 2 {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        Ok(outcome)
    }
}
