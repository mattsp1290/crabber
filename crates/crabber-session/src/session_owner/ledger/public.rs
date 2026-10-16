//! Coherent bounded public owner projection with charged source evidence.
use super::{Ledger, SessionId, SessionOwnerState};
impl Ledger {
    /// Authenticate independent membership before excluding Joined owners. The
    /// scan borrows historical entries and never builds an unbounded identity map.
    pub(crate) fn public_registered(
        &self,
        session: &SessionId,
        limit: u16,
        safe: &mut crate::public_session::budget::Budget,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Vec<crate::PublicRegisteredOwner>, crate::public_session::v2::proof::Failure> {
        self.checked_owner_history(session, limit, safe, proof, |_, _| Ok(false))
            .map(|owners| owners.live)
    }

    pub(crate) fn checked_owner_history(
        &self,
        session: &SessionId,
        limit: u16,
        safe: &mut crate::public_session::budget::Budget,
        proof: &mut crate::public_session::v2::proof::Proof,
        mut classified: impl FnMut(
            &crate::session_owner::record::Record,
            &mut crate::public_session::v2::proof::Proof,
        ) -> Result<bool, crate::public_session::v2::proof::Failure>,
    ) -> Result<
        crate::session_owner::coverage::CheckedOwners,
        crate::public_session::v2::proof::Failure,
    > {
        use crate::public_session::v2::proof::{Failure, unknown};
        self.validate_publications(session, proof)?;
        self.validate_owner_membership(session).map_err(unknown)?;
        let mut result = Vec::new();
        let mut unclassified = false;
        for ((scope, id), record) in &self.records {
            if scope != session {
                continue;
            }
            let checked = self.checked_for_read(session, id, record, proof)?;
            if checked.snapshot.state == SessionOwnerState::Joined {
                unclassified |= !classified(checked, proof)?;
                continue;
            }
            // The sentinel is authenticated too. Invalid source evidence must
            // never be converted into a count-limit response.
            if result.len() >= usize::from(limit) {
                return Err(Failure::Limited(
                    crate::PublicSessionLimitV2::RegisteredOwners,
                ));
            }
            result.push(crate::public_session::v2::proof::owner(
                &checked.snapshot,
                safe,
            )?);
        }
        Ok(crate::session_owner::coverage::CheckedOwners {
            live: result,
            unclassified,
        })
    }
    fn checked_for_read(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerId,
        record: &crate::session_owner::record::Record,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<&crate::session_owner::record::Record, crate::public_session::v2::proof::Failure>
    {
        use crate::public_session::v2::proof::unknown;
        // Charge each borrowed carrier before its decoder/validator can
        // allocate. These are the very sources used by checked(), not a
        // cached status or a claimed latest pointer.
        proof.measure(record)?;
        let latest = self
            .audits
            .values()
            .filter(|audit| {
                audit.receipt.session_id == *session && audit.receipt.owner.target.id == *id
            })
            .max_by_key(|audit| audit.record.sequence)
            .ok_or_else(|| unknown("owner latest"))?;
        proof.measure(latest)?;
        let origin = self
            .audits
            .get(&(session.clone(), record.origin.clone()))
            .ok_or_else(|| unknown("owner origin"))?;
        proof.measure(origin)?;
        let context = self
            .contexts
            .get(session)
            .ok_or_else(|| unknown("owner context"))?;
        proof.measure(context)?;
        let context_origin = self
            .audits
            .get(&(session.clone(), context.origin.clone()))
            .ok_or_else(|| unknown("owner context origin"))?;
        proof.measure(context_origin)?;
        if let Some(cancel) = &record.cancellation {
            let request = self
                .audits
                .get(&(session.clone(), cancel.request.id.clone()))
                .ok_or_else(|| unknown("owner cancellation source"))?;
            proof.measure(request)?;
            if let Some(observed) = &cancel.observed {
                let audit = self
                    .audits
                    .get(&(session.clone(), observed.request.id.clone()))
                    .ok_or_else(|| unknown("owner observation source"))?;
                proof.measure(audit)?;
            }
        }
        self.measure_owned_sources(record, proof)?;
        self.checked(session, id).map_err(unknown)
    }
}
