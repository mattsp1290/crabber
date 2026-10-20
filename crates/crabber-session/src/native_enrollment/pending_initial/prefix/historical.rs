//! Historical semantic corroboration over independently selected native originals.
//! Full original carrier commitments are authenticated by the caller's protected
//! birth/cohort selection, never recomputed from a fabricated opaque carrier.
#[path = "historical/intent.rs"]
mod intent;
use super::{Correlation, SourceKey};
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crate::session_owner::{
    SessionOwnerOperation,
    audit::Audit,
    owned_scope,
    preparation::{
        PreparationPurpose,
        admission::{SourceAudits, Witness},
    },
};
use crate::{AdmissionExecutionRecord, RetainedExecutionBinding, admission_journal::Init};
use crabber_core::Session;

/// A borrowed historical package, not permission or a physical join. Backends
/// authenticate the actual native publications/cohorts, original binding/input
/// and protected keyed journal before calling this portable verifier. SQL also
/// requires protected v3 owner publications; Memory verifies its actual Ledger.
pub(crate) struct HistoricalInitialSources<'a> {
    pub(crate) session: &'a Session,
    pub(crate) initial: &'a RunIssuance,
    pub(crate) witness: &'a Witness,
    pub(crate) audits: SourceAudits<'a>,
    pub(crate) activated: &'a Audit,
    pub(crate) binding: &'a RetainedExecutionBinding,
    pub(crate) keyed: Option<(&'a AdmissionExecutionRecord, &'a Init)>,
}
impl HistoricalInitialSources<'_> {
    pub(crate) fn verify(&self, expected: &Correlation, proof: &mut Proof) -> Result<(), Failure> {
        expected.validate(proof)?;
        proof.measure(&(
            self.session,
            self.initial,
            self.witness,
            [
                self.audits.reserve,
                self.audits.begin,
                self.audits.join,
                self.audits.child_reserve,
                self.audits.child_begin,
                self.activated,
            ],
            self.binding,
            self.keyed,
        ))?;
        self.witness
            .verify_sources(self.session, self.initial, &self.audits)
            .map_err(unknown)?;
        for audit in [
            self.audits.reserve,
            self.audits.begin,
            self.audits.join,
            self.audits.child_reserve,
            self.audits.child_begin,
            self.activated,
        ] {
            audit.validate().map_err(unknown)?;
            if !matches!(audit.version, 2 | 3)
                || audit.publication.is_none()
                || audit.receipt.session_id != self.session.id
            {
                return Err(unknown("historical Initial publication/scope"));
            }
        }
        if expected.key.session_id != self.session.id
            || self.binding.session_id != self.session.id
            || self.binding.run_id != self.initial.state.run_id
            || self.witness.run_id() != &self.initial.state.run_id
        {
            return Err(unknown("historical Initial assignment/binding scope"));
        }
        let registration = self.registration()?;
        let key = SourceKey::from_original_identities(
            &self.session.id,
            &self.audits.reserve.receipt.operation_id,
            &self.audits.begin.receipt.operation_id,
            &self.audits.begin.record.snapshot.target,
            registration,
            proof,
        )?;
        if key != expected.key {
            return Err(unknown("historical Initial original source key"));
        }
        intent::verify(self, proof)
    }

    fn registration(&self) -> Result<&owned_scope::OwnedScopeActivation, Failure> {
        let registration = self
            .activated
            .record
            .owned_scope
            .as_ref()
            .ok_or_else(|| unknown("missing original child activation"))?;
        if self.activated.receipt.operation != SessionOwnerOperation::ActivateScope
            || self.activated.receipt.operation_id != registration.id
            || self.activated.receipt.semantic_digest
                != registration.fingerprint().map_err(unknown)?
            || self.activated.record.token != self.audits.child_begin.record.token
        {
            return Err(unknown("historical original child activation"));
        }
        owned_scope::source::current(
            &self.activated.record,
            self.audits.child_reserve,
            self.audits.child_begin,
            Some(self.activated),
        )
        .map_err(unknown)?;
        Ok(registration)
    }
}

#[cfg(test)]
#[path = "historical/tests.rs"]
mod tests;
