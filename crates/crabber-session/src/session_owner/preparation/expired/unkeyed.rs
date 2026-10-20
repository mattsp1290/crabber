//! Explicit original unkeyed input proof, independent of current Run state.
use super::Error;
use crate::{
    native_parent::RunIssuance,
    session_owner::{
        audit::Audit,
        preparation::admission::{SourceAudits, Witness},
    },
};
use crabber_core::{InputFingerprint, Session};
pub(crate) struct Original {
    pub session: Session,
    pub witness: Witness,
    pub initial: RunIssuance,
    pub audits: [Audit; 5],
}
impl Original {
    pub fn digest(&self) -> Result<InputFingerprint, Error> {
        for valid in [
            crate::snapshot::encoded_bytes(&self.session, 16384).is_some(),
            crate::snapshot::encoded_bytes(&self.witness, 8192).is_some(),
            crate::snapshot::encoded_bytes(&self.initial, 2048).is_some(),
        ] {
            if !valid {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        for audit in &self.audits {
            if crate::snapshot::encoded_bytes(audit, 32768).is_none() {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        let input = self.witness.unkeyed_binding_digest()?;
        self.witness.verify_sources(
            &self.session,
            &self.initial,
            &SourceAudits::from_array(&self.audits),
        )?;
        crate::session_owner::digest(&(
            "crabber.preparation.expired-unkeyed.v1",
            self.witness.fingerprint()?,
            crate::native_parent::issuance_digest(&self.initial)
                .map_err(|_| Error::UnknownStoreOutcome)?,
            input,
        ))
    }
    pub fn origin(&self) -> Result<super::ExpiredRunOrigin, Error> {
        Ok(super::ExpiredRunOrigin::UnkeyedAdmission(
            self.witness.join_id()?,
        ))
    }
    pub fn verify_binding(&self, binding: &crate::ExecutionBindingCapsule) -> Result<(), Error> {
        self.digest()?;
        self.witness.verify_unkeyed_binding(binding)?;
        Ok(())
    }
}
