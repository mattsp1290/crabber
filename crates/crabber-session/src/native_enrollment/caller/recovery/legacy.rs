//! Actual compatibility claim, without a fabricated prepared source or event.
use super::{RecoveryPublication, publication, replacement};
use crate::native_enrollment::caller::verify::Scope;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{Run, RunFence, RunId};

pub(crate) struct LegacyExpired<'a> {
    pub id: &'a RunId,
    pub owner: &'a str,
    pub previous: &'a Run,
    pub updated: &'a Run,
    pub fence: &'a RunFence,
    pub publication: &'a RecoveryPublication,
}
impl<'a> LegacyExpired<'a> {
    pub(in super::super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        proof.measure(&(self.id, self.owner, self.previous, self.updated, self.fence))?;
        if self.id != &self.previous.id
            || self.previous.status.is_terminal()
            || self.previous.lease_until > self.updated.updated_at
        {
            return Err(unknown("actual compatibility expired claim"));
        }
        replacement(
            self.previous,
            self.updated,
            self.fence,
            self.owner,
            std::time::Duration::from_secs(30),
            proof,
        )?;
        publication(self.publication, self.updated, None, proof)?;
        Ok(Scope {
            session: &self.previous.session_id,
            run: &self.previous.id,
        })
    }
}
