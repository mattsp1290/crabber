//! Close-only native transition, never a public `SessionOwnerAuthority`.
use super::{Error, MutateSessionOwner, OffsetDateTime, Record, SessionOwnerTermination};
use crate::native_enrollment::initial_consumer::VerifiedUnactivatedRetirement;
impl Record {
    pub(crate) fn retire_unactivated_initial(
        &mut self,
        witness: VerifiedUnactivatedRetirement<'_>,
        now: OffsetDateTime,
    ) -> Result<MutateSessionOwner, Error> {
        let request = witness.into_request(self)?;
        self.join(SessionOwnerTermination::Cancelled);
        self.publish_change(now, true)?;
        Ok(request)
    }
}
