//! Metadata remains available after host ingress closes.
use super::Agent;
use crabber_core::SessionId;
use crabber_session::session_owner::{
    SessionOwnerError,
    preparation::{PreparationKey, PreparationReconciliation},
};
impl Agent {
    /// Read original preparation correlation without factories or authority.
    /// Receiving this descriptor grants no Join or cleanup right.
    /// # Errors
    /// Returns unsupported or immutable source-integrity failures.
    pub async fn lookup_preparation_operation(
        &self,
        session: &SessionId,
        key: &PreparationKey,
    ) -> Result<Option<PreparationReconciliation>, SessionOwnerError> {
        self.runtime
            .lookup_preparation_operation(session, key)
            .await
    }
}
