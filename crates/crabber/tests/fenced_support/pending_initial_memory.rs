//! The fixture cannot consume native private task or transport authority.
impl crate::memory::MemoryStore {
    // Match the real backend's private receiver-shaped ingress without touching
    // the opaque call or pretending to authenticate a native producer.
    #[allow(clippy::unused_self)]
    pub(crate) fn apply_pending_initial(
        &self,
        _call: crate::session_owner::owned_scope::PendingInitialNativeCall<'_>,
    ) -> Result<
        crate::session_owner::owned_scope::PendingInitialNativeReply,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported.into())
    }
}
