//! The fixture cannot consume native private task or transport authority.
impl crate::memory::MemoryStore {
    pub(crate) fn classify_initial_pending(
        _state: &impl Sized,
        _run: &crabber::core::Run,
        _assignment: &crate::native_enrollment::pending_initial::Assignment,
        _proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        crate::native_enrollment::initial_consumer::InitialConsumption,
        crate::public_session::v2::proof::Failure,
    > {
        Err(crate::public_session::v2::proof::unknown(
            "native historical Initial source unavailable in fixture",
        ))
    }
    // No native factory exists in this embedded fixture. A generic negative
    // result avoids exposing Memory's private State or constructing a package.
    pub(crate) fn fixed_closed_initial_source<T>(
        _state: &impl Sized,
        _run: &crabber::core::Run,
        _proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<T>, crate::public_session::v2::proof::Failure> {
        Err(crate::public_session::v2::proof::unknown(
            "native fixed source unavailable in fixture",
        ))
    }
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

    #[allow(clippy::unused_self)]
    pub(crate) fn apply_pending_initial_consumer(
        &self,
        _call: crate::session_owner::owned_scope::PendingInitialConsumerNativeCall<'_>,
    ) -> Result<
        crate::session_owner::owned_scope::PendingInitialConsumerNativeReply,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Err(crate::session_owner::SessionOwnerError::Unsupported.into())
    }
}
