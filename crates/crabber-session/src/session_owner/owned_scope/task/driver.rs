//! Opaque provenance delivered only through the actual fresh Driver task gate.
use super::OwnedScopeActivation;
use crabber_core::SessionId;

/// The activation delivered through an initial Driver's original task gate.
/// Only the activation matched by the actual fresh permit is retained here.
/// Canonical Run and source identities require independent native source reads;
/// metadata supplied with the registration grant is not copied into this context.
/// This is neither execution authority nor evidence that work has joined.
/// It cannot be recreated from a fence, activation or metadata receipt.
/// A returned or escaped context remains provenance only: its existence does
/// not prove the main is still running, even before native Join acknowledges.
/// Any future native consumer must independently close/check actual main ingress
/// and serialize native enrollment closure before granting fresh work.
///
/// ```compile_fail
/// use crabber_session::session_owner::owned_scope::DriverMainContext;
/// fn duplicate(context: DriverMainContext) { let _ = context.clone(); }
/// ```
/// ```compile_fail
/// use crabber_session::session_owner::owned_scope::DriverMainContext;
/// fn serialize(context: DriverMainContext) { let _ = serde_json::to_string(&context); }
/// ```
/// ```compile_fail
/// use crabber_session::session_owner::owned_scope::DriverMainContext;
/// let _: DriverMainContext = serde_json::from_str("{}").unwrap();
/// ```
/// ```compile_fail
/// use crabber_session::session_owner::owned_scope::DriverMainContext;
/// let _ = DriverMainContext {};
/// ```
pub struct DriverMainContext {
    activation: OwnedScopeActivation,
}
impl DriverMainContext {
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        self.activation.session_id()
    }
    pub(super) fn dispatched(activation: OwnedScopeActivation) -> Self {
        Self { activation }
    }
}
