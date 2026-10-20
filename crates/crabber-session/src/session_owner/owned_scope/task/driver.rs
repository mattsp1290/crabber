//! Opaque provenance delivered only through the actual fresh Driver task gate.
use super::OwnedScopeActivation;
use crabber_core::SessionId;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// The activation delivered through an initial Driver's original task gate.
/// Only the activation matched by the actual fresh permit is retained here.
/// Canonical Run and source identities require independent native source reads;
/// metadata supplied with the registration grant is not copied into this context.
/// This is neither execution authority nor evidence that work has joined.
/// It cannot be recreated from a fence, activation or metadata receipt.
/// A returned or escaped context remains provenance only: its existence does
/// not prove the main is still running, even before native Join acknowledges.
/// Local ingress closes with the actual task, independently of returned context.
/// Any future native consumer must retain/join actual in-flight calls and
/// serialize native enrollment closure before granting fresh work.
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
    ingress: Arc<MainIngress>,
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
    /// Local admission only. Native I/O still needs independently retained call
    /// ownership and native source/enrollment validation under the caller Proof.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "crate-private ingress awaits a native call consumer"
        )
    )]
    pub(crate) fn try_enter_main(&self) -> Result<DriverMainEntry<'_>, DriverMainClosed> {
        if self.ingress.is_closed() {
            Err(DriverMainClosed)
        } else {
            Ok(DriverMainEntry { _context: self })
        }
    }
    pub(super) fn dispatched(activation: OwnedScopeActivation, ingress: Arc<MainIngress>) -> Self {
        Self {
            activation,
            ingress,
        }
    }
}

/// A local association, never native permission or physical completion proof.
/// Holding an entry does not keep the actual main alive or delay its close.
pub(crate) struct DriverMainEntry<'a> {
    _context: &'a DriverMainContext,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DriverMainClosed;

pub(super) struct MainIngress {
    closed: AtomicBool,
}
impl MainIngress {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
        })
    }
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
    pub(super) fn guard(self: &Arc<Self>) -> MainIngressGuard {
        MainIngressGuard {
            ingress: self.clone(),
        }
    }
}
/// Captured before spawn/poll, independently of any returned context.
pub(super) struct MainIngressGuard {
    ingress: Arc<MainIngress>,
}
impl Drop for MainIngressGuard {
    fn drop(&mut self) {
        self.ingress.closed.store(true, Ordering::Release);
    }
}
