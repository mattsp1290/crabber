//! Same pure rules applied to genuine external opaque joined evidence.
pub use crabber::session::session_owner::preparation::AdmissionRecoveryPreparation;
#[path = "../../../crabber-session/src/session_owner/preparation/recovery/guard.rs"]
mod guard;
pub(crate) use guard::completion_source;
#[path = "../../../crabber-session/src/session_owner/preparation/recovery/replay.rs"]
pub(crate) mod replay;
