//! Reuse pure validators with the genuine external public metadata types.
pub use crabber::session::session_owner::preparation::PreparedDriverCompletion;
#[path = "../../../crabber-session/src/session_owner/preparation/driver_completion/verify.rs"]
mod verify;
pub(crate) use verify::*;
