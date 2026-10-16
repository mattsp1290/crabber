//! Pure fixture checks inspect the genuine opaque observations without reminting them.
#![allow(dead_code)]
use crate::session_owner::SessionOwnerError as Error;
pub use crabber::session::session_owner::owned_scope::*;
#[path = "../../../crabber-session/src/session_owner/owned_scope/join_verify.rs"]
pub(crate) mod join_verify;
#[path = "../../../crabber-session/src/session_owner/owned_scope/source.rs"]
pub(crate) mod source;
