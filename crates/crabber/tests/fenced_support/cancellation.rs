//! Reuse the public DTOs and actual shared private validator for this adapter.
use crabber::session::session_owner::SessionOwnerError as Error;
pub use crabber::session::session_owner::cancellation::*;
#[path = "../../../crabber-session/src/session_owner/cancellation/record.rs"]
mod record;
pub(crate) use record::Cancellation;
#[path = "../../../crabber-session/src/session_owner/cancellation/proof.rs"]
pub(crate) mod proof;
