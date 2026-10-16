//! Actual external source types and the identical read-only private validators.
use crate::session_owner::SessionOwnerError as Error;
#[path = "../../../crabber-session/src/session_owner/preparation/expired/native.rs"]
pub(crate) mod native;
pub(crate) use native::{claimed, completion_source, replacement};
#[path = "../../../crabber-session/src/session_owner/preparation/expired/history.rs"]
pub(crate) mod history;
#[path = "../../../crabber-session/src/session_owner/preparation/expired/replay.rs"]
pub(crate) mod replay;
#[path = "../../../crabber-session/src/session_owner/preparation/expired/suspension.rs"]
mod suspension;
pub use crabber::session::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunPreparation, ExpiredRunTarget, InspectExpiredRunPreparation,
};
pub(crate) use suspension::suspension_digest;
