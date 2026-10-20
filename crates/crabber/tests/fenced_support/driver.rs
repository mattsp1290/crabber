//! Genuine public capabilities with the same private native source rules.
#![allow(dead_code)]
use crate::session_owner::SessionOwnerError as Error;
pub use crabber::session::session_owner::driver::*;
#[path = "../../../crabber-session/src/session_owner/driver/source.rs"]
mod source;
pub(crate) use source::DriverLink;

#[path = "../../../crabber-session/src/session_owner/driver/history_fixture.rs"]
pub(crate) mod history_fixture;

#[path = "successor_driver.rs"]
pub(crate) mod successor;
pub(crate) use successor::SuccessorDriverLink;
