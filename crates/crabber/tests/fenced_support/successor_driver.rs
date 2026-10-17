//! Identical private source rules; genuine public grants remain opaque.
use super::Error;
pub use super::{NativeClaimLocator, RegisterSuccessorExecutionDriver};
#[path = "../../../crabber-session/src/session_owner/driver/successor/source.rs"]
pub(crate) mod source;
pub(crate) use source::SuccessorDriverLink;

#[path = "../../../crabber-session/src/session_owner/driver/successor/tool_fresh.rs"]
pub(crate) mod tool_fresh;
