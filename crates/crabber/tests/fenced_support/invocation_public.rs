//! The source-embedding harness uses the same public, audit-derived lifecycle.
//! No native grant constructors or effect-writing methods are imported.
use crate::session_owner::{SessionOwnerError, SessionOwnerKind, SessionOwnerOperation, audit};

#[path = "../../../crabber-session/src/session_owner/public_invocation.rs"]
mod canonical;
pub(crate) use canonical::event;
