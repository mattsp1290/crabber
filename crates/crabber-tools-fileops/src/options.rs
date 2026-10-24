//! Host-owned options and shared capacity.
use crabber_tools_core::{Capacity, Limits, WorkspaceRoot};
use serde::Serialize;
use std::sync::Arc;
/// Serializable file policy, included in mount identity.
#[derive(Clone, Serialize)]
pub struct FileopsPolicy {
    /// Finite wait and concurrency bounds.
    pub limits: Limits,
}
/// Resources shared by every file executor in a mount.
pub struct Options {
    /// Host path exclusions, shared across read and write tools.
    pub deny: crabber_tools_core::PathDenyPolicy,
    /// Admitted capability directory.
    pub root: Arc<WorkspaceRoot>,
    /// Finite wait and concurrency bounds.
    pub limits: Limits,
    /// Capacity shared with other tools in the mount.
    pub capacity: Capacity,
    /// Optional host hook immediately before atomic writes; for `file_move`, between link and unlink.
    /// Useful for interruption tests.
    pub pre_write: Option<Arc<dyn Fn() + Send + Sync>>,
}
