//! Canonical tool order and safety declarations.
use serde::Serialize;
/// The standard tool catalog.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Debug, PartialOrd, Ord)]
pub enum ToolId {
    /// Read UTF-8 files.
    FileRead,
    /// Atomically create or overwrite files.
    FileWrite,
    /// Replace one unique anchor atomically.
    FileEdit,
    /// List directory entries.
    FileList,
    /// Search with ripgrep.
    Search,
    /// Execute a host-configured shell.
    Shell,
    /// Discover matching workspace paths.
    Glob,
    /// Preflight and apply structured multi-file patches.
    ApplyPatch,
    /// Fetch policy-approved HTTPS or confined file text.
    UrlFetch,
    /// Mutate an explicitly routed tracker hub.
    TrackerWrite,
    /// Ask the host's user a question or report a pending question.
    UserInteract,
    /// Apply ordered edits in one atomic write.
    MultiEdit,
    /// Delete one regular file.
    FileDelete,
    /// Move one regular file without overwriting a destination.
    FileMove,
}
impl ToolId {
    /// Deterministic registration order.
    pub const ALL: [Self; 14] = [
        Self::FileRead,
        Self::FileWrite,
        Self::FileEdit,
        Self::FileList,
        Self::Search,
        Self::Shell,
        Self::Glob,
        Self::ApplyPatch,
        Self::UrlFetch,
        Self::TrackerWrite,
        Self::UserInteract,
        Self::MultiEdit,
        Self::FileDelete,
        Self::FileMove,
    ];
    /// The eleven tools in eino-tools `catalog.Standard` at the pinned revision,
    /// compared against `fixtures/eino-tools/catalog.json`.
    pub const EINO_PARITY: [Self; 11] = [
        Self::FileRead,
        Self::FileWrite,
        Self::FileEdit,
        Self::FileList,
        Self::Search,
        Self::Shell,
        Self::Glob,
        Self::ApplyPatch,
        Self::UrlFetch,
        Self::TrackerWrite,
        Self::UserInteract,
    ];
    /// Tools beyond the eleven-tool upstream parity set.
    pub const BEYOND_PARITY: [Self; 3] = [Self::MultiEdit, Self::FileDelete, Self::FileMove];
    /// Stable catalog id.
    pub const fn id(self) -> &'static str {
        match self {
            Self::FileRead => "standard.file-read",
            Self::FileWrite => "standard.file-write",
            Self::FileEdit => "standard.file-edit",
            Self::FileList => "standard.file-list",
            Self::Search => "standard.search",
            Self::Shell => "standard.shell",
            Self::Glob => "standard.glob",
            Self::ApplyPatch => "standard.apply-patch",
            Self::UrlFetch => "standard.url-fetch",
            Self::TrackerWrite => "standard.tracker-write",
            Self::UserInteract => "standard.user-interact",
            Self::MultiEdit => "standard.multi-edit",
            Self::FileDelete => "standard.file-delete",
            Self::FileMove => "standard.file-move",
        }
    }
    /// Model-facing tool name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::FileRead => "file_read",
            Self::FileWrite => "file_write",
            Self::FileEdit => "file_edit",
            Self::FileList => "file_list",
            Self::Search => "search",
            Self::Shell => "shell",
            Self::Glob => "glob",
            Self::ApplyPatch => "apply_patch",
            Self::UrlFetch => "url_fetch",
            Self::TrackerWrite => "tracker_write",
            Self::UserInteract => "user_interact",
            Self::MultiEdit => "multi_edit",
            Self::FileDelete => "file_delete",
            Self::FileMove => "file_move",
        }
    }
    /// Whether repeating the tool has no intentional writes.
    pub const fn retry_safe(self) -> bool {
        matches!(
            self,
            Self::FileRead | Self::FileList | Self::Search | Self::Glob | Self::UrlFetch
        )
    }
    /// Whether execution serializes with other writers of this root.
    pub const fn mutating(self) -> bool {
        matches!(
            self,
            Self::FileWrite
                | Self::FileEdit
                | Self::Shell
                | Self::ApplyPatch
                | Self::MultiEdit
                | Self::FileDelete
                | Self::FileMove
        )
    }
}
/// Deterministic, deduplicated allowlist. Empty is rejected at construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnabledSet(std::collections::BTreeSet<ToolId>);
impl EnabledSet {
    /// Enable every tool in `ToolId::ALL`; URL, tracker and interaction policies are required.
    pub fn all() -> Self {
        Self::only(ToolId::ALL)
    }
    /// Enable the eleven eino-tools parity tools; all three optional policies are required.
    pub fn eino_parity() -> Self {
        Self::only(ToolId::EINO_PARITY)
    }
    /// Register exactly these ids. Does not impose run-wide restrictions.
    pub fn only(ids: impl IntoIterator<Item = ToolId>) -> Self {
        Self(ids.into_iter().collect())
    }
    /// Whether this id will be mounted.
    pub fn contains(&self, id: ToolId) -> bool {
        self.0.contains(&id)
    }
}
