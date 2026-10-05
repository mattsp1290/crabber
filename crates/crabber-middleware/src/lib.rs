//! Dependency-light, first-party model middleware recipes.
//!
//! [`agents_md`] provides the plan-sealed `AGENTS.md` prompt recipe. It has no
//! ambient filesystem access: an embedding host must supply a deny-by-default
//! [`crabber_extension::WorkspaceReaderResolver`], validate every persisted
//! workspace mapping, and return a rooted reader that enforces path, symlink,
//! access, and byte-bound policy.
//!
//! See the [middleware security and embedding guide](../../../docs/middleware.md)
//! and its linked credential-free runnable example.

pub mod agents_md;

pub use agents_md::*;
