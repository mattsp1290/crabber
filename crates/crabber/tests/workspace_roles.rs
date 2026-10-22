//! Public API denial with real PostgreSQL LOGIN roles and row-integrity controls.
#![cfg(feature = "postgres")]
#[path = "workspace_roles/controls.rs"]
mod controls;
#[path = "workspace_roles/fixture.rs"]
mod fixture;
#[path = "workspace_roles/management.rs"]
mod management;
#[path = "workspace_roles/positive.rs"]
mod positive;
#[path = "run_controls/provider.rs"]
mod provider;
#[path = "workspace_roles/queue.rs"]
mod queue;
#[path = "workspace_roles/reads.rs"]
mod reads;
#[path = "workspace_roles/recovery.rs"]
mod recovery;
