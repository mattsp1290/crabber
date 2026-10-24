//! Bounded execution, capability-rooted files, process cleanup, and tool identities.
#![warn(missing_docs)]
#![cfg_attr(not(unix), allow(unused))]
#[cfg(not(unix))]
compile_error!("crabber-tools requires Unix");
pub mod deny;
pub mod glob_bounds;
pub use deny::PathDenyPolicy;
pub mod envelope;
pub mod exec;
pub mod identity;
pub mod limits;
pub mod process;
pub mod workspace;
pub use envelope::*;
pub use exec::*;
pub use identity::*;
pub use limits::*;
pub use process::*;
pub use workspace::*;

pub mod capture;
pub use capture::*;

pub mod atomic;

mod owned_process;
pub use owned_process::*;
