//! Protected Raw origin facts and private actual native caller routing.
#[path = "native_enrollment/origin.rs"]
mod origin;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) use origin::SOURCE_BYTES;
pub(crate) use origin::{Origin, Status, Unclassified};
#[path = "native_enrollment/caller.rs"]
pub(crate) mod caller;

#[path = "native_enrollment/access.rs"]
pub(crate) mod access;

pub(crate) mod pending_initial;
