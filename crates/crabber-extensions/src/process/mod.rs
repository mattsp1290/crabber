//! Bounded, tracked child lifecycles for trusted native extensions.

#[cfg(unix)]
mod script;
mod tail;
#[cfg(unix)]
mod unix;
#[cfg(not(unix))]
mod unsupported;

#[cfg(unix)]
pub(crate) use script::{SUPERVISOR_PROTOCOL, supervisor_digest};
pub(crate) use tail::Tail;
#[cfg(unix)]
pub(crate) use unix::*;
#[cfg(not(unix))]
pub(crate) use unsupported::*;

#[cfg(all(test, unix))]
mod tests;
#[cfg(all(test, unix))]
pub(crate) use tests::python;
