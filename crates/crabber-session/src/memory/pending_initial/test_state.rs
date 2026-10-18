//! Native-only test observers, never authority or persisted protocol state.
use crate::session_owner::owned_scope::NativeInitialOperation;
use std::sync::{Arc, Mutex, atomic::AtomicU8};
#[derive(Clone, Default)]
pub(crate) struct TestState {
    pub(crate) fault: Arc<AtomicU8>,
    pub(crate) precommit_usage: Arc<Mutex<Vec<(NativeInitialOperation, usize)>>>,
}
