//! Per-session append order keeps another session's erasure from shifting a
//! captured history cutoff or continuation offset.
use crabber_core::SessionId;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct SessionSequence<T> {
    records: BTreeMap<SessionId, Vec<T>>,
}
impl<T> Default for SessionSequence<T> {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
        }
    }
}
impl<T> SessionSequence<T> {
    pub(super) fn push(&mut self, session: SessionId, value: T) {
        self.records.entry(session).or_default().push(value);
    }
    pub(super) fn for_session(&self, session: &SessionId) -> &[T] {
        self.records.get(session).map_or(&[], Vec::as_slice)
    }
    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        self.records.values().flatten()
    }
    pub(super) fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut T> {
        self.records.values_mut().flatten()
    }
}

#[cfg(test)]
#[path = "history/tests.rs"]
mod tests;
