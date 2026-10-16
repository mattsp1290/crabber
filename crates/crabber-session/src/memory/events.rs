//! Stable cursor storage. Erasing records must never renumber surviving receipts
//! or make an already allocated cursor available again.
use crate::StoreError;
use crabber_core::{EventCursor, EventRecord};
use std::collections::{BTreeMap, btree_map::Values};

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct EventLog {
    high_water: u64,
    records: BTreeMap<u64, EventRecord>,
}
impl EventLog {
    pub(super) fn next_cursor(&self) -> Result<EventCursor, StoreError> {
        self.high_water
            .checked_add(1)
            .map(EventCursor)
            .ok_or_else(|| StoreError::Validation("event cursor exhausted".into()))
    }
    pub(super) fn append(&mut self, event: EventRecord) -> Result<(), StoreError> {
        let next = self.next_cursor()?;
        if event.cursor != Some(next) || self.records.contains_key(&next.0) {
            return Err(StoreError::Validation(
                "invalid event cursor allocation".into(),
            ));
        }
        self.records.insert(next.0, event);
        self.high_water = next.0;
        Ok(())
    }
    pub(super) fn get(&self, cursor: EventCursor) -> Option<&EventRecord> {
        self.records.get(&cursor.0)
    }
    pub(super) fn iter(&self) -> Values<'_, u64, EventRecord> {
        self.records.values()
    }
    pub(super) fn remove_session(&mut self, session: &crabber_core::SessionId) {
        self.records.retain(|_, event| &event.session_id != session);
    }
    pub(super) fn last(&self) -> Option<&EventRecord> {
        self.records.last_key_value().map(|(_, event)| event)
    }
}

#[cfg(test)]
#[path = "events/tests.rs"]
mod tests;
