//! Private immutable maps. Every publication is validated before mutation.
use super::{
    Assignment, Bundle, CloseRecord, Epoch, Failure, Head, Identity, Proof, Publication,
    RecordDelta, RegisteredComplete, SourceKey, SourceSlot, bounded, unknown,
};
use crabber_core::{ControlId, RunId, SessionId};
use std::collections::BTreeMap;
#[derive(Clone, Default)]
pub(crate) struct Ledger {
    slots: BTreeMap<SourceKey, SourceSlot>,
    epochs: BTreeMap<ControlId, Epoch>,
    registered: BTreeMap<ControlId, RegisteredComplete>,
    assignments: BTreeMap<ControlId, Assignment>,
    closes: BTreeMap<ControlId, CloseRecord>,
    publications: BTreeMap<ControlId, Vec<Publication>>,
    heads: BTreeMap<ControlId, Head>,
}
impl Ledger {
    pub(crate) fn selected(
        &self,
        key: &SourceKey,
        identity: &Identity,
        proof: &mut Proof,
    ) -> Result<Bundle<'_>, Failure> {
        // Selected routing includes every row which could allege either original
        // source or epoch. Orphaned children cannot disappear behind no slot.
        for (id, epoch) in &self.epochs {
            if epoch.correlation.key == *key || *id == identity.epoch {
                bounded(epoch, proof)?;
                if id != &epoch.correlation.identity.epoch
                    || self
                        .slots
                        .get(&epoch.correlation.key)
                        .is_none_or(|s| &s.epoch != id)
                {
                    return Err(unknown("orphaned Pending epoch"));
                }
            }
        }
        self.selected_children(key, identity, proof)?;
        for (stored, slot) in &self.slots {
            if stored == key
                || slot.key == *key
                || stored.identity == key.identity
                || slot.key.identity == key.identity
            {
                bounded(&(stored, slot), proof)?;
                if stored != &slot.key
                    || self
                        .epochs
                        .get(&slot.epoch)
                        .is_none_or(|e| e.correlation.key != slot.key)
                {
                    return Err(unknown("Pending slot reverse route"));
                }
            }
        }
        for (id, publications) in &self.publications {
            if *id == identity.epoch
                || publications
                    .iter()
                    .any(|p| p.key == *key || p.key.identity == key.identity)
            {
                proof.measure(&(id, publications))?;
                let epoch = self
                    .epochs
                    .get(id)
                    .ok_or_else(|| unknown("orphaned Pending publications"))?;
                if publications
                    .iter()
                    .any(|p| p.epoch != *id || p.key != epoch.correlation.key)
                {
                    return Err(unknown("Pending publication routing"));
                }
            }
        }
        for (id, head) in &self.heads {
            if *id == identity.epoch || head.key == *key || head.key.identity == key.identity {
                bounded(&(id, head), proof)?;
                if self
                    .epochs
                    .get(id)
                    .is_none_or(|e| e.correlation.key != head.key)
                    || head.epoch != *id
                {
                    return Err(unknown("orphaned Pending head"));
                }
            }
        }
        let slot = self.slots.get(key);
        let id = slot.map_or(&identity.epoch, |s| &s.epoch);
        if let Some(other) = self.epochs.get(&identity.epoch)
            && other.correlation.key != *key
        {
            return Err(unknown("Pending epoch reused across source"));
        }
        let bundle = Bundle {
            slot,
            epoch: self.epochs.get(id),
            registered: self.registered.get(id),
            assignment: self.assignments.get(id),
            close: self.closes.get(id),
            publications: self.publications.get(id).map_or(&[], Vec::as_slice),
            head: self.heads.get(id),
        };
        bundle.verify(key, proof)?;
        Ok(bundle)
    }
    fn selected_children(
        &self,
        key: &SourceKey,
        identity: &Identity,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        for (id, record) in &self.registered {
            if record.key == *key || *id == identity.epoch {
                bounded(record, proof)?;
                if self
                    .epochs
                    .get(id)
                    .is_none_or(|e| e.correlation.key != record.key)
                {
                    return Err(unknown("orphaned Pending registration"));
                }
            }
        }
        for (id, record) in &self.assignments {
            if record.correlation.key == *key || *id == identity.epoch {
                bounded(record, proof)?;
                if self
                    .epochs
                    .get(id)
                    .is_none_or(|e| e.correlation != record.correlation)
                {
                    return Err(unknown("orphaned Pending assignment"));
                }
            }
        }
        for (id, record) in &self.closes {
            if record.correlation.key == *key || *id == identity.epoch {
                bounded(record, proof)?;
                if self
                    .epochs
                    .get(id)
                    .is_none_or(|e| e.correlation != record.correlation)
                {
                    return Err(unknown("orphaned Pending close"));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn publish(&mut self, delta: RecordDelta, proof: &mut Proof) -> Result<(), Failure> {
        let p = delta
            .publication
            .as_ref()
            .ok_or_else(|| unknown("missing Pending publication delta"))?;
        let key = &p.key;
        let epoch = delta
            .epoch
            .as_ref()
            .or_else(|| self.epochs.get(&p.epoch))
            .ok_or_else(|| unknown("missing Pending epoch delta"))?;
        let before = self.selected(key, &epoch.correlation.identity, proof)?;
        if before.close.is_some() {
            return Err(unknown("closed Pending epoch"));
        }
        if delta.epoch.is_some() && before.epoch.is_some()
            || delta.slot.is_some() && before.slot.is_some()
            || delta.registered.is_some() && before.registered.is_some()
            || delta.assignment.is_some() && before.assignment.is_some()
            || delta.close.is_some() && before.close.is_some()
        {
            return Err(unknown("immutable Pending record replacement"));
        }
        proof.measure(&before.publications)?;
        let mut publications = before.publications.to_vec();
        bounded(p, proof)?;
        publications.push(p.clone());
        let after = Bundle {
            slot: delta.slot.as_ref().or(before.slot),
            epoch: Some(epoch),
            registered: delta.registered.as_ref().or(before.registered),
            assignment: delta.assignment.as_ref().or(before.assignment),
            close: delta.close.as_ref().or(before.close),
            publications: &publications,
            head: delta.head.as_ref(),
        };
        after.verify(key, proof)?;
        bounded(&epoch.correlation.identity.epoch, proof)?;
        let id = epoch.correlation.identity.epoch.clone();
        let head = delta
            .head
            .ok_or_else(|| unknown("missing Pending head delta"))?;
        // Independent owned index keys are separate materialized carriers.
        for present in [
            delta.epoch.is_some(),
            delta.registered.is_some(),
            delta.assignment.is_some(),
            delta.close.is_some(),
            true,
        ] {
            if present {
                proof.measure(&id)?;
            }
        }
        if let Some(slot) = &delta.slot {
            proof.measure(&slot.key)?;
        }
        if let Some(value) = delta.epoch {
            self.epochs.insert(id.clone(), value);
        }
        if let Some(value) = delta.slot {
            self.slots.insert(value.key.clone(), value);
        }
        if let Some(value) = delta.registered {
            self.registered.insert(id.clone(), value);
        }
        if let Some(value) = delta.assignment {
            self.assignments.insert(id.clone(), value);
        }
        if let Some(value) = delta.close {
            self.closes.insert(id.clone(), value);
        }
        self.publications.insert(id.clone(), publications);
        self.heads.insert(id, head);
        Ok(())
    }
    pub(crate) fn assignment_for_run(
        &self,
        session: &SessionId,
        run: &RunId,
        proof: &mut Proof,
    ) -> Result<Option<&Assignment>, Failure> {
        let mut result = None;
        for assignment in self.assignments.values().filter(|a| &a.run_id == run) {
            bounded(assignment, proof)?;
            if &assignment.correlation.key.session_id != session || result.is_some() {
                return Err(unknown("Pending Run assignment route"));
            }
            self.selected(
                &assignment.correlation.key,
                &assignment.correlation.identity,
                proof,
            )?;
            result = Some(assignment);
        }
        Ok(result)
    }
    pub(crate) fn slot(&self, key: &SourceKey) -> Option<&SourceSlot> {
        self.slots.get(key)
    }
    pub(crate) fn epoch_for_slot(&self, slot: &SourceSlot) -> Option<&Epoch> {
        self.epochs.get(&slot.epoch)
    }
    #[cfg(test)]
    pub(crate) fn test_epoch(&self, key: &SourceKey) -> Option<&Epoch> {
        self.slots
            .get(key)
            .and_then(|slot| self.epochs.get(&slot.epoch))
    }
    /// Complete borrowed originals, including nonwinning/misindexed Epoch rows.
    pub(crate) fn epoch_entries(&self) -> impl Iterator<Item = (&ControlId, &Epoch)> {
        self.epochs.iter()
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &SourceKey> {
        self.slots
            .keys()
            .chain(self.slots.values().map(|s| &s.key))
            .chain(self.epochs.values().map(|e| &e.correlation.key))
            .chain(self.registered.values().map(|r| &r.key))
            .chain(self.assignments.values().map(|a| &a.correlation.key))
            .chain(self.closes.values().map(|c| &c.correlation.key))
            .chain(
                self.publications
                    .values()
                    .flat_map(|v| v.iter().map(|p| &p.key)),
            )
            .chain(self.heads.values().map(|h| &h.key))
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
            && self.epochs.is_empty()
            && self.registered.is_empty()
            && self.assignments.is_empty()
            && self.closes.is_empty()
            && self.publications.is_empty()
            && self.heads.is_empty()
    }
    pub(crate) fn contains_scope(&self, session: &SessionId) -> bool {
        self.keys().any(|key| &key.session_id == session)
            // A retained empty roster with no epoch has no attributable scope.
            // It cannot justify treating any proposed target identity as free.
            || self.publications.iter().any(|(id,records)|records.is_empty()&&!self.epochs.contains_key(id))
    }
    #[cfg(test)]
    pub(crate) fn test_erase_routes_keep_empty_roster(&mut self, key: &SourceKey) {
        let id = self.slots.get(key).unwrap().epoch.clone();
        let mut records = self.publications.remove(&id).unwrap();
        records.clear();
        *self = Self::default();
        self.publications.insert(id, records);
    }
    pub(crate) fn source_claimed(
        &self,
        key: &SourceKey,
        proof: &mut Proof,
    ) -> Result<bool, Failure> {
        if let Some(slot) = self.slots.get(key) {
            bounded(slot, proof)?;
            let epoch = self
                .epochs
                .get(&slot.epoch)
                .ok_or_else(|| unknown("missing Pending slot epoch"))?;
            self.selected(key, &epoch.correlation.identity, proof)?;
            return Ok(true);
        }
        if self
            .keys()
            .any(|candidate| candidate == key || candidate.identity == key.identity)
        {
            return Err(unknown("Pending source missing slot"));
        }
        Ok(false)
    }
    pub(crate) fn routes(&self, session: &SessionId, proof: &mut Proof) -> Result<bool, Failure> {
        let mut present = false;
        for key in self.keys().filter(|key| &key.session_id == session) {
            bounded(key, proof)?;
            if !self.source_claimed(key, proof)? {
                return Err(unknown("orphaned Pending relation"));
            }
            present = true;
        }
        for (id, records) in &self.publications {
            if records.is_empty() && !self.epochs.contains_key(id) {
                return Err(unknown("orphaned empty Pending publication roster"));
            }
        }
        Ok(present)
    }
    pub(crate) fn purge(&mut self, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
        self.routes(session, proof)?;
        for (id, epoch) in &self.epochs {
            if &epoch.correlation.key.session_id == session {
                proof.measure(id)?;
            }
        }
        let ids: Vec<_> = self
            .epochs
            .iter()
            .filter(|(_, e)| &e.correlation.key.session_id == session)
            .map(|(id, _)| id.clone())
            .collect();
        self.slots.retain(|key, _| &key.session_id != session);
        for id in ids {
            self.epochs.remove(&id);
            self.registered.remove(&id);
            self.assignments.remove(&id);
            self.closes.remove(&id);
            self.publications.remove(&id);
            self.heads.remove(&id);
        }
        Ok(())
    }
}

#[cfg(test)]
impl Ledger {
    pub(crate) fn test_add_nonwinning_epoch(&mut self, key: &SourceKey) {
        let slot = self.slots.get(key).unwrap();
        let mut extra = self.epochs.get(&slot.epoch).unwrap().clone();
        let id = ControlId::new();
        extra.correlation.identity.epoch = id.clone();
        self.epochs.insert(id, extra);
    }
    pub(crate) fn test_misindex_winning_epoch(&mut self, key: &SourceKey) {
        let id = &self.slots.get(key).unwrap().epoch;
        let epoch = self.epochs.remove(id).unwrap();
        self.epochs.insert(ControlId::new(), epoch);
    }
    pub(crate) fn test_foreign_winning_epoch_session(&mut self, key: &SourceKey) {
        let id = &self.slots.get(key).unwrap().epoch;
        self.epochs.get_mut(id).unwrap().correlation.key.session_id = SessionId::new();
    }
}
