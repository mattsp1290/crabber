//! Private append/CAS storage. The independent original cohort is held outside
//! this projection, and must be supplied from the same selected native state.
use super::{
    AssignmentLink, CallResult, ConsumerClose, Correlation, Failure, Head, Origin, Proof,
    Publication, RecordDelta, RegisteredCall, ReverseKind, ReverseMembership, RosterEntry,
    Selection, SourceKey, copied, unknown,
};
use crabber_core::{ControlId, SessionId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct Ledger {
    inner: Arc<Records>,
}
/// Transaction cloning shares immutable payloads. Copy-on-write copies routing
/// maps and Arc handles, never every session's full private result bodies.
#[derive(Clone, Default)]
struct Records {
    origins: BTreeMap<SourceKey, Arc<Origin>>,
    assignments: BTreeMap<SourceKey, Arc<AssignmentLink>>,
    calls: BTreeMap<(SourceKey, ControlId), Arc<RegisteredCall>>,
    results: BTreeMap<(SourceKey, ControlId), Arc<CallResult>>,
    roster: BTreeMap<(SourceKey, ControlId), Arc<RosterEntry>>,
    reverse: BTreeMap<(ReverseKind, String), Arc<ReverseMembership>>,
    publications: BTreeMap<SourceKey, Arc<Vec<Publication>>>,
    heads: BTreeMap<SourceKey, Arc<Head>>,
    closes: BTreeMap<SourceKey, Arc<ConsumerClose>>,
}
fn references<'a, T>(
    values: impl Iterator<Item = &'a T>,
    proof: &mut Proof,
) -> Result<Vec<&'a T>, Failure> {
    let mut result = Vec::new();
    for value in values {
        proof.charge(std::mem::size_of::<&T>())?;
        result.push(value);
    }
    Ok(result)
}
impl Ledger {
    pub(crate) fn selected<'a>(
        &'a self,
        expected: &Correlation,
        cohort: Option<&'a Origin>,
        proof: &mut Proof,
    ) -> Result<Selection<'a>, Failure> {
        self.check_routes(expected, proof)?;
        let key = &expected.key;
        let selected = Selection {
            origin: self.inner.origins.get(key).map(AsRef::as_ref),
            cohort,
            assignment: self.inner.assignments.get(key).map(AsRef::as_ref),
            calls: references(
                self.inner
                    .calls
                    .values()
                    .map(AsRef::as_ref)
                    .filter(|r| r.correlation.key == *key),
                proof,
            )?,
            results: references(
                self.inner
                    .results
                    .values()
                    .map(AsRef::as_ref)
                    .filter(|r| r.correlation.key == *key),
                proof,
            )?,
            roster: references(
                self.inner
                    .roster
                    .values()
                    .map(AsRef::as_ref)
                    .filter(|r| r.correlation.key == *key),
                proof,
            )?,
            reverse: references(
                self.inner
                    .reverse
                    .values()
                    .map(AsRef::as_ref)
                    .filter(|r| r.correlation.key == *key),
                proof,
            )?,
            publications: self
                .inner
                .publications
                .get(key)
                .map_or(&[], |ps| ps.as_slice()),
            head: self.inner.heads.get(key).map(AsRef::as_ref),
            close: self.inner.closes.get(key).map(AsRef::as_ref),
        };
        selected.verify(expected, proof)?;
        Ok(selected)
    }
    fn check_routes(&self, expected: &Correlation, proof: &mut Proof) -> Result<(), Failure> {
        let relevant = |c: &Correlation| {
            c.key == expected.key
                || c.key.identity == expected.key.identity
                || c.identity.epoch == expected.identity.epoch
        };
        for (key, origin) in &self.inner.origins {
            let c = origin.correlation();
            if key == &expected.key || relevant(c) {
                proof.measure(&(key, origin.as_ref()))?;
                if key != &c.key || c.key != expected.key {
                    return Err(unknown("consumer origin index/cross-scope"));
                }
            }
        }
        macro_rules! singleton {
            ($map:expr) => {
                for (key, r) in $map {
                    if key == &expected.key || relevant(&r.correlation) {
                        proof.measure(&(key, r.as_ref()))?;
                        if key != &r.correlation.key
                            || self
                                .inner
                                .origins
                                .get(key)
                                .is_none_or(|o| o.correlation() != &r.correlation)
                        {
                            return Err(unknown("orphaned consumer singleton"));
                        }
                    }
                }
            };
        }
        singleton!(&self.inner.assignments);
        singleton!(&self.inner.heads);
        singleton!(&self.inner.closes);
        macro_rules! calls {
            ($map:expr) => {
                for ((key, id), r) in $map {
                    if key == &expected.key || relevant(&r.correlation) {
                        proof.measure(&(key, id, r.as_ref()))?;
                        if key != &r.correlation.key
                            || id != &r.call
                            || self
                                .inner
                                .origins
                                .get(key)
                                .is_none_or(|o| o.correlation() != &r.correlation)
                        {
                            return Err(unknown("orphaned consumer call original"));
                        }
                    }
                }
            };
        }
        calls!(&self.inner.calls);
        calls!(&self.inner.results);
        calls!(&self.inner.roster);
        for ((kind, id), r) in &self.inner.reverse {
            if relevant(&r.correlation) {
                proof.measure(&(kind, id, r.as_ref()))?;
                if *kind != r.kind
                    || id != r.member_key.as_str()
                    || self
                        .inner
                        .origins
                        .get(&r.correlation.key)
                        .is_none_or(|o| o.correlation() != &r.correlation)
                {
                    return Err(unknown("orphaned consumer reverse"));
                }
            }
        }
        for (key, ps) in &self.inner.publications {
            if ps.is_empty() && !self.inner.origins.contains_key(key) {
                return Err(unknown("unroutable consumer publication roster"));
            }
            if key == &expected.key || ps.iter().any(|p| relevant(&p.correlation)) {
                proof.measure(&(key, ps.as_ref()))?;
                let origin = self
                    .inner
                    .origins
                    .get(key)
                    .ok_or_else(|| unknown("orphaned consumer publications"))?;
                if ps
                    .iter()
                    .any(|p| &p.correlation.key != key || &p.correlation != origin.correlation())
                {
                    return Err(unknown("consumer publication indexed routing"));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn publish(
        &mut self,
        delta: RecordDelta,
        cohort: Option<&Origin>,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        let expected = &delta.publication.correlation;
        let before = self.selected(expected, cohort, proof)?;
        if before.close.is_some() || matches!(before.origin, Some(Origin::NoOpenedMember(_))) {
            return Err(unknown("consumer close is absorbing"));
        }
        if delta.origin.is_some() && before.origin.is_some()
            || delta.assignment.is_some() && before.assignment.is_some()
            || delta.close.is_some() && before.close.is_some()
        {
            return Err(unknown("immutable consumer replacement"));
        }
        if delta.publication.previous_digest.as_ref() != before.head.map(|h| &h.digest) {
            return Err(unknown("consumer publication CAS"));
        }
        for reverse in &delta.reverse {
            if self
                .inner
                .reverse
                .contains_key(&(reverse.kind, reverse.member_key.as_str().to_owned()))
            {
                return Err(unknown("consumer reverse member already assigned"));
            }
        }
        let publications = extended(before.publications, &delta.publication, proof)?;
        let mut after = Selection {
            origin: delta.origin.as_ref().or(before.origin),
            cohort: delta.origin.as_ref().or(cohort),
            assignment: delta.assignment.as_ref().or(before.assignment),
            calls: before.calls,
            results: before.results,
            roster: before.roster,
            reverse: before.reverse,
            publications: &publications,
            head: Some(&delta.head),
            close: delta.close.as_ref().or(before.close),
        };
        push_optional(&mut after.calls, delta.call.as_ref(), proof)?;
        push_optional(&mut after.results, delta.result.as_ref(), proof)?;
        push_optional(&mut after.roster, delta.roster.as_ref(), proof)?;
        for r in &delta.reverse {
            proof.charge(std::mem::size_of::<&ReverseMembership>())?;
            after.reverse.push(r);
        }
        after.verify(expected, proof)?;
        self.insert(delta, publications, proof)
    }
    fn insert(
        &mut self,
        delta: RecordDelta,
        publications: Vec<Publication>,
        proof: &mut Proof,
    ) -> Result<(), Failure> {
        let key = copied(&delta.publication.correlation.key, proof)?;
        let records = Arc::make_mut(&mut self.inner);
        macro_rules! singleton {
            ($value:expr,$map:expr) => {
                if let Some(value) = $value {
                    $map.insert(copied(&key, proof)?, Arc::new(value));
                }
            };
        }
        singleton!(delta.origin, records.origins);
        singleton!(delta.assignment, records.assignments);
        singleton!(delta.close, records.closes);
        macro_rules! call {
            ($value:expr,$map:expr) => {
                if let Some(value) = $value {
                    let index = (copied(&key, proof)?, copied(&value.call, proof)?);
                    if $map.contains_key(&index) {
                        return Err(unknown("immutable consumer call replacement"));
                    }
                    $map.insert(index, Arc::new(value));
                }
            };
        }
        call!(delta.call, records.calls);
        call!(delta.result, records.results);
        call!(delta.roster, records.roster);
        for reverse in delta.reverse {
            proof.measure(&reverse.member_key)?;
            let index = (reverse.kind, reverse.member_key.as_str().to_owned());
            records.reverse.insert(index, Arc::new(reverse));
        }
        records
            .heads
            .insert(copied(&key, proof)?, Arc::new(delta.head));
        records.publications.insert(key, Arc::new(publications));
        Ok(())
    }
    pub(crate) fn origin(&self, key: &SourceKey) -> Option<&Origin> {
        self.inner.origins.get(key).map(AsRef::as_ref)
    }
    pub(crate) fn keys(&self) -> impl Iterator<Item = &SourceKey> {
        self.inner
            .origins
            .keys()
            .chain(self.inner.origins.values().map(|r| &r.correlation().key))
            .chain(self.inner.assignments.keys())
            .chain(self.inner.assignments.values().map(|r| &r.correlation.key))
            .chain(self.inner.calls.keys().map(|(k, _)| k))
            .chain(self.inner.calls.values().map(|r| &r.correlation.key))
            .chain(self.inner.results.keys().map(|(k, _)| k))
            .chain(self.inner.results.values().map(|r| &r.correlation.key))
            .chain(self.inner.roster.keys().map(|(k, _)| k))
            .chain(self.inner.roster.values().map(|r| &r.correlation.key))
            .chain(self.inner.reverse.values().map(|r| &r.correlation.key))
            .chain(self.inner.publications.keys())
            .chain(
                self.inner
                    .publications
                    .values()
                    .flat_map(|ps| ps.iter().map(|p| &p.correlation.key)),
            )
            .chain(self.inner.heads.keys())
            .chain(self.inner.heads.values().map(|r| &r.correlation.key))
            .chain(self.inner.closes.keys())
            .chain(self.inner.closes.values().map(|r| &r.correlation.key))
    }
    pub(crate) fn contains_scope(&self, session: &SessionId) -> bool {
        self.keys().any(|key| &key.session_id == session)
            || self
                .inner
                .publications
                .iter()
                .any(|(key, ps)| ps.is_empty() && !self.inner.origins.contains_key(key))
    }
    pub(crate) fn routes(
        &self,
        session: &SessionId,
        cohorts: &BTreeMap<SourceKey, Arc<Origin>>,
        proof: &mut Proof,
    ) -> Result<bool, Failure> {
        let mut keys = BTreeSet::new();
        for key in self
            .keys()
            .chain(
                cohorts
                    .iter()
                    .flat_map(|(key, origin)| [key, &origin.correlation().key]),
            )
            .filter(|key| &key.session_id == session)
        {
            if !keys.contains(key) {
                proof.measure(key)?;
                keys.insert(key);
            }
        }
        for key in keys {
            let origin = self
                .inner
                .origins
                .get(key)
                .or_else(|| cohorts.get(key))
                .ok_or_else(|| unknown("consumer missing origin"))?;
            let selected = self.selected(
                origin.correlation(),
                cohorts.get(key).map(AsRef::as_ref),
                proof,
            )?;
            if selected.empty() {
                return Err(unknown("consumer orphaned route"));
            }
        }
        for (key, ps) in &self.inner.publications {
            if ps.is_empty() && !self.inner.origins.contains_key(key) {
                return Err(unknown("unroutable consumer roster"));
            }
        }
        Ok(self.contains_scope(session))
    }
    pub(crate) fn purge(&mut self, session: &SessionId) {
        let records = Arc::make_mut(&mut self.inner);
        records.origins.retain(|k, _| &k.session_id != session);
        records.assignments.retain(|k, _| &k.session_id != session);
        records.calls.retain(|(k, _), _| &k.session_id != session);
        records.results.retain(|(k, _), _| &k.session_id != session);
        records.roster.retain(|(k, _), _| &k.session_id != session);
        records
            .reverse
            .retain(|_, r| &r.correlation.key.session_id != session);
        records.publications.retain(|k, _| &k.session_id != session);
        records.heads.retain(|k, _| &k.session_id != session);
        records.closes.retain(|k, _| &k.session_id != session);
    }
}
fn extended(
    before: &[Publication],
    next: &Publication,
    proof: &mut Proof,
) -> Result<Vec<Publication>, Failure> {
    proof.measure(&(before, next))?;
    let mut values = before.to_vec();
    values.push(next.clone());
    Ok(values)
}
fn push_optional<'a, T>(
    values: &mut Vec<&'a T>,
    next: Option<&'a T>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(next) = next {
        proof.charge(std::mem::size_of::<&T>())?;
        values.push(next);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) enum Projection {
    Origin,
    Assignment,
    Calls,
    Results,
    Roster,
    Reverse,
    Publications,
    Head,
    Close,
}
#[cfg(test)]
impl Ledger {
    pub(crate) fn test_erase(&mut self, key: &SourceKey, projection: Projection) {
        let records = Arc::make_mut(&mut self.inner);
        match projection {
            Projection::Origin => {
                records.origins.remove(key);
            }
            Projection::Assignment => {
                records.assignments.remove(key);
            }
            Projection::Calls => records.calls.retain(|(k, _), _| k != key),
            Projection::Results => records.results.retain(|(k, _), _| k != key),
            Projection::Roster => records.roster.retain(|(k, _), _| k != key),
            Projection::Reverse => records.reverse.retain(|_, r| &r.correlation.key != key),
            Projection::Publications => {
                records.publications.remove(key);
            }
            Projection::Head => {
                records.heads.remove(key);
            }
            Projection::Close => {
                records.closes.remove(key);
            }
        }
    }
    pub(crate) fn test_shared_payloads(&self, other: &Self) -> bool {
        self.inner.results.iter().all(|(key, value)| {
            other
                .inner
                .results
                .get(key)
                .is_some_and(|r| Arc::ptr_eq(value, r))
        })
    }
}
