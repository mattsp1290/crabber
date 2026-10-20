//! Independent append-only birth membership under `MemoryStore`'s transaction lock.
use super::{Domain, Error, Input, Kind, Receipt, Scope, Source};
use crate::public_session::v2::proof::{Proof, ReadError};
type Result<T> = std::result::Result<T, ReadError<Error>>;
use crabber_core::{ControlId, EventRecord, InputFingerprint, SessionId};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Default)]
pub(crate) struct Ledger {
    protocols: BTreeSet<Scope>,
    cohorts: BTreeMap<Scope, (ControlId, InputFingerprint)>,
    sources: BTreeMap<(SessionId, ControlId), Source>,
    subjects: BTreeMap<(Scope, Kind, String), ControlId>,
}
impl Ledger {
    pub(crate) fn publish(&mut self, source: Source) -> std::result::Result<(), Error> {
        source.validate()?;
        let scope = &source.scope;
        if source.kind == Kind::Answer && !self.protocol_raw(scope)? {
            return Ok(());
        }
        if self
            .sources
            .contains_key(&(scope.session.clone(), source.operation.clone()))
            || self
                .subjects
                .contains_key(&(scope.clone(), source.kind, source.subject.clone()))
        {
            return Err(Error);
        }
        if source.kind == Kind::Suspension {
            if self.protocols.contains(scope) || self.cohorts.contains_key(scope) {
                return Err(Error);
            }
            self.protocols.insert(scope.clone());
            self.cohorts.insert(
                scope.clone(),
                (source.operation.clone(), source.digest.clone()),
            );
        }
        self.subjects.insert(
            (scope.clone(), source.kind, source.subject.clone()),
            source.operation.clone(),
        );
        self.sources
            .insert((scope.session.clone(), source.operation.clone()), source);
        Ok(())
    }
    pub(crate) fn protocol(&self, scope: &Scope, proof: &mut Proof) -> Result<bool> {
        proof
            .measure(&(scope, self.protocols.contains(scope)))
            .map_err(ReadError::Budget)?;
        if !self.protocols.contains(scope)
            && let Some((key, source)) = self
                .sources
                .iter()
                .find(|(_, source)| source.scope == *scope)
        {
            proof.measure(&(key, source)).map_err(ReadError::Budget)?;
            return Err(Error.into());
        }
        let protected = self.protocol_raw(scope)?;
        if protected {
            let cohort = self.cohorts.get(scope).ok_or(Error)?;
            proof.measure(cohort).map_err(ReadError::Budget)?;
            let origin = self
                .sources
                .get(&(scope.session.clone(), cohort.0.clone()))
                .ok_or(Error)?;
            proof.measure(origin).map_err(ReadError::Budget)?;
            origin.validate()?;
            if origin.scope != *scope
                || origin.kind != Kind::Suspension
                || origin.digest != cohort.1
            {
                return Err(Error.into());
            }
        }
        Ok(protected)
    }
    fn protocol_raw(&self, scope: &Scope) -> std::result::Result<bool, Error> {
        let protected = self.protocols.contains(scope);
        let cohort = self.cohorts.contains_key(scope);
        let member = self
            .subjects
            .range((scope.clone(), Kind::Suspension, String::new())..)
            .next()
            .is_some_and(|(key, _)| key.0 == *scope);
        let source = !protected && self.sources.values().any(|source| source.scope == *scope);
        if protected != cohort || (!protected && (member || source)) {
            return Err(Error);
        }
        Ok(protected)
    }
    pub(crate) fn verify(
        &self,
        input: Input<'_>,
        receipt: Receipt<'_>,
        event: &EventRecord,
        proof: &mut Proof,
    ) -> Result<bool> {
        proof.measure(&input).map_err(ReadError::Budget)?;
        let desired = Source::new(input, receipt, event)?;
        if let Some(saved) = self
            .sources
            .get(&(desired.scope.session.clone(), desired.operation.clone()))
        {
            proof.measure(saved).map_err(ReadError::Budget)?;
            if saved.scope != desired.scope {
                return Err(Error.into());
            }
        }
        if !self.protocol(&desired.scope, proof)? {
            return Ok(false);
        }
        let indexed = self
            .subjects
            .get(&(desired.scope.clone(), desired.kind, desired.subject.clone()))
            .ok_or(Error)?;
        proof.measure(indexed).map_err(ReadError::Budget)?;
        let source = self
            .sources
            .get(&(desired.scope.session.clone(), indexed.clone()))
            .ok_or(Error)?;
        proof.measure(source).map_err(ReadError::Budget)?;
        source.validate()?;
        if source != &desired {
            return Err(Error.into());
        }
        Ok(true)
    }
    pub(crate) fn subjects(
        &self,
        scope: &Scope,
        proof: &mut Proof,
    ) -> Result<Vec<(String, ControlId)>> {
        for (key, source) in &self.sources {
            if source.scope != *scope || source.kind != Kind::Answer {
                continue;
            }
            proof.measure(&(key, source)).map_err(ReadError::Budget)?;
            source.validate()?;
            if key.0 != scope.session
                || key.1 != source.operation
                || self
                    .subjects
                    .get(&(scope.clone(), Kind::Answer, source.subject.clone()))
                    != Some(&source.operation)
            {
                return Err(Error.into());
            }
        }
        let mut out = Vec::new();
        for ((key, kind, subject), operation) in self
            .subjects
            .range((scope.clone(), Kind::Answer, String::new())..)
        {
            if key != scope || *kind != Kind::Answer {
                break;
            }
            proof
                .measure(&(subject, operation))
                .map_err(ReadError::Budget)?;
            if out.len() == 64 {
                return Err(Error.into());
            }
            out.push((subject.clone(), operation.clone()));
        }
        Ok(out)
    }
    /// Borrowed retained identities only; this does not establish complete enrollment.
    pub(crate) fn consistent(
        &self,
        domain: Domain,
        session: &SessionId,
        run: Option<&crabber_core::RunId>,
        proof: &mut Proof,
        exists: impl Fn(&Scope) -> bool,
    ) -> Result<()> {
        for (key, source) in &self.sources {
            if source.scope.domain != domain
                || (key.0 != *session && source.scope.session != *session)
                || run.is_some_and(|run| source.scope.run != *run)
            {
                continue;
            }
            proof.measure(&(key, source)).map_err(ReadError::Budget)?;
            source.validate()?;
            if key.0 != source.scope.session
                || key.1 != source.operation
                || !self.protocol_raw(&source.scope)?
                || self
                    .subjects
                    .get(&(source.scope.clone(), source.kind, source.subject.clone()))
                    != Some(&source.operation)
                || !exists(&source.scope)
            {
                return Err(Error.into());
            }
        }
        for scope in self.protocols.iter().chain(self.cohorts.keys()) {
            if scope.domain != domain
                || scope.session != *session
                || run.is_some_and(|run| scope.run != *run)
            {
                continue;
            }
            proof.measure(scope).map_err(ReadError::Budget)?;
            if !self.protocol_raw(scope)? || !exists(scope) {
                return Err(Error.into());
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn erase_answer_indexes_for_test(&mut self, scope: &Scope) {
        self.subjects
            .retain(|(key, kind, _), _| key != scope || *kind != Kind::Answer);
        assert!(
            self.sources
                .values()
                .any(|s| s.scope == *scope && s.kind == Kind::Answer)
        );
    }
    #[cfg(test)]
    pub(crate) fn erase_indexes_for_test(&mut self, scope: &Scope) {
        self.protocols.remove(scope);
        self.cohorts.remove(scope);
        self.subjects.retain(|(key, _, _), _| key != scope);
        assert!(self.sources.values().any(|source| source.scope == *scope));
    }
    pub(crate) fn purge(&mut self, session: &SessionId) {
        self.protocols.retain(|s| s.session != *session);
        self.cohorts.retain(|s, _| s.session != *session);
        self.subjects.retain(|(s, _, _), _| s.session != *session);
        self.sources.retain(|(s, _), _| s != session);
    }
}
