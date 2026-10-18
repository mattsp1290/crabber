//! Memory's independent publication membership; mutable queue audits are leaves.
use super::{Audit, Scope, Source, StoreError, corrupt};
use crate::public_session::v2::proof::{Proof, ReadError};
use crabber_core::{ControlId, SessionId};
use std::collections::{BTreeMap, BTreeSet};
type Result<T> = std::result::Result<T, ReadError<StoreError>>;
#[derive(Clone, Default)]
pub(crate) struct Ledger {
    protocols: BTreeSet<Scope>,
    origins: BTreeMap<Scope, Source>,
    heads: BTreeMap<Scope, Source>,
    sources: BTreeMap<(SessionId, ControlId), Source>,
    members: BTreeMap<(Scope, u64), ControlId>,
}
impl Ledger {
    #[cfg(test)]
    pub(crate) fn raw_initial_snapshot(
        &self,
        session: &SessionId,
    ) -> crabber_core::InputFingerprint {
        crate::native_enrollment::caller::contract::snapshot_digest(&(
            self.protocols
                .iter()
                .filter(|s| s.session == *session)
                .collect::<Vec<_>>(),
            self.origins
                .iter()
                .filter(|(s, _)| s.session == *session)
                .collect::<Vec<_>>(),
            self.heads
                .iter()
                .filter(|(s, _)| s.session == *session)
                .collect::<Vec<_>>(),
            self.sources
                .iter()
                .filter(|((s, _), _)| s == session)
                .collect::<Vec<_>>(),
            self.members
                .iter()
                .filter(|((s, _), _)| s.session == *session)
                .collect::<Vec<_>>(),
        ))
    }
    /// Unverified borrowed metadata. Full history/Source authentication and
    /// caller Proof precharge remain mandatory before this can be used.
    pub(crate) fn source(&self, session: &SessionId, operation: &ControlId) -> Option<&Source> {
        self.sources.get(&(session.clone(), operation.clone()))
    }
    pub(crate) fn contains_operation(
        &self,
        session: &SessionId,
        operation: &ControlId,
        proof: &mut Proof,
    ) -> Result<bool> {
        let source = self.sources.get(&(session.clone(), operation.clone()));
        proof
            .measure(&(session, operation, source))
            .map_err(ReadError::Budget)?;
        Ok(source.is_some())
    }
    pub(crate) fn protocol(&self, scope: &Scope, proof: &mut Proof) -> Result<bool> {
        let protected = self.protocols.contains(scope);
        proof
            .measure(&(scope, protected))
            .map_err(ReadError::Budget)?;
        let origin = self.origins.get(scope);
        let head = self.heads.get(scope);
        if !protected {
            if origin.is_some()
                || head.is_some()
                || self.sources.values().any(|s| s.scope == *scope)
                || self.members.keys().any(|(s, _)| s == scope)
            {
                return Err(corrupt().into());
            }
            return Ok(false);
        }
        let origin = origin.ok_or_else(corrupt)?;
        proof.measure(origin).map_err(ReadError::Budget)?;
        origin.validate()?;
        if origin.scope != *scope
            || origin.sequence != 1
            || self
                .sources
                .get(&(scope.session.clone(), origin.operation.clone()))
                != Some(origin)
            || self.members.get(&(scope.clone(), 1)) != Some(&origin.operation)
        {
            return Err(corrupt().into());
        }
        Ok(true)
    }
    pub(crate) fn selected<'a>(
        &'a self,
        audit: &Audit,
        proof: &mut Proof,
    ) -> Result<Option<&'a Source>> {
        proof.measure(audit).map_err(ReadError::Budget)?;
        let scope = Scope {
            session: audit.receipt.session_id.clone(),
            queue: audit.receipt.summary.queue_id.clone(),
        };
        if !self.protocol(&scope, proof)? {
            return Ok(None);
        }
        let source = self
            .sources
            .get(&(scope.session.clone(), audit.receipt.operation_id.clone()))
            .ok_or_else(corrupt)?;
        proof.measure(source).map_err(ReadError::Budget)?;
        source.verify(audit)?;
        if self.members.get(&(scope.clone(), source.sequence)) != Some(&source.operation) {
            return Err(corrupt().into());
        }
        if source.sequence > 1 {
            let previous = self
                .members
                .get(&(scope.clone(), source.sequence - 1))
                .and_then(|id| self.sources.get(&(scope.session.clone(), id.clone())))
                .ok_or_else(corrupt)?;
            proof.measure(previous).map_err(ReadError::Budget)?;
            previous.validate()?;
            source.follows(previous)?;
        }
        Ok(Some(source))
    }
    /// Authenticate only the immutable prefix ending at the selected operation.
    pub(crate) fn prefix<'a>(
        &self,
        selected: &Audit,
        proof: &mut Proof,
        mut audit: impl FnMut(&ControlId, &mut Proof) -> Result<&'a Audit>,
    ) -> Result<Vec<&'a Audit>> {
        let last = self
            .selected(selected, proof)?
            .ok_or(crate::StoreError::HostUnsupported)?;
        let mut result = Vec::new();
        let mut previous: Option<&Source> = None;
        for sequence in 1..=last.sequence {
            let operation = self
                .members
                .get(&(last.scope.clone(), sequence))
                .ok_or_else(corrupt)?;
            proof
                .measure(&(sequence, operation))
                .map_err(ReadError::Budget)?;
            let source = self
                .sources
                .get(&(last.scope.session.clone(), operation.clone()))
                .ok_or_else(corrupt)?;
            proof.measure(source).map_err(ReadError::Budget)?;
            let original = audit(operation, proof)?;
            source.verify(original)?;
            if source.scope != last.scope
                || source.sequence != sequence
                || source.operation != *operation
            {
                return Err(corrupt().into());
            }
            if let Some(previous) = previous {
                source.follows(previous)?;
            } else if self.origins.get(&last.scope) != Some(source) {
                return Err(corrupt().into());
            }
            result.push(original);
            previous = Some(source);
        }
        if previous != Some(last) {
            return Err(corrupt().into());
        }
        Ok(result)
    }
    pub(crate) fn original_receipt(
        &self,
        receipt: &super::QueueReceipt,
        proof: &mut Proof,
    ) -> Result<bool> {
        let scope = Scope {
            session: receipt.session_id.clone(),
            queue: receipt.summary.queue_id.clone(),
        };
        if !self.protocol(&scope, proof)? {
            return Ok(false);
        }
        let source = self
            .sources
            .get(&(scope.session.clone(), receipt.operation_id.clone()))
            .ok_or_else(corrupt)?;
        proof.measure(source).map_err(ReadError::Budget)?;
        source.verify_receipt(receipt)?;
        if self.members.get(&(scope, source.sequence)) != Some(&source.operation) {
            return Err(corrupt().into());
        }
        Ok(true)
    }
    pub(crate) fn history<'a>(
        &'a self,
        scope: &Scope,
        proof: &mut Proof,
        mut audit: impl FnMut(&ControlId, &mut Proof) -> Result<&'a Audit>,
    ) -> Result<Option<&'a Source>> {
        if !self.protocol(scope, proof)? {
            return Ok(None);
        }
        let head = self.heads.get(scope).ok_or_else(corrupt)?;
        proof.measure(head).map_err(ReadError::Budget)?;
        head.validate()?;
        if head.scope != *scope {
            return Err(corrupt().into());
        }
        let mut previous: Option<&Source> = None;
        for ((key, sequence), operation) in self.members.range((scope.clone(), 0)..) {
            if key != scope {
                break;
            }
            proof
                .measure(&(sequence, operation))
                .map_err(ReadError::Budget)?;
            let source = self
                .sources
                .get(&(scope.session.clone(), operation.clone()))
                .ok_or_else(corrupt)?;
            proof.measure(source).map_err(ReadError::Budget)?;
            if source.scope != *scope
                || source.operation != *operation
                || source.sequence != *sequence
            {
                return Err(corrupt().into());
            }
            source.verify(audit(operation, proof)?)?;
            if let Some(previous) = previous {
                source.follows(previous)?;
            } else if source.sequence != 1 || self.origins.get(scope) != Some(source) {
                return Err(corrupt().into());
            }
            previous = Some(source);
        }
        if previous != Some(head) {
            return Err(corrupt().into());
        }
        // A retained protected source cannot disappear merely by losing an index.
        for ((session, operation), source) in &self.sources {
            if source.scope != *scope {
                continue;
            }
            proof
                .measure(&(session, operation, source))
                .map_err(ReadError::Budget)?;
            if *session != scope.session
                || *operation != source.operation
                || self.members.get(&(scope.clone(), source.sequence)) != Some(operation)
            {
                return Err(corrupt().into());
            }
        }
        Ok(Some(head))
    }
    /// Safe projection uses the independently published original receipt. It does
    /// not hash, clone or decode the private original control body.
    pub(crate) fn projected<'a>(
        &'a self,
        scope: &Scope,
        proof: &mut Proof,
        mut receipt: impl FnMut(&ControlId, &mut Proof) -> Result<&'a super::QueueReceipt>,
    ) -> Result<Option<&'a Source>> {
        if !self.protocol(scope, proof)? {
            return Ok(None);
        }
        let head = self.heads.get(scope).ok_or_else(corrupt)?;
        proof.measure(head).map_err(ReadError::Budget)?;
        head.validate()?;
        let mut previous: Option<&Source> = None;
        for ((key, sequence), operation) in self.members.range((scope.clone(), 0)..) {
            if key != scope {
                break;
            }
            proof
                .measure(&(sequence, operation))
                .map_err(ReadError::Budget)?;
            let source = self
                .sources
                .get(&(scope.session.clone(), operation.clone()))
                .ok_or_else(corrupt)?;
            proof.measure(source).map_err(ReadError::Budget)?;
            source.validate()?;
            if source.scope != *scope
                || source.operation != *operation
                || source.sequence != *sequence
            {
                return Err(corrupt().into());
            }
            source.verify_receipt(receipt(operation, proof)?)?;
            if let Some(previous) = previous {
                source.follows(previous)?;
            } else if source.sequence != 1 || self.origins.get(scope) != Some(source) {
                return Err(corrupt().into());
            }
            previous = Some(source);
        }
        if previous != Some(head) {
            return Err(corrupt().into());
        }
        for ((session, operation), source) in &self.sources {
            if source.scope != *scope {
                continue;
            }
            proof
                .measure(&(session, operation, source))
                .map_err(ReadError::Budget)?;
            if *session != scope.session
                || *operation != source.operation
                || self.members.get(&(scope.clone(), source.sequence)) != Some(operation)
            {
                return Err(corrupt().into());
            }
        }
        Ok(Some(head))
    }
    pub(crate) fn publish(
        &mut self,
        audit: &Audit,
        source: Source,
    ) -> std::result::Result<(), StoreError> {
        source.verify(audit)?;
        let scope = &source.scope;
        if self
            .sources
            .contains_key(&(scope.session.clone(), source.operation.clone()))
            || self.members.contains_key(&(scope.clone(), source.sequence))
        {
            return Err(corrupt());
        }
        if let Some(previous) = self.heads.get(scope) {
            if !self.protocols.contains(scope) || !self.origins.contains_key(scope) {
                return Err(corrupt());
            }
            source.follows(previous)?;
        } else {
            if source.sequence != 1
                || self.protocols.contains(scope)
                || self.origins.contains_key(scope)
            {
                return Err(corrupt());
            }
            self.protocols.insert(scope.clone());
            self.origins.insert(scope.clone(), source.clone());
        }
        self.members
            .insert((scope.clone(), source.sequence), source.operation.clone());
        self.heads.insert(scope.clone(), source.clone());
        self.sources
            .insert((scope.session.clone(), source.operation.clone()), source);
        Ok(())
    }
    pub(crate) fn consistent(
        &self,
        session: &SessionId,
        proof: &mut Proof,
        exists: impl Fn(&Scope) -> bool,
    ) -> Result<()> {
        for scope in self
            .protocols
            .iter()
            .chain(self.origins.keys())
            .chain(self.heads.keys())
            .chain(self.sources.values().map(|s| &s.scope))
            .chain(self.members.keys().map(|(s, _)| s))
        {
            if scope.session != *session {
                continue;
            }
            proof.measure(scope).map_err(ReadError::Budget)?;
            if !self.protocols.contains(scope)
                || !self.origins.contains_key(scope)
                || !exists(scope)
            {
                return Err(corrupt().into());
            }
        }
        Ok(())
    }
    pub(crate) fn purge(&mut self, session: &SessionId) {
        self.protocols.retain(|s| s.session != *session);
        self.origins.retain(|s, _| s.session != *session);
        self.heads.retain(|s, _| s.session != *session);
        self.sources.retain(|(s, _), _| s != session);
        self.members.retain(|(s, _), _| s.session != *session);
    }
}
