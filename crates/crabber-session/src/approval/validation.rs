//! Canonical bounded control semantics, separate from generated worker bookkeeping.
use super::{
    ApprovalAnswer, ApprovalContinuationClaim, ApprovalError as Error, ApprovalSuspension,
    CallApproval, MAX_APPROVAL_CALLS,
};
use crabber_core::{InputFingerprint, InteractionKind, RunId, SessionId};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(super) fn id(value: &str) -> Result<(), Error> {
    crate::attempt::validate_identifier(value).map_err(|_| Error::InvalidRequest)
}
fn identity(value: &str, max: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}
pub(super) fn fingerprint(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    if crate::snapshot::encoded_bytes(value, 4 * 1024 * 1024).is_none() {
        return Err(Error::InvalidRequest);
    }
    let mut value = serde_json::to_value(value).map_err(|_| Error::InvalidRequest)?;
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).map_err(|_| Error::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", Sha256::digest(bytes))).map_err(|_| Error::InvalidRequest)
}
impl ApprovalSuspension {
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self
            .source_turn_id
            .as_ref()
            .is_some_and(|id| identity(&id.0, 512).is_err())
        {
            return Err(Error::InvalidRequest);
        }
        for value in [
            &self.id.0,
            &self.attempt.id.0,
            &self.next_attempt.id.0,
            &self.continuation.id.0,
        ] {
            id(value)?;
        }
        if self.turns_used.is_none_or(|count| count == 0)
            || self.calls.is_empty()
            || self.calls.len() > MAX_APPROVAL_CALLS
            || self.next_attempt.id == self.attempt.id
            || self.attempt.generation.next().ok() != Some(self.next_attempt.generation)
        {
            return Err(Error::InvalidRequest);
        }
        let mut calls = BTreeSet::new();
        let mut phases = BTreeSet::new();
        let mut questions = BTreeSet::new();
        for call in &self.calls {
            id(&call.target.id.0)?;
            identity(&call.target.call_id.0, 512)?;
            if !calls.insert(&call.target.call_id)
                || !phases.insert(&call.target.id)
                || call.target.attempt != self.next_attempt
                || call.target.generation != crabber_core::PhaseGeneration::INITIAL
            {
                return Err(Error::InvalidRequest);
            }
            if let CallApproval::Ask {
                id: question,
                description,
            } = &call.decision
            {
                id(&question.0)?;
                if !questions.insert(question)
                    || description.kind() != InteractionKind::Approval
                    || description.response_schema() != &serde_json::json!({"type":"boolean"})
                {
                    return Err(Error::InvalidRequest);
                }
            }
        }
        if questions.is_empty() {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
    /// Original outbox semantics. Planned identities remain stable on retry.
    /// # Errors
    /// Rejects invalid target sets, non-boolean approval contracts or oversized input.
    pub fn fingerprint(&self, session: &SessionId, run: &RunId) -> Result<InputFingerprint, Error> {
        self.validate()?;
        identity(&session.0, 512)?;
        identity(&run.0, 128)?;
        fingerprint(&(
            "crabber.approval.suspend.v1",
            session,
            run,
            self.expected_revision,
            &self.attempt,
            &self.expected_plan,
            &self.continuation,
            &self.next_attempt,
            &self.calls,
            &self.usage,
            self.turns_used,
        ))
    }
}
impl ApprovalAnswer {
    /// The host assertion, exact choice and generation participate in retry identity.
    /// # Errors
    /// Rejects invalid or unbounded control/actor identity.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        for value in [&self.id.0, &self.continuation.id.0, &self.interaction_id.0] {
            id(value)?;
        }
        identity(&self.session_id.0, 512)?;
        identity(&self.run_id.0, 128)?;
        identity(&self.resolver.resolver_id, 512)?;
        identity(&self.resolver.authority_claim, 1024)?;
        fingerprint(&(
            "crabber.approval.answer.v1",
            &self.session_id,
            &self.run_id,
            self.expected_revision,
            &self.continuation,
            &self.interaction_id,
            &self.resolver,
            self.reply,
        ))
    }
}
impl ApprovalContinuationClaim {
    /// Generated worker identity/lease do not change semantic retry identity.
    /// # Errors
    /// Rejects unbounded identity or a lease outside the supported 1..=900000 ms range.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        for value in [&self.id.0, &self.continuation.id.0] {
            id(value)?;
        }
        identity(&self.session_id.0, 512)?;
        identity(&self.run_id.0, 128)?;
        identity(&self.owner, 512)?;
        if self.lease_ms == 0 || self.lease_ms > 900_000 {
            return Err(Error::InvalidRequest);
        }
        fingerprint(&(
            "crabber.approval.claim.v1",
            &self.session_id,
            &self.run_id,
            self.expected_revision,
            &self.continuation,
            &self.expected_plan,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{ApprovalError, MemoryStore};
    use crabber_core::ManualClock;

    #[tokio::test]
    async fn missing_or_zero_turn_budget_is_not_legacy_zero() {
        let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = MemoryStore::with_clock(clock.clone());
        let fixture = crate::approval_contract::fixture::staged(&store, &clock).await;
        let valid = fixture.batch;
        assert!(
            valid
                .fingerprint(&fixture.run.session_id, &fixture.run.id)
                .is_ok()
        );
        for count in [None, Some(0)] {
            let mut invalid = valid.clone();
            invalid.turns_used = count;
            assert_eq!(
                invalid.fingerprint(&fixture.run.session_id, &fixture.run.id),
                Err(ApprovalError::InvalidRequest)
            );
        }
        let mut legacy = serde_json::to_value(&valid).unwrap();
        legacy.as_object_mut().unwrap().remove("turns_used");
        let legacy: crate::ApprovalSuspension = serde_json::from_value(legacy).unwrap();
        assert_eq!(legacy.turns_used, None);
        assert_eq!(
            legacy.fingerprint(&fixture.run.session_id, &fixture.run.id),
            Err(ApprovalError::InvalidRequest)
        );
        let mut changed = valid.clone();
        changed.turns_used = Some(2);
        assert_ne!(
            changed
                .fingerprint(&fixture.run.session_id, &fixture.run.id)
                .unwrap(),
            valid
                .fingerprint(&fixture.run.session_id, &fixture.run.id)
                .unwrap()
        );
    }
}
