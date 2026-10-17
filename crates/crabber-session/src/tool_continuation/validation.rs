use super::canonical::{Error, digest, identity};
use super::{
    InputFingerprint, RunId, SessionId, ToolContinuationClaim, ToolContinuationSuspension,
    ToolInteractionAnswer, ToolInteractionReply,
};
use std::collections::BTreeSet;
impl ToolContinuationSuspension {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub(crate) fn require_frontend_support(&self, supported: bool) -> Result<(), Error> {
        if !supported && !self.frontend.is_empty() {
            return Err(Error::Unsupported);
        }
        Ok(())
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for text in [
            &self.id.0,
            &self.attempt.id.0,
            &self.next_attempt.id.0,
            &self.continuation.id.0,
        ] {
            identity(text, 128)?;
        }
        if self.attempt.generation.next().ok() != Some(self.next_attempt.generation)
            || self.attempt.id == self.next_attempt.id
            || self.turns_used == 0
            || self.calls.is_empty()
            || self.calls.len() > 64
            || (self.phases.is_empty() && self.frontend.is_empty())
            || self.phases.len() > 64
            || self.frontend.len() > 64
        {
            return Err(Error::InvalidRequest);
        }
        if let Some(turn) = &self.source_turn_id {
            identity(&turn.0, 512)?;
        }
        let mut calls = BTreeSet::new();
        let mut targets = BTreeSet::new();
        let mut phases = BTreeSet::new();
        let mut questions = BTreeSet::new();
        for call in &self.calls {
            identity(&call.target.call_id.0, 512)?;
            identity(&call.target.id.0, 128)?;
            if !calls.insert(&call.target.call_id)
                || !targets.insert(&call.target.id)
                || call.target.attempt != self.next_attempt
                || matches!(call.decision, crate::CallApproval::Ask { .. })
            {
                return Err(Error::InvalidRequest);
            }
        }
        let mut count = 0;
        for phase in &self.phases {
            phase
                .identity
                .validate()
                .map_err(|_| Error::InvalidRequest)?;
            if !phase.identity.accepts(&phase.checkpoint) {
                return Err(Error::SchemaMismatch);
            }
            if phase.previous.attempt != self.attempt
                || phase.next.attempt != self.next_attempt
                || phase.previous.call_id != phase.next.call_id
                || phase.previous.id == phase.next.id
                || phase.previous.generation.next().ok() != Some(phase.next.generation)
                || !phases.insert(&phase.previous.call_id)
                || phase.interactions.is_empty()
                || phase.interactions.len() > 64
            {
                return Err(Error::InvalidRequest);
            }
            if !self
                .calls
                .iter()
                .any(|c| c.target == phase.next && c.call_binding == phase.call_binding)
            {
                return Err(Error::InvalidRequest);
            }
            let mut names = BTreeSet::new();
            for question in &phase.interactions {
                identity(&question.id.0, 128)?;
                if !questions.insert(&question.id) || !names.insert(question.description.name()) {
                    return Err(Error::InvalidRequest);
                }
                jsonschema::validator_for(question.description.response_schema())
                    .map_err(|_| Error::InvalidRequest)?;
                count += 1;
            }
        }
        for frontend in &self.frontend {
            frontend.validate()?;
            if frontend.target.attempt != self.next_attempt
                || !phases.insert(&frontend.target.call_id)
                || !self.calls.iter().any(|call| {
                    call.target == frontend.target
                        && call.call_binding == frontend.call_binding
                        && matches!(call.decision, crate::CallApproval::NotRequired)
                })
            {
                return Err(Error::InvalidRequest);
            }
        }
        if count + self.frontend.len() > 64 {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
    /// Canonical original suspension semantics, including private checkpoint,
    /// every required ID, source identity, versions and captured revision.
    /// # Errors
    /// Rejects malformed targets, bounds or schema declarations.
    pub fn fingerprint(&self, session: &SessionId, run: &RunId) -> Result<InputFingerprint, Error> {
        self.validate()?;
        identity(&session.0, 512)?;
        identity(&run.0, 128)?;
        let domain = if self.frontend.is_empty() {
            "crabber.tool-continuation.suspend.v1"
        } else {
            "crabber.tool-continuation.suspend.v2"
        };
        digest(&(domain, session, run, self))
    }
}
impl ToolInteractionAnswer {
    /// # Errors
    /// Rejects unbounded original answer or host authority assertions.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        for text in [&self.id.0, &self.continuation.id.0, &self.interaction_id.0] {
            identity(text, 128)?;
        }
        identity(&self.session_id.0, 512)?;
        identity(&self.run_id.0, 128)?;
        identity(&self.resolver.resolver_id, 512)?;
        identity(&self.resolver.authority_claim, 4096)?;
        if let ToolInteractionReply::Answer { value } = &self.reply {
            crabber_core::AcceptedToolResponse::validate_value(value)
                .map_err(|_| Error::InvalidRequest)?;
            crate::ensure_storable(value).map_err(|_| Error::InvalidRequest)?;
            if crate::snapshot::encoded_bytes(value, 65536).is_none() {
                return Err(Error::InvalidRequest);
            }
        }
        digest(&("crabber.tool-continuation.answer.v1", self))
    }
}
impl ToolContinuationClaim {
    /// Worker bookkeeping is excluded; replay never reissues native authority.
    /// # Errors
    /// Rejects invalid identities and lease declarations.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        identity(&self.owner, 512)?;
        if self.lease_ms == 0 || self.lease_ms > 900_000 {
            return Err(Error::InvalidRequest);
        }
        super::canonical::claim_fingerprint(
            &self.id,
            &self.session_id,
            &self.run_id,
            &self.continuation,
            self.expected_revision,
            &self.expected_plan,
        )
    }
}
