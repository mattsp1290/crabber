use super::{
    InputFingerprint, MAX_CONTROL_BYTES, RunControlAction, RunControlError, RunControlRequest,
    RunControlTarget, unknown, valid_scope,
};
use crabber_core::{AdmissionKind, InputContent, InputState, Role, Run};
use sha2::{Digest, Sha256};
impl RunControlTarget {
    #[must_use]
    pub fn of(run: &Run) -> Self {
        Self {
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            config_hash: run.config_hash.clone(),
            plan_fingerprint: run.plan_fingerprint.clone(),
        }
    }
}
impl RunControlRequest {
    /// Canonical retained semantics, including the caller's revision and input IDs.
    /// # Errors
    /// Rejects unsupported steer shapes, unbounded payloads and invalid bindings.
    pub fn fingerprint(&self) -> Result<InputFingerprint, RunControlError> {
        valid_scope(&self.target.session_id, &self.id)?;
        crate::attempt::validate_identifier(&self.target.run_id.0)
            .map_err(|_| RunControlError::InvalidRequest)?;
        if [&self.target.config_hash, &self.target.plan_fingerprint]
            .iter()
            .any(|value| {
                value.is_empty() || value.len() > 512 || value.chars().any(char::is_control)
            })
        {
            return Err(RunControlError::InvalidRequest);
        }
        if let RunControlAction::Cancel { resolver } = &self.action
            && [
                (&resolver.resolver_id, 512),
                (&resolver.authority_claim, 1024),
            ]
            .iter()
            .any(|(s, max)| s.is_empty() || s.len() > *max || s.chars().any(char::is_control))
        {
            return Err(RunControlError::InvalidRequest);
        }
        if let RunControlAction::Steer { input } = &self.action
            && (input.expected_revision() != self.expected_revision
                || !matches!(input.admission_kind(), AdmissionKind::Ordinary)
                || !input.frontend_tools().is_empty()
                || !input.context().is_empty()
                || !matches!(input.state(), InputState::Absent)
                || input.messages().iter().any(|message| {
                    message.role != Role::User
                        || message
                            .parts
                            .iter()
                            .any(|part| !matches!(part.content, InputContent::Content { .. }))
                }))
        {
            return Err(RunControlError::InvalidRequest);
        }
        let semantics = ("crabber.run-control.accept.v1", self);
        crate::snapshot::encoded_bytes(&semantics, MAX_CONTROL_BYTES)
            .ok_or(RunControlError::InvalidRequest)?;
        crate::ensure_storable(self).map_err(|_| RunControlError::InvalidRequest)?;
        let mut value = serde_json::to_value(semantics).map_err(unknown)?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value).map_err(unknown)?;
        if bytes.len() > MAX_CONTROL_BYTES {
            return Err(RunControlError::InvalidRequest);
        }
        InputFingerprint::new(format!("{:x}", Sha256::digest(bytes))).map_err(unknown)
    }
}
