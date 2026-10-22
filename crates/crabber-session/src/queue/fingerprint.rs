use super::{
    Duration, InputFingerprint, QueueAction, QueueMutation, StoreError, encode, identifier,
    invalid, scope,
};
impl QueueMutation {
    /// Independently validate and domain-separate every original control.
    /// # Errors
    /// Rejects malformed or oversized private input without echoing it.
    pub fn fingerprint(&self) -> Result<InputFingerprint, StoreError> {
        scope(&self.session_id, &self.id)?;
        match &self.action {
            QueueAction::Enqueue { queue_id, input } => {
                identifier(&queue_id.0)?;
                if input.request.session_id != self.session_id
                    || input.input.expected_revision() != self.expected_revision
                    || !matches!(
                        input.input.admission_kind(),
                        crabber_core::AdmissionKind::Ordinary
                    )
                    || !input.input.frontend_tools().is_empty()
                {
                    return Err(invalid());
                }
                input.request.selection_policy.validate()?;
                if !input.request.text.is_empty()
                    && input.input.legacy_text() != Some(&input.request.text)
                {
                    return Err(invalid());
                }
                if input.request.workspace_id.len() > 512 || input.request.directory.len() > 16384 {
                    return Err(invalid());
                }
            }
            QueueAction::Claim { owner, lease } | QueueAction::Admit { owner, lease, .. } => {
                identifier(owner)?;
                if lease.is_zero() || *lease > Duration::from_secs(3600) {
                    return Err(invalid());
                }
            }
            QueueAction::Prepare { prepared, .. } => {
                prepared.binding.validate_keyed(&prepared.keyed)?;
            }
            QueueAction::Cancel { queue_id } => identifier(&queue_id.0)?,
        }
        // Queue intent has no reflected host profile until preparation wins.
        // Explicit selection and the caller's opaque behavior claim are intent.
        let value = if let QueueAction::Enqueue { queue_id, input } = &self.action {
            let selection = if input.request.selection_policy.is_explicit() {
                serde_json::json!([
                    "explicit",
                    input.request.provider_id,
                    input.request.model_id
                ])
            } else {
                serde_json::json!(["session_defaults"])
            };
            let mut intent = serde_json::json!([
                "crabber.future-queue.enqueue.v1",
                self.session_id,
                self.id,
                self.expected_revision,
                queue_id,
                input.request.workspace_id,
                input.request.directory,
                input.input,
                input.admission,
                selection
            ]);
            // Preserve historical default-control fingerprints while binding
            // every non-default control to the retained enqueue intent.
            if input.request.model_controls != crabber_core::ModelControls::default() {
                intent
                    .as_array_mut()
                    .ok_or_else(invalid)?
                    .push(serde_json::json!(input.request.model_controls));
            }
            intent
        } else {
            serde_json::json!(["crabber.future-queue.control.v1", self])
        };
        encode(self)?;
        encode(&value)?;
        InputFingerprint::new(crate::admission_config_hash(&value))
    }
}
