//! Metadata-only keyed reconciliation, before contextual owner acquisition.
use super::{Orchestrator, Request, RuntimeError};
use crabber_core::{AdmissionOptions, AdmissionReceipt, Run, RunStatus, Session, SessionContext};
use crabber_extension::{ComponentIdentity, RunPlan, compute_fingerprint};
use crabber_session::{
    AdmissionExecutionError, AdmissionExecutionRecord, AdmissionExecutionState, AdmitRequest,
    KeyedAdmitRequest, StoreError,
};
use serde_json::json;
use std::time::Duration;

const REGISTRATION_COMPONENT: &str = "contract:crabber/registration-profile.v1";
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    declaration: crabber_core::InputFingerprint,
    base_plan: crabber_core::InputFingerprint,
}
pub(super) struct Retained {
    pub session: Session,
    pub receipt: AdmissionReceipt,
    pub record: AdmissionExecutionRecord,
    pub run: Run,
    pub binding: crabber_session::ExecutionBindingCapsule,
}
impl Orchestrator {
    /// Verify an existing request without initialization or execution authority.
    /// `None` does not rule out an admission that commits concurrently.
    /// # Errors
    /// Rejects changed semantics, identity, unavailable evidence or storage errors.
    pub async fn replay_admission(
        &self,
        request: &Request,
        options: &AdmissionOptions,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        Ok(self
            .retained_admission(request, options)
            .await?
            .map(|r| r.receipt))
    }
    /// Metadata-only recovery preflight. Started/terminal records replay;
    /// eligible expired Unstarted records return None for explicit restoration.
    /// # Errors
    /// Missing evidence, live ownership or changed semantics grant no execution.
    pub async fn replay_started_admission(
        &self,
        request: &Request,
        options: &AdmissionOptions,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        let retained = self
            .retained_admission(request, options)
            .await?
            .ok_or(StoreError::NotFound)?;
        self.recovery_replay(&retained)
    }
    pub(super) fn recovery_replay(
        &self,
        retained: &Retained,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        if retained.run.status.is_terminal()
            || retained.record.state == AdmissionExecutionState::Started
        {
            return Ok(Some(retained.receipt.clone()));
        }
        if retained.run.status != RunStatus::Running {
            return Err(AdmissionExecutionError::AlreadyTerminal.into());
        }
        if retained.run.lease_until > self.clock.now() {
            return Err(AdmissionExecutionError::LiveLease.into());
        }
        Ok(None)
    }
    pub(crate) async fn acquire_execution_plan(
        &self,
        context: &SessionContext,
    ) -> Result<RunPlan, RuntimeError> {
        let declaration = self
            .plan_provider
            .registration_fingerprint(context)
            .map_err(RuntimeError::ExtensionPlan)?;
        let plan = self
            .plan_provider
            .acquire_plan_in(context)
            .await
            .map_err(RuntimeError::ExtensionPlan)?;
        Self::bind_plan_registration(plan, declaration)
    }
    pub(crate) fn bind_plan_registration(
        mut plan: RunPlan,
        declaration: crabber_core::InputFingerprint,
    ) -> Result<RunPlan, RuntimeError> {
        // This is a library contract component in the existing component list;
        // the retained capsule wire format and admission digest remain unchanged.
        if plan
            .components
            .iter()
            .any(|c| c.id == REGISTRATION_COMPONENT)
        {
            return Err(RuntimeError::ExtensionPlan(
                crabber_extension::ExtensionError::RegistrationDescriptorUnavailable,
            ));
        }
        let registration = Registration {
            declaration,
            base_plan: crabber_core::InputFingerprint::new(plan.fingerprint().to_string())
                .map_err(|_| StoreError::AdmissionConflict)?,
        };
        plan.components.push(ComponentIdentity {
            id: REGISTRATION_COMPONENT.into(),
            version: serde_json::to_string(&registration)
                .map_err(|_| StoreError::AdmissionConflict)?,
        });
        plan.fingerprint = compute_fingerprint(&plan.components);
        Ok(plan)
    }
    pub(super) async fn retained_admission(
        &self,
        request: &Request,
        options: &AdmissionOptions,
    ) -> Result<Option<Retained>, RuntimeError> {
        let session_id = request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        let context = SessionContext::new(
            session_id.clone(),
            request.workspace_id.clone(),
            request.directory.clone(),
        );
        self.validate_context(&context).await?;
        if options.behavior_fingerprint != self.behavior_fingerprint {
            return Err(StoreError::AdmissionConflict.into());
        }
        let Some(receipt) = self
            .store
            .lookup_admission(session_id, &options.key)
            .await?
        else {
            return Ok(None);
        };
        let session = self
            .store
            .get_session(session_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        session.ensure_identity(&request.workspace_id, &request.directory)?;
        let record = self
            .store
            .load_admission_execution(session_id, &options.key)
            .await?
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        let run = self
            .store
            .get_run(&receipt.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let binding = self.stored_binding(session_id, &receipt.run_id).await?;
        if binding.input_envelope != record.capsule.input_envelope
            || binding.request != record.capsule.request
            || binding.runtime_semantics != record.capsule.runtime_semantics
            || binding.behavior_fingerprint != record.capsule.behavior_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        let request = Self::original_default_request(request, &binding)?;
        let request = &request;
        self.verify_binding_request(request, session_id, &binding)?;
        self.verify_declared_provider(&binding)?;
        self.verify_retained(request, options, &receipt, &record, &run, &context)?;
        Ok(Some(Retained {
            session,
            receipt,
            record,
            run,
            binding,
        }))
    }
    fn verify_retained(
        &self,
        request: &Request,
        options: &AdmissionOptions,
        receipt: &AdmissionReceipt,
        record: &AdmissionExecutionRecord,
        run: &Run,
        context: &SessionContext,
    ) -> Result<(), RuntimeError> {
        if receipt.semantic_digest_version != 2
            || record.receipt != *receipt
            || record.key != options.key
            || record.capsule.request != request.admission_data()?
            || run.id != receipt.run_id
            || run.session_id != *context.session_id()
            || record.capsule.config_hash != run.config_hash
            || record.capsule.plan_fingerprint != run.plan_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        // Recompute original request semantics; generated IDs/time/owner are
        // excluded by the canonical typed digest, just as on a fresh submission.
        let input = record
            .capsule
            .input_envelope
            .clone()
            .ok_or(AdmissionExecutionError::Unsupported)?;
        let mut messages = input.materialize_messages(context.session_id(), self.clock.now())?;
        let user = messages.remove(0);
        let keyed = KeyedAdmitRequest {
            request: AdmitRequest {
                input_envelope: Some(input.clone()),
                additional_messages: messages,
                expected_revision: Some(input.expected_revision()),
                session_id: request.session_id.clone(),
                workspace_id: request.workspace_id.clone(),
                directory: request.directory.clone(),
                title: request.title.clone(),
                user_message: user,
                config_hash: run.config_hash.clone(),
                plan_fingerprint: run.plan_fingerprint.clone(),
                owner: run.owner.clone(),
                lease: Duration::from_secs(30),
            },
            options: options.clone(),
            execution: Some(record.capsule.clone()),
        };
        record.capsule.validate(&keyed)?;
        self.verify_configuration(record, context)
    }
    fn verify_configuration(
        &self,
        record: &AdmissionExecutionRecord,
        context: &SessionContext,
    ) -> Result<(), RuntimeError> {
        let semantics = record
            .capsule
            .runtime_semantics
            .as_array()
            .ok_or(StoreError::AdmissionConflict)?;
        if semantics.get(5) != Some(&json!(format!("{:?}", self.execution_mode)))
            || semantics.get(6) != Some(&json!(self.compaction.trigger_ratio))
            || semantics.get(7) != Some(&json!(self.compaction.keep_tail_messages))
            || semantics.get(8) != Some(&json!(self.max_turns))
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        let components: Vec<ComponentIdentity> = serde_json::from_value(
            semantics
                .get(12)
                .cloned()
                .ok_or(AdmissionExecutionError::MissingEvidence)?,
        )
        .map_err(|_| AdmissionExecutionError::MissingEvidence)?;
        let mut declarations = components.iter().filter(|c| c.id == REGISTRATION_COMPONENT);
        let declaration = declarations
            .next()
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        if declarations.next().is_some() {
            return Err(AdmissionExecutionError::SemanticConflict.into());
        }
        let current = self
            .plan_provider
            .registration_fingerprint(context)
            .map_err(RuntimeError::ExtensionPlan)?;
        let registration: Registration = serde_json::from_str(&declaration.version)
            .map_err(|_| AdmissionExecutionError::MissingEvidence)?;
        if registration.declaration != current
            || compute_fingerprint(&components).to_string() != record.capsule.plan_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        Ok(())
    }
}
