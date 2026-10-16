//! Durable preparation capacity preserves original input and revision identity.
use super::{Orchestrator, Request, RuntimeError, SessionId};
use crabber_core::{SessionOwnerId, SessionOwnerOperationId};
use crabber_session::StoreError;
use crabber_session::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerBinding, SessionOwnerCommand,
    SessionOwnerKind, SessionOwnerOutcome,
    preparation::{
        JoinedPreparation, PreparationCompletion, PreparationGrant, PreparationKey,
        PreparationPurpose, PreparedOperation, ReservePreparation,
    },
};

impl Orchestrator {
    /// Original bounded correlation only; no factories, current owner or grant.
    /// # Errors
    /// Rejects unsupported adapters and corrupt immutable preparation evidence.
    pub async fn lookup_preparation_operation(
        &self,
        session: &SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.store.lookup_preparation(session, key).await
    }
    /// Refuse fresh dispatch while the original preparation needs reconciliation.
    /// This path reads immutable correlation only, including after host close.
    /// # Errors
    /// Returns the original descriptor without acquiring any replacement grant.
    pub async fn reject_pending_preparation(
        &self,
        session: &SessionId,
        key: &PreparationKey,
    ) -> Result<(), RuntimeError> {
        if !self.store.supports_preparation_protocol() {
            return Err(StoreError::HostUnsupported.into());
        }
        if let Some(original) = self.lookup_preparation_operation(session, key).await? {
            return Err(incomplete(
                original,
                RuntimeError::PreparationRecoveryRequired,
            ));
        }
        Ok(())
    }
    pub(super) async fn begin_durable_preparation(
        &self,
        request: &mut Request,
        session: &SessionId,
        purpose: PreparationPurpose,
    ) -> Result<PreparationGrant, RuntimeError> {
        self.begin_preparation_at(
            request,
            session,
            PreparationSpec {
                purpose,
                key: PreparationKey::Unkeyed(crabber_core::ControlId::new()),
                revision: None,
                options: None,
                intent: None,
                source: None,
            },
        )
        .await
    }
    pub(super) async fn begin_keyed_preparation(
        &self,
        request: &mut Request,
        session: &SessionId,
        options: crabber_core::AdmissionOptions,
    ) -> Result<PreparationGrant, RuntimeError> {
        self.begin_preparation_at(
            request,
            session,
            PreparationSpec {
                purpose: PreparationPurpose::OrdinaryAdmission,
                key: PreparationKey::Admission(options.key.clone()),
                revision: None,
                options: Some(options),
                intent: None,
                source: None,
            },
        )
        .await
    }
    pub(super) async fn begin_control_preparation(
        &self,
        request: &mut Request,
        session: &SessionId,
        spec: PreparationSpec,
    ) -> Result<PreparationGrant, RuntimeError> {
        self.begin_preparation_at(request, session, spec).await
    }
    async fn begin_preparation_at(
        &self,
        request: &mut Request,
        session: &SessionId,
        spec: PreparationSpec,
    ) -> Result<PreparationGrant, RuntimeError> {
        if !self.store.supports_preparation_protocol() {
            return Err(StoreError::HostUnsupported.into());
        }
        if request.max_output_tokens == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "max output tokens must be positive",
            ));
        }
        let input = self.capture_input(request, session).await?;
        if request
            .captured_revision
            .is_some_and(|value| value != input.expected_revision())
        {
            return Err(StoreError::AdmissionRevisionConflict.into());
        }
        request.captured_revision = Some(input.expected_revision());
        let reservation = ReservePreparation {
            key: spec.key.clone(),
            source: spec.source,
            owner: ReserveSessionOwner {
                id: SessionOwnerOperationId::new(),
                binding: SessionOwnerBinding {
                    session_id: session.clone(),
                    workspace_id: request.workspace_id.clone(),
                    directory: request.directory.clone(),
                    id: SessionOwnerId::new(),
                    kind: SessionOwnerKind::Preparation,
                    component_id: "crabber.runtime.preparation.v1".into(),
                    behavior_fingerprint: self.behavior_fingerprint.clone(),
                    resource_contract: None,
                    parent: None,
                },
                owner: crabber_core::RunId::new().to_string(),
                lease_ms: 30_000,
            },
            expected_revision: spec.revision.unwrap_or(input.expected_revision()),
            purpose: spec.purpose,
            intent: match spec.intent {
                Some(intent) => intent,
                None => crabber_session::session_owner::preparation::admission_intent(
                    &request.admission_data_for(session)?,
                    &input,
                    spec.options.as_ref(),
                )?,
            },
        }
        .bind_key(spec.key)?;
        let context = crabber_core::SessionContext::new(
            session.clone(),
            request.workspace_id.clone(),
            request.directory.clone(),
        );
        self.plan_provider
            .check_preparation_context(&context)
            .map_err(RuntimeError::ExtensionPlan)?;
        let descriptor = reservation.reconciliation()?;
        let reserved = self.store.reserve_preparation(reservation.clone()).await;
        let authority = match reserved {
            Ok(SessionOwnerOutcome::Applied {
                authority: Some(authority),
                ..
            }) => authority,
            Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome) => {
                return Err(incomplete(
                    descriptor,
                    crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
                ));
            }
            Err(error) => return Err(owner_error(error)),
            _ => {
                return Err(incomplete(
                    descriptor,
                    StoreError::AdmissionRecoveryRequired.into(),
                ));
            }
        };
        self.begin_preparation_owner(&reservation, &authority)
            .await
            .map_err(|error| incomplete(descriptor.clone(), error))?;
        self.ensure_preparation_not_cancelled(&authority)
            .await
            .map_err(|error| incomplete(descriptor, error.into()))?;
        Ok(PreparationGrant::from_fresh_begin(reservation, authority)?)
    }
    async fn begin_preparation_owner(
        &self,
        reservation: &ReservePreparation,
        authority: &crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<(), RuntimeError> {
        let begin = MutateSessionOwner {
            id: reservation.reconciliation()?.begin_id,
            session_id: reservation.owner.binding.session_id.clone(),
            target: authority.target().clone(),
            command: SessionOwnerCommand::Begin,
        };
        match self
            .store
            .mutate_session_owner(begin, Some(authority.clone()))
            .await
        {
            Ok(SessionOwnerOutcome::Applied { .. }) => Ok(()),
            Ok(_) => Err(StoreError::AdmissionRecoveryRequired.into()),
            Err(error) => Err(error.into()),
        }
    }
    async fn ensure_preparation_not_cancelled(
        &self,
        authority: &crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
        if self.store.supports_session_owner_cancellation() {
            // No factory task or real signal exists yet. Preserve the blocker
            // without inventing Observe or Join evidence.
            if self
                .store
                .pending_session_owner_cancel(authority)
                .await?
                .is_some()
            {
                return Err(crabber_session::session_owner::SessionOwnerError::CancellationPending);
            }
        }
        Ok(())
    }
    pub(super) async fn finish_durable_preparation<T: Send + 'static>(
        &self,
        grant: PreparationGrant,
        work: impl std::future::Future<Output = Result<(T, PreparedOperation), RuntimeError>>
        + Send
        + 'static,
    ) -> Result<(T, PreparationCompletion), RuntimeError> {
        let descriptor = grant.reconciliation()?;
        let store = self.store.clone();
        let heartbeat_authority = grant.owner_authority().clone();
        let stop = tokio_util::sync::CancellationToken::new();
        let child_stop = stop.clone();
        let heartbeat = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    () = child_stop.cancelled() => break,
                    _ = interval.tick() => {
                        let renewal = MutateSessionOwner {
                            id: SessionOwnerOperationId::new(),
                            session_id: heartbeat_authority.session_id().clone(),
                            target: heartbeat_authority.target().clone(),
                            command: SessionOwnerCommand::Renew { lease_ms: 30_000 },
                        };
                        if store.mutate_session_owner(renewal, Some(heartbeat_authority.clone())).await.is_err() { break; }
                    }
                }
            }
        });
        let task = tokio::spawn(async move {
            let stop_guard = stop.drop_guard();
            let result = work.await;
            drop(stop_guard);
            let _ = heartbeat.await;
            result
        });
        let (plan, joined) = match JoinedPreparation::observe(grant, task).await {
            Ok(Ok(prepared)) => prepared,
            Ok(Err(error)) => return Err(incomplete(descriptor, error)),
            Err(_) => return Err(incomplete(descriptor, RuntimeError::TaskStopped)),
        };
        let result = self.store.complete_preparation(joined).await;
        match result {
            Ok(completed) => Ok((plan, completed)),
            Err(error) => {
                drop(plan);
                Err(incomplete(descriptor, error.into()))
            }
        }
    }
}
impl Request {
    fn admission_data_for(
        &self,
        session: &SessionId,
    ) -> Result<crabber_session::AdmissionRequestData, StoreError> {
        let mut request = self.clone();
        request.session_id = Some(session.clone());
        request.admission_data()
    }
}

fn owner_error(error: crabber_session::session_owner::SessionOwnerError) -> RuntimeError {
    match error {
        crabber_session::session_owner::SessionOwnerError::ApprovalSource(error) => error.into(),
        crabber_session::session_owner::SessionOwnerError::ToolSource(error) => error.into(),
        crabber_session::session_owner::SessionOwnerError::Busy => RuntimeError::SessionBusy,
        crabber_session::session_owner::SessionOwnerError::IdentityMismatch => {
            StoreError::SessionIdentityMismatch.into()
        }
        crabber_session::session_owner::SessionOwnerError::RevisionConflict => {
            StoreError::AdmissionRevisionConflict.into()
        }
        error => error.into(),
    }
}

fn incomplete(
    preparation: crabber_session::session_owner::preparation::PreparationReconciliation,
    source: RuntimeError,
) -> RuntimeError {
    RuntimeError::PreparationIncomplete {
        preparation: Box::new(preparation),
        source: Box::new(source),
    }
}

impl RuntimeError {
    /// Native/business classification retained under preparation correlation.
    #[must_use]
    pub fn operation_error(&self) -> &Self {
        match self {
            Self::PreparationIncomplete { source, .. } => source.operation_error(),
            error => error,
        }
    }
    /// Safe exact-scope correlation for an owner requiring explicit progress.
    /// This metadata never grants a Join or execution right.
    #[must_use]
    pub fn preparation_reconciliation(
        &self,
    ) -> Option<&crabber_session::session_owner::preparation::PreparationReconciliation> {
        match self {
            Self::PreparationIncomplete { preparation, .. } => Some(preparation),
            _ => None,
        }
    }
}

impl From<crabber_session::session_owner::preparation::PreparationCommitError> for RuntimeError {
    fn from(error: crabber_session::session_owner::preparation::PreparationCommitError) -> Self {
        use crabber_session::session_owner::preparation::PreparationCommitError;
        match error {
            PreparationCommitError::Store(error) => error.into(),
            PreparationCommitError::Owner(error) => error.into(),
            PreparationCommitError::Approval(error) => error.into(),
            PreparationCommitError::ToolContinuation(error) => error.into(),
            PreparationCommitError::Admission(error) => error.into(),
        }
    }
}

pub(super) struct PreparationSpec {
    pub purpose: PreparationPurpose,
    pub key: PreparationKey,
    pub revision: Option<crabber_core::SessionRevision>,
    pub options: Option<crabber_core::AdmissionOptions>,
    pub intent: Option<crabber_core::InputFingerprint>,
    pub source: Option<crabber_session::session_owner::preparation::PreparationSource>,
}

#[path = "preparation/resume.rs"]
mod resume;
