use super::{
    Admission, AdmissionExecutionCapsule, AdmissionExecutionError, AdmissionExecutionState,
    AdmissionKey, AdmissionOptions, AdmissionReceipt, AdmitRequest, Arc,
    ClaimUnstartedAdmissionRequest, ContextObserver, Duration, InitialEntry, KeyedAdmitOutcome,
    KeyedAdmitRequest, Orchestrator, Request, RunFence, RunHandle, RunId, RunPlan, RunStatus,
    RuntimeError, SessionContext, SessionId, StoreError, TraceContext, Value, admission,
    admission_config_hash, frontend, json, preparation,
};
use crate::telemetry_targets::targets;
use tracing::Instrument;

impl Orchestrator {
    /// Admits a run and starts its turn loop in the background.
    ///
    /// # Errors
    ///
    /// Returns `SessionBusy` for a session with an active run, or a plan/store error.
    pub async fn start(&self, request: Request) -> Result<RunHandle, RuntimeError> {
        self.start_with_context(request, None).await
    }

    /// Starts a run with validated transport metadata, excluded from its semantic fingerprint.
    /// # Errors
    /// Once polled, preparation/admission remain owned if this waiter is dropped.
    /// Returns the same admission errors as `start`.
    pub async fn start_with_context(
        &self,
        request: Request,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            return Err(refusal);
        }
        let session_id = request.session_id.clone().unwrap_or_default();
        self.own_preparation(session_id.clone(), move |runtime| async move {
            runtime.start_admission(request, session_id, context).await
        })
        .await
    }

    async fn start_admission(
        &self,
        request: Request,
        session_id: SessionId,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let session = session_id.clone();
        let span = tracing::info_span!(target: targets::RUNTIME, parent: &self.trace_parent, "crabber.admission", "session.id" = %session, keyed = false, outcome = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
        let result = Box::pin(self.start_admission_inner(request, session_id, context))
            .instrument(span.clone())
            .await;
        super::telemetry::record_admission_result(&span, result.as_ref().map(|_| "started"));
        result
    }
    async fn start_admission_inner(
        &self,
        request: Request,
        session_id: SessionId,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let runtime = self.with_context(context);
        let mut request = runtime.resolve_session_defaults(request).await?;
        let grant = runtime
            .begin_durable_preparation(
                &mut request,
                &session_id,
                crabber_session::session_owner::preparation::PreparationPurpose::OrdinaryAdmission,
            )
            .await?;
        let prepared_runtime = runtime.clone();
        let prepared_request = request.clone();
        let owned = grant.retains_resources();
        let work = move |scope| async move {
            let prepared_runtime = prepared_runtime.with_preparation_scope(scope);
            let (plan, admission, _) = prepared_runtime
                .prepare_admission_in_session(&prepared_request, session_id)
                .await?;
            let bound = prepared_runtime
                .prebind_owned_provider(&plan, &admission.binding.provider)
                .await?;
            Ok((
                (plan, bound),
                crabber_session::session_owner::preparation::PreparedOperation::Admit(Box::new(
                    admission,
                )),
            ))
        };
        if owned {
            let outcome =
                Box::pin(runtime.finish_initial_preparation(grant, request, work)).await?;
            return match outcome {
                preparation::pending::PendingOutcome::Started { handle, .. } => Ok(handle),
                preparation::pending::PendingOutcome::Replayed(
                    preparation::pending::main::Metadata::Initial(
                        crabber_session::session_owner::owned_scope::InitialAdmissionReplay::Unkeyed(receipt),
                    ),
                ) => Err(RuntimeError::AdmissionAlreadyCommitted(receipt)),
                preparation::pending::PendingOutcome::Replayed(_) => Err(StoreError::AdmissionConflict.into()),
            };
        }
        let ((plan, bound), completion) = runtime.finish_durable_preparation(grant, work).await?;
        let admitted = match completion {
            crabber_session::session_owner::preparation::PreparationCompletion::Admitted(admitted) => admitted,
            crabber_session::session_owner::preparation::PreparationCompletion::AdmissionReplayed(receipt) => return Err(RuntimeError::AdmissionAlreadyCommitted(receipt)),
            _ => return Err(StoreError::AdmissionConflict.into()),
        };
        runtime.spawn_admitted(
            request,
            plan,
            admitted.fence,
            &admitted.session,
            InitialEntry::Unkeyed,
            bound,
        )
    }

    /// Admit with a host-selected stable session ID, or reconcile its original receipt.
    /// # Errors
    /// Rejects missing session ID, semantic conflicts, immutable identity changes,
    /// busy sessions, unsupported stores, or plan/store failures.
    pub async fn start_keyed(
        &self,
        request: Request,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_with_context(request, options, None).await
    }

    /// Keyed admission with optional host identity. Replay never attaches replacement
    /// context or executes the retained receipt; identity is not semantic input.
    /// # Errors
    /// Once polled, preparation/admission remain owned if this waiter is dropped.
    /// Returns the same admission errors as `start_keyed`.
    pub async fn start_keyed_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            return self
                .replay_admission(&request, &options)
                .await?
                .map(Admission::Replayed)
                .ok_or(refusal);
        }
        let session_id = request.session_id.clone().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        self.own_preparation(session_id, move |runtime| async move {
            runtime
                .start_keyed_admission(request, options, context)
                .await
        })
        .await
    }

    async fn start_keyed_admission(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let session = request
            .session_id
            .as_ref()
            .map_or_else(|| "none".into(), ToString::to_string);
        let span = tracing::info_span!(target: targets::RUNTIME, parent: &self.trace_parent, "crabber.admission", "session.id" = %session, keyed = true, outcome = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
        let result = Box::pin(self.start_keyed_admission_inner(request, options, context))
            .instrument(span.clone())
            .await;
        super::telemetry::record_admission_result(
            &span,
            result.as_ref().map(|admission| match admission {
                Admission::Replayed(_) => "replayed",
                Admission::Started { .. } => "started",
            }),
        );
        result
    }
    async fn start_keyed_admission_inner(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if request.session_id.is_none() {
            return Err(StoreError::Validation(
                "keyed admission requires a stable session ID".into(),
            )
            .into());
        }
        if let Some(receipt) = self.replay_admission(&request, &options).await? {
            return Ok(Admission::Replayed(receipt));
        }
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        self.reject_pending_preparation(
            session,
            &crabber_session::session_owner::preparation::PreparationKey::Admission(
                options.key.clone(),
            ),
        )
        .await?;
        let mut request = self.resolve_session_defaults(request).await?;
        let session = request.session_id.clone().ok_or(StoreError::NotFound)?;
        let grant = self
            .begin_keyed_preparation(&mut request, &session, options.clone())
            .await?;
        let prepared_runtime = self.clone();
        let prepared_request = request.clone();
        let prepared_options = options.clone();
        let owned = grant.retains_resources();
        let work = move |scope| async move {
            let prepared_runtime = prepared_runtime.with_preparation_scope(scope);
            let (plan, keyed) = prepared_runtime
                .prepare_keyed_admission(&prepared_request, prepared_options)
                .await?;
            let bound = prepared_runtime
                .prebind_owned_provider(&plan, &keyed.binding.provider)
                .await?;
            Ok((
                (plan, bound),
                crabber_session::session_owner::preparation::PreparedOperation::AdmitKeyed(
                    Box::new(keyed),
                ),
            ))
        };
        if owned {
            let outcome = Box::pin(
                self.with_context(context)
                    .finish_initial_preparation(grant, request, work),
            )
            .await?;
            return match outcome {
                preparation::pending::PendingOutcome::Started { handle, metadata } => {
                    let preparation::pending::main::Metadata::Initial(
                        crabber_session::session_owner::owned_scope::InitialAdmissionReplay::Keyed(
                            receipt,
                        ),
                    ) = metadata
                    else {
                        return Err(StoreError::AdmissionConflict.into());
                    };
                    Ok(Admission::Started { receipt, handle })
                }
                preparation::pending::PendingOutcome::Replayed(
                    preparation::pending::main::Metadata::Initial(
                        crabber_session::session_owner::owned_scope::InitialAdmissionReplay::Keyed(
                            receipt,
                        ),
                    ),
                ) => Ok(Admission::Replayed(receipt)),
                preparation::pending::PendingOutcome::Replayed(_) => {
                    Err(StoreError::AdmissionConflict.into())
                }
            };
        }
        let ((plan, bound), completion) = self.finish_durable_preparation(grant, work).await?;
        let crabber_session::session_owner::preparation::PreparationCompletion::Keyed(outcome) =
            completion
        else {
            return Err(StoreError::AdmissionConflict.into());
        };
        match outcome {
            KeyedAdmitOutcome::Started { receipt, admitted } => Ok(Admission::Started {
                receipt,
                handle: self.with_context(context).spawn_admitted(
                    request,
                    plan,
                    admitted.fence,
                    &admitted.session,
                    InitialEntry::Keyed,
                    bound,
                )?,
            }),
            KeyedAdmitOutcome::Replayed(receipt) => Ok(Admission::Replayed(receipt)),
        }
    }

    /// Completes an expired, durably Unstarted keyed admission with the original
    /// semantics. Exact retries and Started/terminal evidence remain metadata-only.
    /// # Errors
    /// Missing evidence, semantic drift, live/stale owners, unsupported adapters,
    /// and unknown claim responses grant no execution authority.
    pub async fn recover_admission(
        &self,
        request: Request,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.recover_admission_with_context(request, options, None)
            .await
    }

    /// Recovers with fresh transport metadata, excluded from retained authority.
    /// # Errors
    /// Once polled, preparation/claim remain owned if this waiter is dropped.
    /// Returns the same failures as `recover_admission`.
    pub async fn recover_admission_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            return self
                .replay_started_admission(&request, &options)
                .await?
                .map(Admission::Replayed)
                .ok_or(refusal);
        }
        let session_id = request.session_id.clone().ok_or(StoreError::NotFound)?;
        self.own_preparation(session_id, move |runtime| async move {
            Box::pin(runtime.recover_retained_admission(request, options, context)).await
        })
        .await
    }

    pub(super) async fn recover_retained_admission(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let session = request
            .session_id
            .as_ref()
            .map_or_else(|| "none".into(), ToString::to_string);
        let span = tracing::info_span!(target: targets::RUNTIME, parent: &self.trace_parent, "crabber.admission", "session.id" = %session, keyed = true, outcome = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
        let result = Box::pin(self.recover_retained_admission_inner(request, options, context))
            .instrument(span.clone())
            .await;
        super::telemetry::record_admission_result(
            &span,
            result.as_ref().map(|admission| match admission {
                Admission::Replayed(_) => "replayed",
                Admission::Started { .. } => "started",
            }),
        );
        result
    }
    pub(super) async fn recover_retained_admission_inner(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.check_original_stop()?;
        if let Some(session) = &request.session_id {
            self.validate_context(&SessionContext::new(
                session.clone(),
                request.workspace_id.clone(),
                request.directory.clone(),
            ))
            .await?;
        }
        let retained = self
            .retained_admission(&request, &options)
            .await?
            .ok_or(StoreError::NotFound)?;
        if let Some(receipt) = self.recovery_replay(&retained)? {
            return Ok(Admission::Replayed(receipt));
        }
        if self.store.supports_preparation_protocol() {
            return Box::pin(self.recover_with_preparation(request, options, context, retained))
                .await;
        }
        let admission::Retained {
            session: persisted,
            receipt,
            record,
            run: _,
            binding: original_binding,
        } = retained;
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        let mut captured = Self::original_default_request(&request, &original_binding)?;
        captured
            .input_envelope
            .clone_from(&original_binding.input_envelope);
        let (plan, keyed) = self.prepare_keyed_admission(&captured, options).await?;
        if keyed.binding != original_binding {
            return Err(StoreError::AdmissionConflict.into());
        }
        let keyed = keyed.keyed;
        let capsule = keyed
            .execution
            .as_ref()
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        // Check actual restored instance semantics against the retained capsule
        // before claiming ownership; metadata already checked frozen declarations.
        record.capsule.validate(&keyed)?;
        if receipt.semantic_digest_version != 2
            || record.receipt != receipt
            || record.key != keyed.options.key
            || record.capsule != *capsule
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        let run = self
            .store
            .get_run(&receipt.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        if run.session_id != *session
            || run.config_hash != capsule.config_hash
            || run.plan_fingerprint != capsule.plan_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        if run.status.is_terminal() || record.state == AdmissionExecutionState::Started {
            return Ok(Admission::Replayed(receipt));
        }
        let bound = self
            .restore_provider(&plan, &original_binding.provider)
            .await?;
        let claimed = self
            .store
            .claim_unstarted_admission(ClaimUnstartedAdmissionRequest {
                session_id: session.clone(),
                key: keyed.options.key,
                expected_fence: RunFence {
                    run_id: run.id,
                    claim_token: run.claim_token,
                },
                expected_owner: run.owner,
                fingerprint: capsule.fingerprint.clone(),
                behavior_fingerprint: capsule.behavior_fingerprint.clone(),
                semantic_digest: capsule.semantic_digest.clone(),
                capsule_digest: capsule.digest()?,
                owner: keyed.request.owner,
                lease: keyed.request.lease,
            })
            .await?;
        Ok(Admission::Started {
            receipt: claimed.record.receipt,
            handle: self.with_context(context).spawn_admitted(
                Request::from_execution(&claimed.record.capsule),
                plan,
                claimed.fence,
                &persisted,
                InitialEntry::RecoveredKeyed(None),
                Some(bound),
            )?,
        })
    }

    pub(crate) async fn prepare_keyed_admission(
        &self,
        request: &Request,
        options: AdmissionOptions,
    ) -> Result<(RunPlan, crabber_session::BoundKeyedAdmitRequest), RuntimeError> {
        if options.behavior_fingerprint != self.behavior_fingerprint {
            return Err(StoreError::AdmissionConflict.into());
        }
        let (plan, bound, runtime_semantics) = self.prepare_admission(request).await?;
        let admission = bound.request;
        let mut keyed = KeyedAdmitRequest {
            request: admission,
            options,
            execution: None,
        };
        let capsule = AdmissionExecutionCapsule {
            input_envelope: keyed.request.input_envelope.clone(),
            version: 2,
            request: request.admission_data()?,
            runtime_semantics,
            config_hash: keyed.request.config_hash.clone(),
            plan_fingerprint: keyed.request.plan_fingerprint.clone(),
            fingerprint: keyed.options.fingerprint.clone(),
            behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
            semantic_digest: keyed.options.fingerprint.clone(),
        };
        keyed.execution = Some(capsule);
        let semantic_digest = keyed.semantic_digest()?;
        keyed
            .execution
            .as_mut()
            .ok_or(AdmissionExecutionError::MissingEvidence)?
            .semantic_digest = semantic_digest;
        keyed
            .execution
            .as_ref()
            .ok_or(AdmissionExecutionError::MissingEvidence)?
            .validate(&keyed)?;
        Ok((
            plan,
            crabber_session::BoundKeyedAdmitRequest {
                keyed,
                binding: bound.binding,
            },
        ))
    }

    // A generic store Conflict is not evidence of lease loss. Verify retained
    // authority before classifying it or skipping settlement, and keep the original error.
    pub(super) async fn verified_lease_loss(&self, fence: &RunFence) -> bool {
        self.store
            .get_run(&fence.run_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|run| {
                run.claim_token != fence.claim_token
                    || (run.status != RunStatus::Paused
                        && !run.status.is_terminal()
                        && run.lease_until <= self.clock.now())
            })
    }

    pub(super) fn with_context(&self, context: Option<TraceContext>) -> Self {
        let mut runtime = self.clone();
        let attempt = RunId::new();
        runtime.attempt = Some(super::AttemptIdentity {
            attempt: attempt.clone(),
            context: context.clone(),
            parent: Arc::new(std::sync::Mutex::new(Some(runtime.trace_parent.clone()))),
        });
        runtime.trace_parent = tracing::Span::none();
        runtime.observer = Arc::new(ContextObserver {
            inner: Arc::clone(&self.observer),
            context,
            attempt,
        });
        runtime
    }

    /// Settles orphaned work without resolving providers, preparing plans or
    /// executing extension hooks. Retry the same request after unknown responses.
    /// # Errors
    /// Returns typed eligibility, ownership or durable settlement failures.
    pub async fn abandon(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        self.store.abandon_run(request).await
    }

    /// Reads retained metadata without acquiring a plan or execution lease.
    /// # Errors
    /// Returns store errors; None does not rule out a concurrent commit.
    pub async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        Ok(self.store.lookup_admission(session, key).await?)
    }

    /// Check routing identity before acquiring any contextual plan. Admission
    /// repeats the check in its transaction to close concurrent-create races.
    /// # Errors
    /// Returns identity mismatch or store validation/availability errors.
    pub async fn validate_context(&self, context: &SessionContext) -> Result<(), RuntimeError> {
        crabber_session::ensure_storable_text(&context.session_id().0)?;
        crabber_session::ensure_storable_text(context.workspace_id())?;
        crabber_session::ensure_storable_text(context.directory())?;
        if let Some(session) = self.store.get_session(context.session_id()).await? {
            session.ensure_identity(context.workspace_id(), context.directory())?;
        }
        Ok(())
    }

    pub(crate) async fn prepare_admission(
        &self,
        request: &Request,
    ) -> Result<(RunPlan, crabber_session::BoundAdmitRequest, Value), RuntimeError> {
        self.prepare_admission_in_session(request, request.session_id.clone().unwrap_or_default())
            .await
    }

    async fn prepare_admission_in_session(
        &self,
        request: &Request,
        session_id: SessionId,
    ) -> Result<(RunPlan, crabber_session::BoundAdmitRequest, Value), RuntimeError> {
        if !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(StoreError::HostUnsupported.into());
        }
        if request.max_output_tokens == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "max output tokens must be positive",
            ));
        }
        let now = self.clock.now();
        let context = SessionContext::new(
            session_id.clone(),
            request.workspace_id.clone(),
            request.directory.clone(),
        );
        self.validate_context(&context).await?;
        let input = self.capture_input(request, &session_id).await?;
        let mut messages = input.materialize_messages(&session_id, now)?;
        let first_message = messages.remove(0);
        let plan = self.acquire_execution_plan(&context).await?;
        let plan = frontend::bind(plan, input.frontend_tools())?;
        let plan = Self::bind_selection_policy(plan, &request.selection_policy)?;
        let provider = self.describe_provider(&plan, &request.selection)?;
        let plan = self.bind_plan_provider(plan, &provider, &self.behavior_fingerprint)?;
        // Hash structured, independently observed semantics, never a caller's claim.
        let config = json!([
            "crabber.runtime.admission.v1",
            request.selection.provider_id,
            request.selection.model_id,
            request.system_prompt,
            request.max_output_tokens,
            format!("{:?}", self.execution_mode),
            self.compaction.trigger_ratio,
            self.compaction.keep_tail_messages,
            self.max_turns,
            plan.tools.iter().map(|tool| &tool.info).collect::<Vec<_>>(),
            plan.prompts
                .iter()
                .map(|p| (&p.name, p.order, &p.text))
                .collect::<Vec<_>>(),
            plan.restrictions,
            plan.components,
            plan.guards
                .iter()
                .map(|guard| guard.id())
                .collect::<Vec<_>>(),
            plan.providers
                .iter()
                .map(|provider| {
                    let info = provider.info();
                    (info.id, info.name)
                })
                .collect::<Vec<_>>(),
            request.model_controls
        ]);
        let config = self.freeze_preparation_semantics(config)?;
        let config_hash = admission_config_hash(&config);
        let admission = AdmitRequest {
            input_envelope: Some(input.clone()),
            additional_messages: messages,
            expected_revision: Some(input.expected_revision()),
            session_id: request.session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            user_message: first_message,
            config_hash,
            plan_fingerprint: plan.fingerprint().to_string(),
            owner: RunId::new().to_string(),
            lease: Duration::from_secs(30),
        };
        let mut stable = request.clone();
        stable.session_id = Some(session_id);
        let mut binding = crabber_session::ExecutionBindingCapsule {
            input_fingerprint: None,
            input_envelope: Some(input),
            version: 3,
            provider,
            request: stable.admission_data()?,
            runtime_semantics: config.clone(),
            config_hash: crabber_core::InputFingerprint::new(admission.config_hash.clone())?,
            plan_fingerprint: crabber_core::InputFingerprint::new(
                admission.plan_fingerprint.clone(),
            )?,
            behavior_fingerprint: self.behavior_fingerprint.clone(),
        };
        binding.input_fingerprint = binding.computed_input_fingerprint()?;
        binding.validate(&admission)?;
        Ok((
            plan,
            crabber_session::BoundAdmitRequest {
                request: admission,
                binding,
            },
            config,
        ))
    }
}
