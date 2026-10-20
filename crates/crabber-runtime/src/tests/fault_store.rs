//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InitialTaskStage {
    CompletePreparation,
    InspectQueue,
    RegisterDriver,
    BeginAttempt,
}

#[derive(Default)]
pub(super) struct SettlementGate {
    pub(super) continuation_source_fault: AtomicUsize,
    pub(super) source_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) source_worker_hold: std::sync::atomic::AtomicBool,
    pub(super) source_hold_absent_lookup: std::sync::atomic::AtomicBool,
    pub(super) source_absent_lookup_entered: Notify,
    pub(super) source_absent_lookup_release: Notify,
    pub(super) source_worker_entered: Notify,
    pub(super) source_worker_release: Notify,
    pub(super) source_workers:
        Mutex<Vec<tokio::task::JoinHandle<Result<(), crabber_session::PublicStateError>>>>,
    // Diagnostics only: task identity is never execution authority.
    pub(super) initial_task_diagnostics: std::sync::atomic::AtomicBool,
    pub(super) grant_payload_protections: AtomicUsize,
    pub(super) foreign_grant_capabilities: Mutex<Option<grant_ack_faults::Capabilities>>,
    pub(super) foreign_grant_ack: Mutex<Option<crabber_session::ToolContinuationClaimOutcome>>,
    pub(super) queue_inspection_disabled: std::sync::atomic::AtomicBool,
    pub(super) initial_task_ids: Mutex<Vec<(InitialTaskStage, Option<tokio::task::Id>)>>,
    pub(super) initial_preparation_session: Mutex<Option<SessionId>>,
    pub(super) preparation_heartbeat: Option<Arc<preparation_resources::heartbeat::Gate>>,
    pub(super) service_register_fault: AtomicUsize,
    pub(super) service_register_calls: AtomicUsize,
    pub(super) service_register_hold: std::sync::atomic::AtomicBool,
    pub(super) service_register_entered: Notify,
    pub(super) service_register_release: Notify,
    pub(super) service_access_panic: std::sync::atomic::AtomicBool,
    pub(super) service_activation_fault: AtomicUsize,
    pub(super) service_activation_hold: std::sync::atomic::AtomicBool,
    pub(super) service_activation_entered: Notify,
    pub(super) service_activation_release: Notify,
    pub(super) service_join_fault: AtomicUsize,
    pub(super) service_join_panic: AtomicUsize,
    pub(super) service_join_bad_termination: std::sync::atomic::AtomicBool,
    pub(super) service_join_bad_binding: std::sync::atomic::AtomicBool,
    pub(super) service_join_calls: AtomicUsize,
    pub(super) service_join_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    pub(super) invocation_poll_panic: std::sync::atomic::AtomicBool,
    pub(super) invocation_poll_entered: Notify,
    pub(super) driver_register_fault: AtomicUsize,
    pub(super) successor_disabled: std::sync::atomic::AtomicBool,
    pub(super) successor_request:
        Mutex<Option<crabber_session::session_owner::driver::RegisterSuccessorExecutionDriver>>,
    pub(super) successor_join_bad: AtomicUsize,
    pub(super) successor_join_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    pub(super) driver_register_request_override:
        Mutex<Option<crabber_session::session_owner::driver::RegisterExecutionDriver>>,
    pub(super) driver_register_captured:
        Mutex<Option<crabber_session::session_owner::driver::ExecutionDriverOutcome>>,
    pub(super) driver_register_hold: std::sync::atomic::AtomicBool,
    pub(super) driver_register_entered: Notify,
    pub(super) driver_register_release: Notify,
    pub(super) driver_activation_fault: AtomicUsize,
    pub(super) driver_join_fault: AtomicUsize,
    pub(super) driver_join_calls: AtomicUsize,
    pub(super) driver_join_bad: AtomicUsize,
    pub(super) driver_join_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    pub(super) driver_poll_panic: std::sync::atomic::AtomicBool,
    pub(super) driver_poll_entered: Notify,
    pub(super) driver_poll_hold: std::sync::atomic::AtomicBool,
    pub(super) driver_poll_release: Notify,
    pub(super) driver_poll_completed: AtomicUsize,
    pub(super) preparation_fault: AtomicUsize,
    pub(super) preparation_hold: std::sync::atomic::AtomicBool,
    pub(super) preparation_entered: Notify,
    pub(super) preparation_release: Notify,
    pub(super) preparation_calls: AtomicUsize,
    pub(super) unsupported_recovery_inspection: std::sync::atomic::AtomicBool,
    pub(super) activation_fault: AtomicUsize,
    pub(super) owned_join_fault: AtomicUsize,
    pub(super) owned_join_calls: AtomicUsize,
    pub(super) cleanup_uncertain_fault: AtomicUsize,
    pub(super) cleanup_uncertain_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
    pub(super) malformed_owned_join: AtomicUsize,
    pub(super) observation: Option<Arc<suspension_observation::ObservationGate>>,
    pub(super) admission: Option<Arc<owned_execution::preparation::AdmissionResponseGate>>,
    pub(super) append: Option<Arc<OperationalAppendGate>>,
    pub(super) settlements: AtomicUsize,
    pub(super) tool_settlements: AtomicUsize,
    pub(super) unfinished_reads: AtomicUsize,
    pub(super) fail_claim: std::sync::atomic::AtomicBool,
    pub(super) fail_settle: std::sync::atomic::AtomicBool,
    pub(super) fail_permission_event: std::sync::atomic::AtomicBool,
    pub(super) terminal_fault: AtomicUsize,
    pub(super) phase_fault: AtomicUsize,
    pub(super) tool_suspension_fault: AtomicUsize,
    pub(super) tool_suspension_calls: AtomicUsize,
    pub(super) tool_suspension_keys: Mutex<Vec<crabber_core::ControlId>>,
    pub(super) checkpoint_override: Mutex<Option<Value>>,
    pub(super) call_arguments_override: Mutex<Option<Value>>,
    pub(super) begin_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) approval_claim_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) approval_recovery_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) approval_recovery_gate: std::sync::atomic::AtomicBool,
    pub(super) approval_recovery_entered: Notify,
    pub(super) approval_recovery_release: Notify,
    pub(super) reserve_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) entered: Mutex<Option<oneshot::Sender<()>>>,
    pub(super) release: Notify,
    pub(super) terminal_reads: AtomicUsize,
    pub(super) terminal_controls: bool,
    pub(super) terminal_control_ack_lost: bool,
    pub(super) terminal_control_seen: Notify,
    pub(super) reject_run_reads: std::sync::atomic::AtomicBool,
    pub(super) session_owner_ack_lost: AtomicUsize,
    pub(super) legacy_cancel_calls: AtomicUsize,
    pub(super) session_owner_cancel_on_begin: AtomicUsize,
    pub(super) owner_observe_ack_lost: std::sync::atomic::AtomicBool,
    pub(super) owner_observe_keys: Mutex<Vec<crabber_core::SessionOwnerOperationId>>,
}

impl SettlementGate {
    pub(super) fn legacy_cancellation(
        &self,
        kind: crabber_session::session_owner::SessionOwnerKind,
        command: &crabber_session::session_owner::SessionOwnerCommand,
    ) -> bool {
        use crabber_session::session_owner::{
            SessionOwnerCommand, SessionOwnerKind, SessionOwnerTermination,
        };
        let cancelled = kind == SessionOwnerKind::Preparation
            && matches!(
                command,
                SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Cancelled
                }
            );
        if cancelled {
            self.legacy_cancel_calls.fetch_add(1, Ordering::SeqCst);
        }
        cancelled
    }
    pub(super) fn malformed_driver_ack(
        &self,
        outcome: &mut crabber_session::session_owner::SessionOwnerOutcome,
    ) {
        use crabber_session::session_owner::{
            SessionOwnerOperation, SessionOwnerOutcome, SessionOwnerState, SessionOwnerTermination,
        };
        match outcome {
            SessionOwnerOutcome::Applied { receipt, .. }
            | SessionOwnerOutcome::Replayed(receipt) => {
                match self.driver_join_bad.load(Ordering::SeqCst) {
                    1 => {
                        receipt.owner.termination = Some(SessionOwnerTermination::Cancelled);
                    }
                    2 => {
                        receipt.owner.binding.component_id = "changed-initial-driver".into();
                    }
                    3 => {
                        receipt.owner.created_at += time::Duration::seconds(1);
                    }
                    4 => {
                        receipt.owner.state = SessionOwnerState::Active;
                    }
                    5 => {
                        receipt.semantic_digest =
                            crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
                    }
                    6 => {
                        receipt.session_id = SessionId::new();
                    }
                    7 => {
                        receipt.operation_id = crabber_core::SessionOwnerOperationId::new();
                    }
                    8 => {
                        receipt.owner.target.id.0 = "changed-initial-owner".into();
                    }
                    9 => {
                        receipt.operation = SessionOwnerOperation::Begin;
                    }
                    _ => {}
                }
            }
        }
    }
}

impl SettlementGate {
    pub(super) fn record_initial_task(&self, stage: InitialTaskStage) {
        if self.initial_task_diagnostics.load(Ordering::SeqCst) {
            self.initial_task_ids
                .lock()
                .unwrap()
                .push((stage, tokio::task::try_id()));
        }
    }
}

pub(super) struct DelayedTerminalStore {
    pub(super) inner: Arc<dyn Store>,
    pub(super) gate: Arc<SettlementGate>,
}

pub(super) struct DelayedTerminalExecution {
    pub(super) inner: Box<dyn ExecutionStore>,
    pub(super) gate: Arc<SettlementGate>,
}

#[async_trait]
impl Store for DelayedTerminalStore {
    async fn public_state(
        &self,
        session: &SessionId,
    ) -> Result<crabber_session::PublicStateSnapshot, crabber_session::PublicStateError> {
        self.inner.public_state(session).await
    }
    async fn lookup_public_state_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::PublicStateReceipt>, crabber_session::PublicStateError>
    {
        let selected = self.inner.lookup_public_state_mutation(session, id).await?;
        if selected.is_none()
            && self
                .gate
                .source_hold_absent_lookup
                .swap(false, Ordering::SeqCst)
        {
            self.gate.source_absent_lookup_entered.notify_one();
            self.gate.source_absent_lookup_release.notified().await;
        }
        Ok(selected)
    }
    async fn lookup_public_source_publication(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::PublicSourcePublication>, crabber_session::PublicStateError>
    {
        self.inner
            .lookup_public_source_publication(session, id)
            .await
    }
    async fn mutate_bound_public_source(
        &self,
        request: crabber_session::PublicSourceMutation,
        authority: crabber_session::PublicSourceAuthority,
    ) -> Result<crabber_session::PublicStateReceipt, crabber_session::PublicStateError> {
        if self.gate.source_worker_hold.swap(false, Ordering::SeqCst) {
            let inner = self.inner.clone();
            let gate = self.gate.clone();
            let (sender, receiver) = oneshot::channel();
            let worker = tokio::spawn(async move {
                gate.source_worker_entered.notify_one();
                gate.source_worker_release.notified().await;
                let result = inner.mutate_bound_public_source(request, authority).await;
                let observed = result.as_ref().map(|_| ()).map_err(Clone::clone);
                let _ = sender.send(result);
                observed
            });
            self.gate.source_workers.lock().unwrap().push(worker);
            return receiver
                .await
                .map_err(|_| crabber_session::PublicStateError::UnknownStoreOutcome)?;
        }
        let receipt = self
            .inner
            .mutate_bound_public_source(request, authority)
            .await?;
        if self.gate.source_ack_lost.swap(false, Ordering::SeqCst) {
            return Err(crabber_session::PublicStateError::UnknownStoreOutcome);
        }
        Ok(receipt)
    }

    fn supports_run_controls(&self) -> bool {
        self.gate.observation.is_some() || self.gate.terminal_controls
    }
    async fn run_control_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<crabber_session::RunControlBinding, crabber_session::RunControlError> {
        self.inner.run_control_binding(session, run).await
    }
    async fn run_controls(
        &self,
        target: &crabber_session::RunControlTarget,
    ) -> Result<crabber_session::RunControlPage, crabber_session::RunControlError> {
        self.inner.run_controls(target).await
    }
    fn supports_preparation_protocol(&self) -> bool {
        true
    }
    async fn reserve_preparation(
        &self,
        request: crabber_session::session_owner::preparation::ReservePreparation,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.reserve_preparation(request).await
    }
    async fn lookup_preparation(
        &self,
        session: &crabber_core::SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_preparation(session, key).await
    }
    fn supports_pending_execution_driver_origin(
        &self,
        origin: crabber_session::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        !self.gate.successor_disabled.load(Ordering::SeqCst)
            && self.inner.supports_pending_execution_driver_origin(origin)
    }
    async fn inspect_pending_approval_preparation(
        &self,
        request: crabber_session::session_owner::preparation::InspectPendingApprovalPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PendingApprovalPreparationMetadata,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if self.gate.preparation_fault.load(Ordering::SeqCst) == 6 {
            return Err(crabber_session::session_owner::SessionOwnerError::Unsupported);
        }
        let mut metadata = self
            .inner
            .inspect_pending_approval_preparation(request)
            .await?;
        if self.gate.preparation_fault.load(Ordering::SeqCst) == 5 {
            metadata.source_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        }
        Ok(metadata)
    }
    async fn inspect_pending_tool_preparation(
        &self,
        request: crabber_session::session_owner::preparation::InspectPendingToolPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PendingToolPreparationMetadata,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if self.gate.preparation_fault.load(Ordering::SeqCst) == 6 {
            return Err(crabber_session::session_owner::SessionOwnerError::Unsupported);
        }
        let mut metadata = self.inner.inspect_pending_tool_preparation(request).await?;
        if self.gate.preparation_fault.load(Ordering::SeqCst) == 5 {
            metadata.source_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        }
        Ok(metadata)
    }
    async fn inspect_prepared_tool_driver(
        &self,
        request: crabber_session::session_owner::preparation::InspectPreparedToolDriver,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparedToolDriverMetadata,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.inspect_prepared_tool_driver(request).await
    }
    async fn inspect_prepared_approval_driver(
        &self,
        request: crabber_session::session_owner::preparation::InspectPreparedApprovalDriver,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparedApprovalDriverMetadata,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.inspect_prepared_approval_driver(request).await
    }
    async fn complete_preparation_with_driver_source(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparedDriverCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        self.gate
            .record_initial_task(InitialTaskStage::CompletePreparation);
        self.gate.preparation_calls.fetch_add(1, Ordering::SeqCst);
        let fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        let mut completed = self
            .inner
            .complete_preparation_with_driver_source(joined)
            .await?;
        if self.gate.preparation_hold.load(Ordering::SeqCst) {
            self.gate.preparation_entered.notify_one();
            self.gate.preparation_release.notified().await;
        }
        if fault == 2 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        if matches!(fault, 4 | 7) {
            use crabber_session::session_owner::preparation::PreparationCompletion as Completed;
            let (digest, fence) = match &mut completed.completion {
                Completed::Approval(crabber_session::ApprovalClaimOutcome::Applied {
                    receipt,
                    grant,
                }) => (&mut receipt.semantic_digest, &mut grant.fence),
                Completed::ToolContinuation(
                    crabber_session::ToolContinuationClaimOutcome::Applied { receipt, grant },
                ) => (&mut receipt.semantic_digest, &mut grant.fence),
                _ => panic!("actual continuation completion"),
            };
            if fault == 4 {
                *digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
            } else {
                fence.claim_token = "wrong actual completion fence".into();
            }
        }
        if let crabber_session::session_owner::preparation::PreparationCompletion::ToolContinuation(outcome) = &mut completed.completion {
            grant_ack_faults::interpose(&self.gate, outcome, fault)?;
        }
        Ok(completed)
    }
    async fn complete_preparation(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        self.gate
            .record_initial_task(InitialTaskStage::CompletePreparation);
        if self.gate.initial_task_diagnostics.load(Ordering::SeqCst) {
            *self.gate.initial_preparation_session.lock().unwrap() = Some(
                joined
                    .observed_grant()
                    .original_reservation()
                    .owner
                    .binding
                    .session_id
                    .clone(),
            );
        }
        self.gate.preparation_calls.fetch_add(1, Ordering::SeqCst);
        let preparation_fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        if preparation_fault == 1 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        if joined.operation_kind() == crabber_session::session_owner::preparation::PreparationOperationKind::ApprovalRecovery
            && self.gate.approval_recovery_gate.load(Ordering::SeqCst)
        {
            self.gate.approval_recovery_entered.notify_one();
            self.gate.approval_recovery_release.notified().await;
        }
        let mut completed = self.inner.complete_preparation(joined).await?;
        if self.gate.preparation_hold.load(Ordering::SeqCst) {
            self.gate.preparation_entered.notify_one();
            self.gate.preparation_release.notified().await;
        }
        if preparation_fault == 3 {
            use crabber_session::session_owner::preparation::{
                AdmissionRecoveryOutcome, ExpiredRunOutcome, PreparationCompletion,
            };
            return Ok(match completed {
                PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Claimed(
                    value,
                )) => PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Replayed(
                    value.record.receipt.clone(),
                )),
                PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed {
                    receipt, ..
                }) => PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Replayed(receipt)),
                _ => panic!("replay adapter is first-two only"),
            });
        }
        if preparation_fault == 2 {
            return Err(
                crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome.into(),
            );
        }
        if let crabber_session::session_owner::preparation::PreparationCompletion::Approval(
            crabber_session::ApprovalClaimOutcome::Applied { receipt, .. },
        ) = &completed
        {
            let lost = match receipt.outcome {
                crabber_session::ApprovalOutcome::Claimed { .. } => {
                    self.gate.approval_claim_ack_lost.load(Ordering::SeqCst)
                }
                crabber_session::ApprovalOutcome::ReclaimedUnstarted { .. } => {
                    self.gate.approval_recovery_ack_lost.load(Ordering::SeqCst)
                }
                _ => false,
            };
            if lost {
                return Err(crabber_session::ApprovalError::UnknownStoreOutcome.into());
            }
        }
        if let Some(gate) = &self.gate.admission {
            use crabber_session::session_owner::preparation::PreparationCompletion;
            match &completed {
                PreparationCompletion::AdmissionRecovery(
                    crabber_session::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(
                        claimed,
                    ),
                ) => {
                    gate.hold(&claimed.run.session_id, &claimed.run.id).await;
                }
                PreparationCompletion::Admitted(admitted) => {
                    gate.hold(&admitted.session.id, &admitted.run.id).await;
                }
                PreparationCompletion::Keyed(crabber_session::KeyedAdmitOutcome::Started {
                    admitted,
                    ..
                }) => gate.hold(&admitted.session.id, &admitted.run.id).await,
                _ => (),
            }
        }
        if let crabber_session::session_owner::preparation::PreparationCompletion::ToolContinuation(outcome) = &mut completed {
            grant_ack_faults::interpose(&self.gate, outcome, preparation_fault)?;
        }
        Ok(completed)
    }

    fn supports_session_service_dependencies(&self) -> bool {
        self.inner.supports_session_service_dependencies()
    }
    async fn register_session_service_dependency(
        &self,
        request: crabber_session::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<
        crabber_session::session_owner::service::SessionServiceDependencyOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .service_register_calls
            .fetch_add(1, Ordering::SeqCst);
        let point = self.gate.service_register_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let result = self
            .inner
            .register_session_service_dependency(request)
            .await?;
        if self.gate.service_register_hold.load(Ordering::SeqCst) {
            self.gate.service_register_entered.notify_one();
            self.gate.service_register_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn check_session_service_dependency_access(
        &self,
        request: crabber_session::session_owner::service::SessionServiceDependencyAccess,
    ) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
        assert!(
            !self.gate.service_access_panic.swap(false, Ordering::SeqCst),
            "service access store future panicked after actual task dispatch"
        );
        self.inner
            .check_session_service_dependency_access(request)
            .await
    }
    fn supports_execution_drivers(&self) -> bool {
        self.inner.supports_execution_drivers()
    }
    fn supports_queue_preparation_admission_inspection(&self) -> bool {
        !self.gate.queue_inspection_disabled.load(Ordering::SeqCst)
            && self.inner.supports_queue_preparation_admission_inspection()
    }
    async fn inspect_queue_preparation_admission(
        &self,
        original: &crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crabber_session::QueueReceipt>,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        self.gate
            .record_initial_task(InitialTaskStage::InspectQueue);
        self.inner
            .inspect_queue_preparation_admission(original)
            .await
    }
    fn supports_owned_queue_preparation_inspection(&self) -> bool {
        self.inner.supports_owned_queue_preparation_inspection()
    }
    async fn inspect_owned_queue_preparation(
        &self,
        context: &crabber_core::SessionContext,
        authority: &crabber_session::QueueAuthority,
        revision: crabber_core::SessionRevision,
    ) -> Result<crabber_session::RetainedQueue, StoreError> {
        self.inner
            .inspect_owned_queue_preparation(context, authority, revision)
            .await
    }
    fn supports_future_queue(&self) -> bool {
        self.inner.supports_future_queue()
    }
    async fn queue_preparation_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<crabber_session::SessionSummary, StoreError> {
        self.inner.queue_preparation_snapshot(id).await
    }
    async fn mutate_queue(
        &self,
        request: crabber_session::QueueMutation,
    ) -> Result<crabber_session::QueueOutcome, StoreError> {
        self.inner.mutate_queue(request).await
    }
    async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::QueueReceipt>, StoreError> {
        self.inner.lookup_queue(session, id).await
    }
    async fn retained_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::QueueId,
    ) -> Result<crabber_session::RetainedQueue, StoreError> {
        self.inner.retained_queue(session, id).await
    }
    fn supports_initial_preparation_admission_inspection(&self) -> bool {
        self.inner
            .supports_initial_preparation_admission_inspection()
    }
    async fn inspect_initial_preparation_admission(
        &self,
        original: &crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationAdmissionReceipt>,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        self.inner
            .inspect_initial_preparation_admission(original)
            .await
    }
    async fn register_execution_driver(
        &self,
        request: crabber_session::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<
        crabber_session::session_owner::driver::ExecutionDriverOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .record_initial_task(InitialTaskStage::RegisterDriver);
        let point = self.gate.driver_register_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let substitute = self
            .gate
            .driver_register_request_override
            .lock()
            .unwrap()
            .take();
        let outcome = self
            .inner
            .register_execution_driver(substitute.clone().unwrap_or(request))
            .await?;
        if substitute.is_some() {
            *self.gate.driver_register_captured.lock().unwrap() = Some(outcome.clone());
        }
        if self.gate.driver_register_hold.load(Ordering::SeqCst) {
            self.gate.driver_register_entered.notify_one();
            self.gate.driver_register_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    fn supports_successor_execution_drivers(&self) -> bool {
        !self.gate.successor_disabled.load(Ordering::SeqCst)
            && self.inner.supports_successor_execution_drivers()
    }
    async fn register_successor_execution_driver(
        &self,
        request: crabber_session::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<
        crabber_session::session_owner::driver::SuccessorExecutionDriverOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .record_initial_task(InitialTaskStage::RegisterDriver);
        *self.gate.successor_request.lock().unwrap() = Some(request.clone());
        let point = self.gate.driver_register_fault.swap(0, Ordering::SeqCst);
        assert_ne!(point, 3, "successor Register store panic before commit");
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let result = self
            .inner
            .register_successor_execution_driver(request.clone())
            .await?;
        assert_ne!(point, 4, "successor Register store panic after commit");
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        if point == 5 {
            return self
                .inner
                .register_successor_execution_driver(request)
                .await;
        }
        Ok(result)
    }
    fn supports_preparation_resources(&self) -> bool {
        true
    }
    fn supports_owned_scopes(&self) -> bool {
        true
    }
    async fn retain_preparation_resources(
        &self,
        grant: crabber_session::session_owner::preparation::PreparationGrant,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationResourceOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.retain_preparation_resources(grant).await
    }
    async fn publish_owned_scope_activation(
        &self,
        request: crabber_session::session_owner::owned_scope::OwnedScopeActivation,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
        parent: Option<crabber_session::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let fault = if authority.target().id.0.starts_with("service-owner-") {
            &self.gate.service_activation_fault
        } else if authority.target().id.0.starts_with("driver-owner-")
            || authority.target().id.0.starts_with("successor-owner-")
        {
            &self.gate.driver_activation_fault
        } else {
            &self.gate.activation_fault
        };
        let point = fault.swap(0, Ordering::SeqCst);
        assert_ne!(point, 3, "activation Store panic before publication");
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let outcome = self
            .inner
            .publish_owned_scope_activation(request, authority.clone(), parent)
            .await?;
        assert_ne!(point, 4, "activation Store panic after publication");
        if authority.target().id.0.starts_with("service-owner-")
            && self.gate.service_activation_hold.load(Ordering::SeqCst)
        {
            self.gate.service_activation_entered.notify_one();
            self.gate.service_activation_release.notified().await;
        }
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    async fn join_owned_scope(
        &self,
        proof: crabber_session::session_owner::owned_scope::JoinedOwnedScope,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if authority.target().id.0.starts_with("service-owner-") {
            self.gate.service_join_calls.fetch_add(1, Ordering::SeqCst);
            self.gate
                .service_join_keys
                .lock()
                .unwrap()
                .push(proof.request()?.id);
            let point = self.gate.service_join_fault.swap(0, Ordering::SeqCst);
            let panic = self.gate.service_join_panic.load(Ordering::SeqCst);
            assert_ne!(panic, 1, "service Join store panicked before commit");
            if point == 1 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            let mut result = self.inner.join_owned_scope(proof, authority).await?;
            assert_ne!(panic, 2, "service Join store panicked after commit");
            match &mut result {
                crabber_session::session_owner::SessionOwnerOutcome::Applied {
                    receipt, ..
                }
                | crabber_session::session_owner::SessionOwnerOutcome::Replayed(receipt) => {
                    if self
                        .gate
                        .service_join_bad_termination
                        .load(Ordering::SeqCst)
                    {
                        receipt.owner.termination = Some(
                            crabber_session::session_owner::SessionOwnerTermination::Cancelled,
                        );
                    }
                    if self.gate.service_join_bad_binding.load(Ordering::SeqCst) {
                        receipt.owner.binding.component_id = "changed-service-component".into();
                    }
                }
            }
            if point == 2 {
                return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
            }
            return Ok(result);
        }
        if authority.target().id.0.starts_with("driver-owner-")
            || authority.target().id.0.starts_with("successor-owner-")
        {
            return self.join_driver_scope(proof, authority).await;
        }
        self.gate.owned_join_calls.fetch_add(1, Ordering::SeqCst);
        let point = self.gate.owned_join_fault.swap(0, Ordering::SeqCst);
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let outcome = self
            .inner
            .join_owned_scope(proof, authority.clone())
            .await?;
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let malformed = self.gate.malformed_owned_join.swap(0, Ordering::SeqCst);
        if malformed != 0 {
            use crabber_session::session_owner::{
                SessionOwnerOutcome, SessionOwnerState, SessionOwnerTermination,
            };
            let mut receipt = match outcome {
                SessionOwnerOutcome::Applied { receipt, .. }
                | SessionOwnerOutcome::Replayed(receipt) => receipt,
            };
            match malformed {
                1 => receipt.owner.state = SessionOwnerState::Active,
                2 => receipt.operation_id = crabber_core::SessionOwnerOperationId::new(),
                3 => receipt.owner.termination = Some(SessionOwnerTermination::Panicked),
                5 => receipt.owner.binding.workspace_id = "different".into(),
                6 => receipt.owner.created_at += time::Duration::seconds(1),
                _ => (),
            }
            return Ok(SessionOwnerOutcome::Applied {
                receipt,
                authority: (malformed == 4).then_some(authority),
            });
        }
        Ok(outcome)
    }

    fn supports_session_owner_cancellation(&self) -> bool {
        true
    }
    async fn request_session_owner_cancel(
        &self,
        request: crabber_session::session_owner::RequestSessionOwnerCancel,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.request_session_owner_cancel(request).await
    }
    async fn pending_session_owner_cancel(
        &self,
        authority: &crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        Option<crabber_session::session_owner::SessionOwnerCancellation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        if self.gate.invocation_poll_panic.load(Ordering::SeqCst)
            && self
                .inner
                .get_session_owner(authority.session_id(), &authority.target().id)
                .await?
                .binding
                .kind
                == crabber_session::session_owner::SessionOwnerKind::DetachedInvocation
            && self
                .gate
                .invocation_poll_panic
                .swap(false, Ordering::SeqCst)
        {
            self.gate.invocation_poll_entered.notify_one();
            panic!("detached control Store future panicked");
        }
        if (authority.target().id.0.starts_with("driver-owner-")
            || authority.target().id.0.starts_with("successor-owner-"))
            && self.gate.driver_poll_panic.swap(false, Ordering::SeqCst)
        {
            self.gate.driver_poll_entered.notify_one();
            panic!("driver control Store future panicked");
        }
        if (authority.target().id.0.starts_with("driver-owner-")
            || authority.target().id.0.starts_with("successor-owner-"))
            && self.gate.driver_poll_hold.swap(false, Ordering::SeqCst)
        {
            self.gate.driver_poll_entered.notify_one();
            self.gate.driver_poll_release.notified().await;
            self.gate
                .driver_poll_completed
                .fetch_add(1, Ordering::SeqCst);
        }
        if !(authority.target().id.0.starts_with("driver-owner-")
            || authority.target().id.0.starts_with("successor-owner-"))
            && let Some(gate) = &self.gate.preparation_heartbeat
        {
            gate.pending().await;
        }
        self.inner.pending_session_owner_cancel(authority).await
    }
    async fn observe_session_owner_cancel(
        &self,
        request: crabber_session::session_owner::ObserveSessionOwnerCancel,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate
            .owner_observe_keys
            .lock()
            .unwrap()
            .push(request.id.clone());
        let result = self
            .inner
            .observe_session_owner_cancel(request, authority)
            .await?;
        if self
            .gate
            .owner_observe_ack_lost
            .swap(false, Ordering::SeqCst)
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    fn supports_session_owners(&self) -> bool {
        true
    }
    async fn reserve_session_owner(
        &self,
        request: crabber_session::session_owner::ReserveSessionOwner,
        parent: Option<crabber_session::session_owner::SessionOwnerParentAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let result = self.inner.reserve_session_owner(request, parent).await?;
        if self
            .gate
            .session_owner_ack_lost
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn mutate_session_owner(
        &self,
        request: crabber_session::session_owner::MutateSessionOwner,
        authority: Option<crabber_session::session_owner::SessionOwnerAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        let kind = self
            .inner
            .get_session_owner(&request.session_id, &request.target.id)
            .await?
            .binding
            .kind;
        if matches!(
            request.command,
            crabber_session::session_owner::SessionOwnerCommand::Renew { .. }
        ) && matches!(
            kind,
            crabber_session::session_owner::SessionOwnerKind::Preparation
                | crabber_session::session_owner::SessionOwnerKind::RetainedCapacity
        ) && let Some(gate) = &self.gate.preparation_heartbeat
        {
            gate.renew().await;
        }
        let uncertain = matches!(
            request.command,
            crabber_session::session_owner::SessionOwnerCommand::MarkUncertain { .. }
        ) && kind
            == crabber_session::session_owner::SessionOwnerKind::RetainedCapacity;
        let uncertain_fault = if uncertain {
            self.gate
                .cleanup_uncertain_keys
                .lock()
                .unwrap()
                .push(request.id.clone());
            self.gate.cleanup_uncertain_fault.swap(0, Ordering::SeqCst)
        } else {
            0
        };
        if uncertain_fault == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let detached = kind == crabber_session::session_owner::SessionOwnerKind::DetachedInvocation;
        let fault = match request.command {
            crabber_session::session_owner::SessionOwnerCommand::Begin => 2,
            crabber_session::session_owner::SessionOwnerCommand::Join { .. } => 3,
            _ => 0,
        };
        let scope = request.session_id.clone();
        let target = request.target.clone();
        let legacy_cancel = self.gate.legacy_cancellation(kind, &request.command);
        let result = self.inner.mutate_session_owner(request, authority).await?;
        if legacy_cancel
            && self
                .gate
                .session_owner_ack_lost
                .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        if uncertain_fault == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        if fault == 2 {
            self.cancel_before_begin_ack(scope, target, detached)
                .await?;
        }
        if detached
            && fault != 0
            && self
                .gate
                .session_owner_ack_lost
                .compare_exchange(fault, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(result)
    }
    async fn lookup_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crabber_session::session_owner::SessionOwnerReceipt>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_session_owner(session, id).await
    }
    async fn get_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerId,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerSnapshot,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.get_session_owner(session, id).await
    }

    fn supports_frontend_tools(&self) -> bool {
        self.inner.supports_frontend_tools()
    }
    async fn public_continuation_source(
        &self,
        fence: &crabber_core::RunFence,
        authority: &crabber_session::AttemptAuthority,
    ) -> Result<crabber_core::continuation_source::ResumeSnapshot, crabber_session::PublicStateError>
    {
        use crabber_session::PublicStateError;
        match self.gate.continuation_source_fault.load(Ordering::SeqCst) {
            1 => Err(PublicStateError::Unsupported),
            2 => Err(PublicStateError::UnknownStoreOutcome),
            fault => {
                let mut source = self
                    .inner
                    .public_continuation_source(fence, authority)
                    .await?;
                if fault == 3 {
                    source.attempt.id = crabber_core::AttemptId::new();
                }
                Ok(source)
            }
        }
    }
    async fn frontend_call_origin(
        &self,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallId,
    ) -> Result<Option<crabber_session::FrontendCallOrigin>, crabber_session::ToolContinuationError>
    {
        self.inner.frontend_call_origin(session, run, call).await
    }
    async fn lookup_frontend_staging(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<
        Option<crabber_session::FrontendStagingReceipt>,
        crabber_session::ToolContinuationError,
    > {
        self.inner.lookup_frontend_staging(session, id).await
    }
    async fn claim_frontend_continuation(
        &self,
        claim: crabber_session::FrontendContinuationClaim,
    ) -> Result<crabber_session::ToolContinuationClaimOutcome, crabber_session::ToolContinuationError>
    {
        let mut outcome = self.inner.claim_frontend_continuation(claim).await?;
        let fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        grant_ack_faults::interpose(&self.gate, &mut outcome, fault)?;
        Ok(outcome)
    }
    async fn recover_frontend_continuation(
        &self,
        request: crabber_session::FrontendContinuationRecovery,
    ) -> Result<crabber_session::ToolContinuationClaimOutcome, crabber_session::ToolContinuationError>
    {
        let mut outcome = self.inner.recover_frontend_continuation(request).await?;
        let fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        grant_ack_faults::interpose(&self.gate, &mut outcome, fault)?;
        Ok(outcome)
    }
    fn supports_tool_continuations(&self) -> bool {
        true
    }
    async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<
        Option<crabber_session::ToolContinuationSnapshot>,
        crabber_session::ToolContinuationError,
    > {
        self.inner.tool_continuation_snapshot(session, run).await
    }

    async fn answer_tool_interaction(
        &self,
        answer: crabber_session::ToolInteractionAnswer,
    ) -> Result<crabber_session::ToolContinuationReceipt, crabber_session::ToolContinuationError>
    {
        self.inner.answer_tool_interaction(answer).await
    }
    async fn claim_tool_continuation(
        &self,
        claim: crabber_session::ToolContinuationClaim,
    ) -> Result<crabber_session::ToolContinuationClaimOutcome, crabber_session::ToolContinuationError>
    {
        let mut outcome = self.inner.claim_tool_continuation(claim).await?;
        let fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        grant_ack_faults::interpose(&self.gate, &mut outcome, fault)?;
        Ok(outcome)
    }
    async fn recover_tool_continuation(
        &self,
        request: crabber_session::ToolContinuationRecovery,
    ) -> Result<crabber_session::ToolContinuationClaimOutcome, crabber_session::ToolContinuationError>
    {
        let mut outcome = self.inner.recover_tool_continuation(request).await?;
        let fault = self.gate.preparation_fault.swap(0, Ordering::SeqCst);
        grant_ack_faults::interpose(&self.gate, &mut outcome, fault)?;
        Ok(outcome)
    }
    async fn original_tool_continuation_plan(
        &self,
        selected: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        target: &crabber_session::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crabber_session::ToolContinuationError> {
        self.inner
            .original_tool_continuation_plan(selected, run, target)
            .await
    }
    async fn lookup_tool_continuation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<
        Option<crabber_session::ToolContinuationReceipt>,
        crabber_session::ToolContinuationError,
    > {
        if self
            .gate
            .observation
            .as_ref()
            .is_some_and(|gate| gate.lose_ack)
        {
            return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome);
        }
        self.inner.lookup_tool_continuation(session, id).await
    }
    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.inner.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.inner.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber_core::RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crabber_session::VersionedPhase, crabber_session::PhaseError> {
        self.inner.tool_phase(run, id).await
    }
    async fn mark_phase_uncertain(
        &self,
        request: crabber_session::PhaseUncertainty,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.mark_phase_uncertain(request).await
    }
    async fn record_phase_cleanup(
        &self,
        request: crabber_session::PhaseCleanup,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.record_phase_cleanup(request).await
    }
    async fn list_tool_phases(
        &self,
        request: crabber_session::PhasePageRequest,
    ) -> Result<crabber_session::PhasePage, crabber_session::PhaseError> {
        self.inner.list_tool_phases(request).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber_session::PhaseResolution,
    ) -> Result<crabber_session::PhaseResolutionOutcome, crabber_session::PhaseError> {
        self.inner.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::PhaseReceipt>, crabber_session::PhaseError> {
        self.inner.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Option<crabber_session::ApprovalSnapshot>, crabber_session::ApprovalError> {
        self.inner.approval_snapshot(session, run).await
    }
    async fn original_approval_plan(
        &self,
        selected: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        target: &crabber_session::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crabber_session::ApprovalError> {
        self.inner
            .original_approval_plan(selected, run, target)
            .await
    }
    async fn lookup_approval(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::ApprovalReceipt>, crabber_session::ApprovalError> {
        self.inner.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber_session::ApprovalAnswer,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.answer_approval(answer).await
    }
    async fn terminate_approval(
        &self,
        request: crabber_session::ApprovalTermination,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.terminate_approval(request).await
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crabber_session::ApprovalContinuationRecovery,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        if self.gate.approval_recovery_gate.load(Ordering::SeqCst) {
            self.gate.approval_recovery_entered.notify_one();
            self.gate.approval_recovery_release.notified().await;
        }
        let outcome = self.inner.reclaim_approval_continuation(request).await?;
        if matches!(
            outcome,
            crabber_session::ApprovalClaimOutcome::Applied { .. }
        ) && self.gate.approval_recovery_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber_session::ApprovalContinuationClaim,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        let outcome = self.inner.claim_approval_continuation(claim).await?;
        if matches!(
            outcome,
            crabber_session::ApprovalClaimOutcome::Applied { .. }
        ) && self.gate.approval_claim_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }

    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::VersionedAttempt>, crabber_session::AttemptError> {
        self.inner.latest_execution_attempt(run).await
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        self.inner.session_revision(session).await
    }

    async fn admit_bound_run(
        &self,
        request: crabber_session::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        let admitted = self.inner.admit_bound_run(request).await?;
        if let Some(gate) = &self.gate.admission {
            gate.hold(&admitted.session.id, &admitted.run.id).await;
        }
        Ok(admitted)
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crabber_session::BoundKeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        let outcome = self.inner.admit_bound_keyed_run(request).await?;
        if let (Some(gate), crabber_session::KeyedAdmitOutcome::Started { admitted, .. }) =
            (&self.gate.admission, &outcome)
        {
            gate.hold(&admitted.session.id, &admitted.run.id).await;
        }
        Ok(outcome)
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::RetainedExecutionBinding>, StoreError> {
        self.inner.execution_binding(session, run).await
    }

    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: crabber_session::KeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        self.inner.admit_keyed_run(request).await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn lookup_expired_run_preparation(
        &self,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::ExpiredRunReceipt>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner
            .lookup_expired_run_preparation(session, run, issuance)
            .await
    }
    async fn inspect_expired_run_preparation(
        &self,
        request: crabber_session::session_owner::preparation::InspectExpiredRunPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::ExpiredRunPreparation,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.inspect_expired_run_preparation(request).await
    }
    async fn inspect_admission_recovery(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crabber_session::session_owner::preparation::AdmissionRecoveryPreparation,
        crabber_session::AdmissionExecutionError,
    > {
        if self
            .gate
            .unsupported_recovery_inspection
            .load(Ordering::SeqCst)
        {
            return Err(crabber_session::AdmissionExecutionError::Unsupported);
        }
        self.inner.inspect_admission_recovery(request).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<crabber_session::ClaimedAdmission, crabber_session::AdmissionExecutionError> {
        let claimed = self.inner.claim_unstarted_admission(request).await?;
        if let Some(gate) = &self.gate.admission {
            gate.hold(&claimed.run.session_id, &claimed.run.id).await;
        }
        Ok(claimed)
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &crabber_core::AdmissionKey,
    ) -> Result<Option<crabber_core::AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        Ok(Box::new(DelayedTerminalExecution {
            inner: self.inner.execution(fence).await?,
            gate: Arc::clone(&self.gate),
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        assert!(
            !self.gate.reject_run_reads.load(Ordering::SeqCst),
            "metadata replay attempted a full private Run read"
        );
        let mut run = if let Some(gate) = &self.gate.observation {
            gate.read_run(self.inner.as_ref(), id).await?
        } else {
            self.inner.get_run(id).await?
        };
        if let Some(checkpoint) = self.gate.checkpoint_override.lock().unwrap().clone()
            && let Some(run) = &mut run
        {
            run.checkpoint = Some(checkpoint);
        }
        if run
            .as_ref()
            .is_some_and(|run| run.status == RunStatus::Completed)
        {
            self.gate.terminal_reads.fetch_add(1, Ordering::SeqCst);
        }
        Ok(run)
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.gate.unfinished_reads.fetch_add(1, Ordering::SeqCst);
        let mut calls = self.inner.list_unfinished_tool_calls(run).await?;
        if let Some(arguments) = self.gate.call_arguments_override.lock().unwrap().clone() {
            for call in &mut calls {
                call.arguments.clone_from(&arguments);
            }
        }
        Ok(calls)
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::AdmissionExecutionState>, StoreError> {
        self.inner.admission_execution_state(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
}

#[async_trait]
impl ExecutionStore for DelayedTerminalExecution {
    async fn observe_run_controls(
        &self,
        turn: Option<&crabber_core::TurnId>,
    ) -> Result<crabber_session::RunControlBoundary, crabber_session::RunControlError> {
        if let Some(gate) = &self.gate.observation {
            return gate.observe(self.inner.as_ref(), turn).await;
        }
        let result = self.inner.observe_run_controls(turn).await;
        if self.gate.terminal_controls
            && matches!(result, Err(crabber_session::RunControlError::StaleOwner))
        {
            self.gate.terminal_control_seen.notify_one();
        }
        result
    }

    async fn stage_frontend_call(
        &self,
        request: crabber_session::FrontendCallStaging,
        authority: crabber_session::AttemptAuthority,
    ) -> Result<crabber_session::FrontendStagingReceipt, crabber_session::ToolContinuationError>
    {
        self.inner.stage_frontend_call(request, authority).await
    }
    async fn suspend_tool_continuation(
        &self,
        request: crabber_session::ToolContinuationSuspension,
        authority: crabber_session::ToolSuspensionAuthority,
    ) -> Result<crabber_session::ToolContinuationReceipt, crabber_session::ToolContinuationError>
    {
        tool_suspension_faults::suspend(self, request, authority).await
    }
    async fn mutate_phase(
        &self,
        request: crabber_session::PhaseMutation,
        authority: Option<crabber_session::PhaseWriteAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        self.inner.mutate_phase(request, authority).await
    }
    async fn start_tool_phase(
        &self,
        start: crabber_session::PhaseStart,
        authority: Option<crabber_session::PhaseAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        if self.gate.fail_claim.load(Ordering::SeqCst)
            || self.gate.phase_fault.load(Ordering::SeqCst) == 1
        {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        let result = self.inner.start_tool_phase(start, authority).await;
        if result.is_ok() && self.gate.phase_fault.load(Ordering::SeqCst) == 2 {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        result
    }
    async fn settle_tool_phase(
        &self,
        settlement: crabber_session::PhaseSettlement,
        authority: Option<crabber_session::PhaseAuthority>,
    ) -> Result<crabber_session::PhaseMutationOutcome, crabber_session::PhaseError> {
        self.gate.tool_settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.fail_settle.load(Ordering::SeqCst)
            || self.gate.phase_fault.load(Ordering::SeqCst) == 3
        {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        let result = self.inner.settle_tool_phase(settlement, authority).await;
        if result.is_ok() && self.gate.phase_fault.load(Ordering::SeqCst) == 4 {
            return Err(crabber_session::PhaseError::UnknownStoreOutcome);
        }
        result
    }
    async fn suspend_for_approval(
        &self,
        batch: crabber_session::ApprovalSuspension,
        authority: crabber_session::AttemptAuthority,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        if self.gate.fail_permission_event.load(Ordering::SeqCst) {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
        }
        let result = self.inner.suspend_for_approval(batch, authority).await;
        if let Some(gate) = &self.gate.observation
            && result.is_ok()
        {
            gate.acknowledge().await;
            if gate.lose_ack {
                return Err(crabber_session::ApprovalError::UnknownStoreOutcome);
            }
        }
        result
    }

    async fn mutate_attempt(
        &self,
        request: crabber_session::AttemptMutation,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        let begin = matches!(
            request.command,
            crabber_session::AttemptCommand::Begin { .. }
        );
        let reserve = matches!(
            &request.command,
            crabber_session::AttemptCommand::Reserve { .. }
                | crabber_session::AttemptCommand::ReserveBound { .. }
        );
        if begin {
            self.gate
                .record_initial_task(InitialTaskStage::BeginAttempt);
        }
        let result = self.inner.mutate_attempt(request, authority).await;
        if reserve
            && matches!(
                &result,
                Ok(crabber_session::AttemptMutationOutcome::Applied { .. })
            )
            && self.gate.reserve_ack_lost.load(Ordering::SeqCst)
        {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        if begin && result.is_ok() && self.gate.begin_ack_lost.load(Ordering::SeqCst) {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        result
    }
    async fn conclude_attempt(
        &self,
        conclusion: crabber_session::AttemptConclusion,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        use crabber_session::AttemptError;
        if matches!(
            &conclusion.boundary,
            crabber_session::RunBoundary::Suspended { .. }
        ) {
            let result = self.inner.conclude_attempt(conclusion, authority).await;
            if let Some(gate) = &self.gate.observation
                && result.is_ok()
            {
                gate.acknowledge().await;
                if gate.lose_ack {
                    return Err(AttemptError::UnknownStoreOutcome);
                }
            }
            return result;
        }
        if self.gate.observation.is_some() {
            // This fixture delays only suspension ACKs, never terminal publication.
            return self.inner.conclude_attempt(conclusion, authority).await;
        }
        self.gate.settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 1 {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        let completed = matches!(
            &conclusion.boundary,
            crabber_session::RunBoundary::Terminal {
                status: RunStatus::Completed,
                ..
            }
        );
        let result = self.inner.conclude_attempt(conclusion, authority).await;
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 2 && result.is_ok() {
            return Err(AttemptError::UnknownStoreOutcome);
        }
        if completed && result.is_ok() {
            if let Some(sender) = self.gate.entered.lock().unwrap().take() {
                let _ = sender.send(());
            }
            self.gate.release.notified().await;
            if self.gate.terminal_control_ack_lost {
                return Err(AttemptError::UnknownStoreOutcome);
            }
        }
        result
    }

    async fn begin_admission_execution(
        &self,
    ) -> Result<(), crabber_session::AdmissionExecutionError> {
        self.inner.begin_admission_execution().await
    }
    async fn renew_lease(&self, until: time::OffsetDateTime) -> Result<(), StoreError> {
        if let Some(gate) = &self.gate.observation {
            return gate.renew(self.inner.as_ref(), until).await;
        }
        self.inner.renew_lease(until).await
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        if message.role == Role::Assistant
            && let Some(gate) = &self.gate.append
            && gate.enabled.load(Ordering::SeqCst)
        {
            gate.entered.notify_one();
            gate.release.notified().await;
            if gate.conflict.load(Ordering::SeqCst) {
                return Err(StoreError::Conflict);
            }
        }
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        if event.kind == EventKind::PermissionRequested
            && self.gate.fail_permission_event.load(Ordering::SeqCst)
        {
            return Err(StoreError::Validation("SECRET permission backend".into()));
        }
        self.inner.append_event(event).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.create_tool_call(call, pending_event).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        if self.gate.fail_claim.load(Ordering::SeqCst) {
            return Err(StoreError::Validation("SECRET backend".into()));
        }
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.gate.tool_settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.fail_settle.load(Ordering::SeqCst) {
            return Err(StoreError::Conflict);
        }
        self.inner
            .settle_tool_call(id, result, result_message, terminal_event)
            .await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.inner.start_epoch(epoch).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.inner.finish_epoch(id, summary).await
    }
    async fn pause_run(&self, checkpoint: Value, event: EventRecord) -> Result<(), StoreError> {
        self.inner.pause_run(checkpoint, event).await
    }
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.gate.settlements.fetch_add(1, Ordering::SeqCst);
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 1 {
            return Err(StoreError::Validation(
                "terminal acknowledgement unavailable".into(),
            ));
        }
        let result = self.inner.settle_run(status, error, usage, event).await;
        if self.gate.terminal_fault.load(Ordering::SeqCst) == 2 && result.is_ok() {
            return Err(StoreError::Validation(
                "terminal acknowledgement unavailable".into(),
            ));
        }
        if status == RunStatus::Completed && result.is_ok() {
            if let Some(sender) = self
                .gate
                .entered
                .lock()
                .expect("settlement gate poisoned")
                .take()
            {
                let _ = sender.send(());
            }
            self.gate.release.notified().await;
        }
        result
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        self.inner.put_extension_state(extension_id, entries).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox(kind).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox_into_history(kind).await
    }
}

impl DelayedTerminalStore {
    pub(super) async fn join_driver_scope(
        &self,
        proof: crabber_session::session_owner::owned_scope::JoinedOwnedScope,
        authority: crabber_session::session_owner::SessionOwnerAuthority,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.gate.driver_join_calls.fetch_add(1, Ordering::SeqCst);
        let point = self.gate.driver_join_fault.swap(0, Ordering::SeqCst);
        assert_ne!(point, 3, "Join Store panic before publication");
        if point == 1 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        let successor = authority.target().id.0.starts_with("successor-owner-");
        let keys = if successor {
            &self.gate.successor_join_keys
        } else {
            &self.gate.driver_join_keys
        };
        keys.lock().unwrap().push(proof.request()?.id);
        let mut outcome = self.inner.join_owned_scope(proof, authority).await?;
        if successor && self.gate.successor_join_bad.load(Ordering::SeqCst) != 0 {
            match &mut outcome {
                crabber_session::session_owner::SessionOwnerOutcome::Applied {
                    receipt, ..
                }
                | crabber_session::session_owner::SessionOwnerOutcome::Replayed(receipt) => {
                    receipt.owner.termination =
                        Some(crabber_session::session_owner::SessionOwnerTermination::Cancelled);
                }
            }
        }
        if !successor {
            self.gate.malformed_driver_ack(&mut outcome);
        }
        assert_ne!(point, 4, "Join Store panic after publication");
        if point == 2 {
            return Err(crabber_session::session_owner::SessionOwnerError::UnknownStoreOutcome);
        }
        Ok(outcome)
    }
    pub(super) async fn cancel_before_begin_ack(
        &self,
        session: SessionId,
        target: crabber_session::session_owner::SessionOwnerTarget,
        detached: bool,
    ) -> Result<(), crabber_session::session_owner::SessionOwnerError> {
        use crabber_session::session_owner::{
            RequestSessionOwnerCancel, SessionOwnerError, SessionOwnerResolver,
        };
        let mode = if detached { 2 } else { 1 };
        if self
            .gate
            .session_owner_cancel_on_begin
            .compare_exchange(mode, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        let expected_revision = if self
            .inner
            .get_session(&session)
            .await
            .map_err(|_| SessionOwnerError::UnknownStoreOutcome)?
            .is_some()
        {
            Some(
                self.inner
                    .session_revision(&session)
                    .await
                    .map_err(|_| SessionOwnerError::UnknownStoreOutcome)?,
            )
        } else {
            None
        };
        self.inner
            .request_session_owner_cancel(RequestSessionOwnerCancel {
                id: crabber_core::SessionOwnerOperationId::new(),
                session_id: session,
                target,
                expected_revision,
                claim: SessionOwnerResolver {
                    resolver_id: "test controller".into(),
                    authority_claim: "cancel before Begin ACK".into(),
                },
            })
            .await?;
        Ok(())
    }
}
