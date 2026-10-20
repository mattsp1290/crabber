//! Host-supervised queue preparation and admission; no background dispatcher.
use super::{Admission, InitialEntry, Orchestrator, Request, RuntimeError};
use crabber_core::{ControlId, QueueId, SessionContext, SessionId, SessionRevision, TraceContext};
use crabber_session::{
    BoundKeyedAdmitRequest, QueueAction, QueueAuthority, QueueInput, QueueMutation, QueueOutcome,
    QueueReceipt, RetainedQueue, StoreError,
};
use std::time::Duration;
#[derive(Debug, Clone)]
pub struct QueueControl {
    pub id: ControlId,
    pub expected_revision: SessionRevision,
    pub authority: QueueAuthority,
}
impl QueueControl {
    fn mutation(&self, action: QueueAction) -> QueueMutation {
        QueueMutation {
            session_id: self.authority.session_id.clone(),
            id: self.id.clone(),
            expected_revision: self.expected_revision,
            action,
        }
    }
    fn admit(&self) -> QueueMutation {
        self.mutation(QueueAction::Admit {
            authority: self.authority.clone(),
            owner: self.authority.token.clone(),
            lease: Duration::from_secs(30),
        })
    }
}
impl Orchestrator {
    /// Apply a host queue control without running factories or providers.
    /// # Errors
    /// Returns native scope, revision, FIFO and storage failures.
    pub async fn mutate_queue(&self, request: QueueMutation) -> Result<QueueOutcome, RuntimeError> {
        if matches!(request.action, QueueAction::Cancel { .. }) {
            return Ok(self.store.mutate_queue(request).await?);
        }
        if self.is_closed() {
            let receipt = self
                .lookup_queue(&request.session_id, &request.id)
                .await?
                .ok_or(RuntimeError::HostClosed)?;
            verify_receipt(&request, &receipt)?;
            return Ok(QueueOutcome::Replayed(receipt));
        }
        self.own_preparation(request.session_id.clone(), move |runtime| async move {
            runtime.check_original_stop()?;
            Ok(runtime.store.mutate_queue(request).await?)
        })
        .await
    }
    /// Read original safe queue-control metadata.
    /// # Errors
    /// Returns unsupported or corrupt storage evidence.
    pub async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, StoreError> {
        self.store.lookup_queue(session, id).await
    }
    /// Read bounded coherent queue metadata.
    /// # Errors
    /// Mutations invalidate the captured continuation revision.
    pub async fn list_queue(
        &self,
        request: crabber_session::QueuePageRequest,
    ) -> Result<crabber_session::QueuePage, StoreError> {
        self.store.list_queue(request).await
    }
    /// Read exact safe queue item metadata.
    /// # Errors
    /// Returns unsupported, missing or corrupt evidence.
    pub async fn get_queue(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<crabber_session::QueueSummary, StoreError> {
        self.store.get_queue(session, id).await
    }

    /// Reconcile enqueue before current revisions/defaults/readiness are consulted.
    /// # Errors
    /// Original intent changes conflict; a text adapter reuses the winning envelope.
    pub async fn replay_queue_enqueue(
        &self,
        request: &Request,
        queue_id: &QueueId,
        id: &ControlId,
        options: &crabber_core::AdmissionOptions,
    ) -> Result<Option<QueueReceipt>, RuntimeError> {
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        let Some(saved) = self.store.lookup_queue(session, id).await? else {
            return Ok(None);
        };
        let retained = self.store.retained_queue(session, queue_id).await?;
        let input = if let Some(input) = &request.input_envelope {
            input.clone()
        } else {
            if retained.input.input.legacy_text() != Some(request.text.as_str()) {
                return Err(StoreError::AdmissionConflict.into());
            }
            retained.input.input
        };
        let mutation = QueueMutation {
            session_id: session.clone(),
            id: id.clone(),
            expected_revision: input.expected_revision(),
            action: QueueAction::Enqueue {
                queue_id: queue_id.clone(),
                input: Box::new(QueueInput {
                    request: request.admission_data()?,
                    input,
                    admission: options.clone(),
                }),
            },
        };
        verify_receipt(&mutation, &saved)?;
        Ok(Some(saved))
    }
    /// Persist future input without factories, provider calls or a fabricated run.
    /// # Errors
    /// Unsupported adapters refuse before accepting anything; enqueue checks captured revision.
    pub async fn enqueue_queue(
        &self,
        request: Request,
        queue_id: QueueId,
        id: ControlId,
        options: crabber_core::AdmissionOptions,
    ) -> Result<QueueReceipt, RuntimeError> {
        if self.is_closed() {
            return self
                .replay_queue_enqueue(&request, &queue_id, &id, &options)
                .await?
                .ok_or(RuntimeError::HostClosed);
        }
        let session = request.session_id.clone().ok_or(StoreError::NotFound)?;
        self.own_preparation(session, move |runtime| async move {
            runtime
                .enqueue_queue_owned(request, queue_id, id, options)
                .await
        })
        .await
    }
    async fn enqueue_queue_owned(
        &self,
        request: Request,
        queue_id: QueueId,
        id: ControlId,
        options: crabber_core::AdmissionOptions,
    ) -> Result<QueueReceipt, RuntimeError> {
        if !self.store.supports_future_queue() {
            return Err(StoreError::HostUnsupported.into());
        }
        if let Some(saved) = self
            .replay_queue_enqueue(&request, &queue_id, &id, &options)
            .await?
        {
            return Ok(saved);
        }
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        let input = self.capture_input(&request, session).await?;
        let mutation = QueueMutation {
            session_id: session.clone(),
            id,
            expected_revision: input.expected_revision(),
            action: QueueAction::Enqueue {
                queue_id,
                input: Box::new(QueueInput {
                    request: request.admission_data()?,
                    input,
                    admission: options,
                }),
            },
        };
        self.check_original_stop()?;
        Ok(self.store.mutate_queue(mutation).await?.receipt().clone())
    }
    /// Explicit private intent read; caller must supply its admitted workspace context.
    /// # Errors
    /// Rejects foreign scope and malformed retained records, never grants authority.
    pub async fn queued_intent(
        &self,
        context: &SessionContext,
        id: &QueueId,
    ) -> Result<RetainedQueue, RuntimeError> {
        self.validate_context(context).await?;
        let retained = self.store.retained_queue(context.session_id(), id).await?;
        if retained.input.request.workspace_id != context.workspace_id()
            || retained.input.request.directory != context.directory()
        {
            return Err(StoreError::SessionIdentityMismatch.into());
        }
        Ok(retained)
    }
    fn verify_queued_request(
        &self,
        request: &Request,
        retained: &RetainedQueue,
    ) -> Result<(), RuntimeError> {
        let original = &retained.input.request;
        let current = request.admission_data()?;
        let selection_matches = !original.selection_policy.is_explicit()
            || (current.provider_id == original.provider_id
                && current.model_id == original.model_id);
        if !selection_matches
            || current.session_id != original.session_id
            || current.workspace_id != original.workspace_id
            || current.directory != original.directory
            || current.text != original.text
            || current.selection_policy.is_explicit() != original.selection_policy.is_explicit()
            || request.input_envelope.as_ref() != Some(&retained.input.input)
            || retained.input.admission.behavior_fingerprint != self.behavior_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        if let Some(prepared) = &retained.prepared {
            let normalized = Self::original_default_request(request, &prepared.binding)?;
            self.verify_binding_request(&normalized, &original.session_id, &prepared.binding)?;
            self.verify_declared_provider(&prepared.binding)?;
            self.verify_configuration_capsule(
                prepared
                    .keyed
                    .execution
                    .as_ref()
                    .ok_or(StoreError::AdmissionConflict)?,
                &SessionContext::new(
                    original.session_id.clone(),
                    original.workspace_id.clone(),
                    original.directory.clone(),
                ),
            )?;
        }
        Ok(())
    }
    async fn queue_retained(
        &self,
        request: &Request,
        control: &QueueControl,
    ) -> Result<RetainedQueue, RuntimeError> {
        if request.session_id.as_ref() != Some(&control.authority.session_id) {
            return Err(StoreError::SessionIdentityMismatch.into());
        }
        let retained = self
            .queued_intent(
                &SessionContext::new(
                    control.authority.session_id.clone(),
                    request.workspace_id.clone(),
                    request.directory.clone(),
                ),
                &control.authority.queue_id,
            )
            .await?;
        self.verify_queued_request(request, &retained)?;
        Ok(retained)
    }
    async fn queue_fresh_owned(
        &self,
        request: &Request,
        control: &QueueControl,
    ) -> Result<RetainedQueue, RuntimeError> {
        if request.session_id.as_ref() != Some(&control.authority.session_id) {
            return Err(StoreError::SessionIdentityMismatch.into());
        }
        if !self.store.supports_owned_queue_preparation_inspection() {
            return Err(StoreError::HostUnsupported.into());
        }
        let retained = self
            .store
            .inspect_owned_queue_preparation(
                &SessionContext::new(
                    control.authority.session_id.clone(),
                    request.workspace_id.clone(),
                    request.directory.clone(),
                ),
                &control.authority,
                control.expected_revision,
            )
            .await?;
        self.verify_queued_request(request, &retained)?;
        Ok(retained)
    }
    fn apply_queue_defaults(
        captured: &mut Request,
        session: crabber_session::SessionSummary,
    ) -> Result<(), RuntimeError> {
        if let crabber_session::AdmissionSelectionPolicy::SessionDefaults { fallback } =
            &captured.selection_policy
        {
            captured.selection_policy.validate()?;
            captured.selection = crabber_providers::Selection {
                provider_id: session
                    .defaults
                    .provider_id
                    .unwrap_or_else(|| fallback.provider_id.clone()),
                model_id: session
                    .defaults
                    .model_id
                    .unwrap_or_else(|| fallback.model_id.clone()),
            };
        }
        Ok(())
    }
    /// Reconcile the exact original prepare key before readiness/default reads/factories.
    /// # Errors
    /// Changed authority, revision, input or configuration conflicts.
    pub async fn replay_queue_prepare(
        &self,
        request: &Request,
        control: &QueueControl,
    ) -> Result<Option<QueueReceipt>, RuntimeError> {
        let Some(saved) = self
            .store
            .lookup_queue(&control.authority.session_id, &control.id)
            .await?
        else {
            return Ok(None);
        };
        let retained = self.queue_retained(request, control).await?;
        let prepared = retained.prepared.ok_or(StoreError::AdmissionConflict)?;
        let mutation = control.mutation(QueueAction::Prepare {
            authority: control.authority.clone(),
            prepared: Box::new(prepared),
        });
        verify_receipt(&mutation, &saved)?;
        Ok(Some(saved))
    }
    /// Freeze one winning provider/default/plan/input binding. An identical retry is metadata only.
    /// # Errors
    /// Returns stale claim, latest revision, readiness or semantic failures.
    pub async fn prepare_queue(
        &self,
        request: Request,
        control: QueueControl,
    ) -> Result<QueueReceipt, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
                return Ok(saved);
            }
            return Err(refusal);
        }
        self.own_preparation(
            control.authority.session_id.clone(),
            move |runtime| async move { runtime.prepare_queue_owned(request, control).await },
        )
        .await
    }
    pub(super) async fn prepare_queue_owned(
        &self,
        request: Request,
        control: QueueControl,
    ) -> Result<QueueReceipt, RuntimeError> {
        if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
            return Ok(saved);
        }
        let retained = match self.queue_fresh_owned(&request, &control).await {
            Ok(retained) => retained,
            Err(error) => {
                if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
                    return Ok(saved);
                }
                return Err(error);
            }
        };
        if retained.prepared.is_some() {
            if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
                return Ok(saved);
            }
            return Err(StoreError::Conflict.into());
        }
        self.reject_pending_preparation(
            &control.authority.session_id,
            &crabber_session::session_owner::preparation::PreparationKey::Queue(control.id.clone()),
        )
        .await?;
        let mut captured = request.clone();
        let session = self
            .store
            .queue_preparation_snapshot(&control.authority.session_id)
            .await?;
        if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
            return Ok(saved);
        }
        if session.revision != control.expected_revision {
            if let Some(saved) = self.replay_queue_prepare(&request, &control).await? {
                return Ok(saved);
            }
            return Err(StoreError::AdmissionRevisionConflict.into());
        }
        if session.workspace_id != captured.workspace_id || session.directory != captured.directory
        {
            return Err(StoreError::SessionIdentityMismatch.into());
        }
        Self::apply_queue_defaults(&mut captured, session)?;
        let grant = self
            .begin_control_preparation(
                &mut captured,
                &control.authority.session_id,
                super::preparation::PreparationSpec {
                    purpose:
                        crabber_session::session_owner::preparation::PreparationPurpose::Queue {
                            authority: control.authority.clone(),
                        },
                    key: crabber_session::session_owner::preparation::PreparationKey::Queue(
                        control.id.clone(),
                    ),
                    revision: Some(control.expected_revision),
                    options: Some(retained.input.admission.clone()),
                    intent: None,
                    source: None,
                },
            )
            .await?;
        let runtime = self.clone();
        let prepared_request = captured.clone();
        let prepared_control = control.clone();
        let ((), completed) = self
            .finish_durable_preparation(grant, move |scope| async move {
                let runtime = runtime.with_preparation_scope(scope);
                let (plan, prepared) = runtime
                    .prepare_keyed_admission(&prepared_request, retained.input.admission)
                    .await?;
                plan.release();
                Ok((
                    (),
                    crabber_session::session_owner::preparation::PreparedOperation::Queue {
                        request: prepared_control.mutation(QueueAction::Prepare {
                            authority: prepared_control.authority.clone(),
                            prepared: Box::new(prepared),
                        }),
                        binding: None,
                    },
                ))
            })
            .await?;
        let crabber_session::session_owner::preparation::PreparationCompletion::Queue(outcome) =
            completed
        else {
            return Err(StoreError::AdmissionConflict.into());
        };
        Ok(outcome.receipt().clone())
    }
    /// Exact admission replay, including after close; grants no worker handle.
    /// # Errors
    /// Verifies original control and frozen configuration without current-default reads.
    pub async fn replay_queue_admit(
        &self,
        request: &Request,
        control: &QueueControl,
    ) -> Result<Option<crabber_core::AdmissionReceipt>, RuntimeError> {
        let Some(saved) = self
            .store
            .lookup_queue(&control.authority.session_id, &control.id)
            .await?
        else {
            return Ok(None);
        };
        self.queue_retained(request, control).await?;
        verify_receipt(&control.admit(), &saved)?;
        Ok(Some(
            saved
                .summary
                .admission
                .ok_or(StoreError::AdmissionConflict)?,
        ))
    }
    /// Atomically admit a prepared head and start only the freshly returned owned worker.
    /// Unknown ACKs require receipt reconciliation and explicit Unstarted recovery.
    /// # Errors
    /// Never dispatches on replay, stale authority or any unknown store result.
    pub async fn admit_queue(
        &self,
        request: Request,
        control: QueueControl,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            if let Some(receipt) = self.replay_queue_admit(&request, &control).await? {
                return Ok(Admission::Replayed(receipt));
            }
            return Err(refusal);
        }
        self.own_preparation(
            control.authority.session_id.clone(),
            move |runtime| async move {
                let runtime = runtime.with_context(context);
                Box::pin(runtime.admit_queue_owned(request, control)).await
            },
        )
        .await
    }
    pub(super) async fn admit_queue_owned(
        &self,
        request: Request,
        control: QueueControl,
    ) -> Result<Admission, RuntimeError> {
        if let Some(receipt) = self.replay_queue_admit(&request, &control).await? {
            return Ok(Admission::Replayed(receipt));
        }
        if self
            .declared_resource_contract(&SessionContext::new(
                control.authority.session_id.clone(),
                request.workspace_id.clone(),
                request.directory.clone(),
            ))?
            .is_some()
            && (!self.store.supports_queue_preparation_admission_inspection()
                || !self.store.supports_execution_drivers())
        {
            return Err(StoreError::HostUnsupported.into());
        }
        self.reject_pending_preparation(
            &control.authority.session_id,
            &crabber_session::session_owner::preparation::PreparationKey::Queue(control.id.clone()),
        )
        .await?;
        let retained = match self.queue_fresh_owned(&request, &control).await {
            Ok(retained) => retained,
            Err(error) => {
                if let Some(saved) = self.replay_queue_admit(&request, &control).await? {
                    return Ok(Admission::Replayed(saved));
                }
                return Err(error);
            }
        };
        let original = retained.prepared.ok_or(StoreError::Conflict)?;
        let mut captured = Request::from_binding(&original.binding);
        let grant = self
            .begin_control_preparation(
                &mut captured,
                &control.authority.session_id,
                super::preparation::PreparationSpec {
                    purpose:
                        crabber_session::session_owner::preparation::PreparationPurpose::Queue {
                            authority: control.authority.clone(),
                        },
                    key: crabber_session::session_owner::preparation::PreparationKey::Queue(
                        control.id.clone(),
                    ),
                    revision: Some(control.expected_revision),
                    options: Some(retained.input.admission.clone()),
                    intent: Some(
                        crabber_session::session_owner::preparation::queue_admission_intent(
                            &original,
                        )?,
                    ),
                    source: None,
                },
            )
            .await?;
        let runtime = self.clone();
        let prepared_request = captured.clone();
        let prepared_control = control.clone();
        let work = move |scope| async move {
            let runtime = runtime.with_preparation_scope(scope);
            let (plan, restored) = runtime
                .prepare_keyed_admission(&prepared_request, retained.input.admission)
                .await?;
            verify_restored(&original, &restored)?;
            let bound = runtime
                .prebind_owned_provider(&plan, &restored.binding.provider)
                .await?;
            Ok((
                (plan, bound),
                crabber_session::session_owner::preparation::PreparedOperation::Queue {
                    request: prepared_control.admit(),
                    binding: Some(Box::new(restored)),
                },
            ))
        };
        if grant.retains_resources() {
            let outcome = Box::pin(self.finish_initial_preparation(grant, captured, work)).await?;
            return pending_admission(outcome);
        }
        let ((plan, bound), completed) =
            Box::pin(self.finish_durable_preparation(grant, work)).await?;
        self.queue_completed_admission(captured, (plan, bound), completed)
    }
    fn queue_completed_admission(
        &self,
        captured: Request,
        value: (
            crabber_extension::RunPlan,
            Option<crabber_providers::BoundProvider>,
        ),
        completed: crabber_session::session_owner::preparation::PreparationCompletion,
    ) -> Result<Admission, RuntimeError> {
        let (plan, bound) = value;
        let crabber_session::session_owner::preparation::PreparationCompletion::Queue(outcome) =
            completed
        else {
            return Err(StoreError::AdmissionConflict.into());
        };
        match outcome {
            QueueOutcome::Replayed(saved) => Ok(Admission::Replayed(
                saved
                    .summary
                    .admission
                    .ok_or(StoreError::AdmissionConflict)?,
            )),
            QueueOutcome::Applied {
                receipt,
                admitted: Some(admitted),
                ..
            } => Ok(Admission::Started {
                receipt: receipt
                    .summary
                    .admission
                    .ok_or(StoreError::AdmissionConflict)?,
                handle: self.spawn_admitted(
                    captured,
                    plan,
                    admitted.fence,
                    &admitted.session,
                    InitialEntry::Keyed,
                    bound,
                )?,
            }),
            QueueOutcome::Applied { .. } => Err(StoreError::AdmissionConflict.into()),
        }
    }
    /// Recover a queue's positively Unstarted admitted winner explicitly.
    /// # Errors
    /// Uses the existing native recovery contract and original frozen selection.
    pub async fn recover_queue(
        &self,
        request: Request,
        session: &SessionId,
        id: &QueueId,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            let retained = self.store.retained_queue(session, id).await?;
            self.verify_queued_request(&request, &retained)?;
            return self
                .replay_started_admission(&request, &retained.input.admission)
                .await?
                .map(Admission::Replayed)
                .ok_or(refusal);
        }
        let session = session.clone();
        let id = id.clone();
        self.own_preparation(session.clone(), move |runtime| async move {
            let retained = runtime.store.retained_queue(&session, &id).await?;
            runtime.verify_queued_request(&request, &retained)?;
            runtime
                .recover_retained_admission(request, retained.input.admission, context)
                .await
        })
        .await
    }
}
fn verify_restored(
    original: &BoundKeyedAdmitRequest,
    restored: &BoundKeyedAdmitRequest,
) -> Result<(), RuntimeError> {
    if original.binding != restored.binding || original.keyed.execution != restored.keyed.execution
    {
        return Err(StoreError::AdmissionConflict.into());
    }
    Ok(())
}
fn verify_receipt(request: &QueueMutation, saved: &QueueReceipt) -> Result<(), RuntimeError> {
    if saved.session_id != request.session_id
        || saved.operation_id != request.id
        || saved.semantic_digest != request.fingerprint()?
    {
        return Err(StoreError::AdmissionConflict.into());
    }
    Ok(())
}

fn pending_admission(
    outcome: super::preparation::pending::PendingOutcome,
) -> Result<Admission, RuntimeError> {
    use super::preparation::pending::{PendingOutcome, main::Metadata};
    match outcome {
        PendingOutcome::Started {
            handle,
            metadata: Metadata::Queue(receipt),
        } => Ok(Admission::Started { receipt, handle }),
        PendingOutcome::Replayed(Metadata::Queue(receipt)) => Ok(Admission::Replayed(receipt)),
        _ => Err(StoreError::AdmissionConflict.into()),
    }
}
