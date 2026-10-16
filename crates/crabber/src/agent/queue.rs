//! The host schedules each queue claim, preparation and admission explicitly.
use super::{Admission, Agent, ExecutionContext, RuntimeError};
use crabber_core::{
    AdmissionOptions, ControlId, InputEnvelope, QueueId, SessionContext, SessionId, TraceContext,
};
use crabber_runtime::{QueueControl, Request};
use crabber_session::{
    QueueMutation, QueueOutcome, QueuePage, QueuePageRequest, QueueReceipt, QueueSummary,
    RetainedQueue,
};
impl Agent {
    fn queue_ingress(&self) -> Result<(), RuntimeError> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst)
            || self
                .contextual
                .as_ref()
                .is_some_and(|provider| provider.is_closed())
        {
            return Err(RuntimeError::HostClosed);
        }
        Ok(())
    }
    /// Enqueue typed future input; stored defaults resolve when this head is prepared.
    /// Configured fallback and reflected profile resolve at the first preparation.
    /// # Errors
    /// Unsupported stores refuse before accepting input; captured revision is required.
    pub async fn enqueue_session_input_in(
        &self,
        context: &SessionContext,
        input: InputEnvelope,
        queue_id: QueueId,
        id: ControlId,
        options: AdmissionOptions,
    ) -> Result<QueueReceipt, RuntimeError> {
        let mut request = self.default_request_in(context, String::new());
        request.input_envelope = Some(input);
        if let Some(saved) = self
            .runtime
            .replay_queue_enqueue(&request, &queue_id, &id, &options)
            .await?
        {
            return Ok(saved);
        }
        self.queue_ingress()?;
        self.runtime
            .enqueue_queue(request, queue_id, id, options)
            .await
    }
    /// Enqueue typed future input using an explicit provider/model override.
    /// # Errors
    /// Returns unsupported, scope, semantic or revision errors without factories.
    pub async fn enqueue_input_in(
        &self,
        context: &ExecutionContext,
        input: InputEnvelope,
        queue_id: QueueId,
        id: ControlId,
        options: AdmissionOptions,
    ) -> Result<QueueReceipt, RuntimeError> {
        let mut request = self.context_request(context, String::new());
        request.input_envelope = Some(input);
        if let Some(saved) = self
            .runtime
            .replay_queue_enqueue(&request, &queue_id, &id, &options)
            .await?
        {
            return Ok(saved);
        }
        self.queue_ingress()?;
        self.runtime
            .enqueue_queue(request, queue_id, id, options)
            .await
    }
    /// Claim or cancel a future item explicitly; replay never restores authority.
    /// # Errors
    /// Returns native queue scope, state, revision and storage failures.
    pub async fn mutate_queue(&self, request: QueueMutation) -> Result<QueueOutcome, RuntimeError> {
        if let Some(saved) = self
            .runtime
            .lookup_queue(&request.session_id, &request.id)
            .await?
        {
            if request.fingerprint()? != saved.semantic_digest {
                return Err(crabber_session::StoreError::AdmissionConflict.into());
            }
            return Ok(QueueOutcome::Replayed(saved));
        }
        if !matches!(request.action, crabber_session::QueueAction::Cancel { .. }) {
            self.queue_ingress()?;
        }
        Ok(self.runtime.mutate_queue(request).await?)
    }
    /// Safe original control metadata; callable after close.
    /// # Errors
    /// Returns unsupported or storage-integrity failures.
    pub async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, crabber_session::StoreError> {
        self.runtime.lookup_queue(session, id).await
    }
    /// Coherent bounded FIFO metadata, never input or worker authority.
    /// # Errors
    /// Continuation requires the captured revision; mutations invalidate it.
    pub async fn list_queue(
        &self,
        request: QueuePageRequest,
    ) -> Result<QueuePage, crabber_session::StoreError> {
        self.runtime.list_queue(request).await
    }
    /// Exact safe item metadata; callable after close.
    /// # Errors
    /// Returns unsupported, missing or corrupt storage evidence.
    pub async fn get_queue(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, crabber_session::StoreError> {
        self.runtime.get_queue(session, id).await
    }
    /// Explicit private host read within a supplied admitted workspace context.
    /// # Errors
    /// Rejects foreign scope; reading never issues queue authority.
    pub async fn queued_intent(
        &self,
        context: &SessionContext,
        id: &QueueId,
    ) -> Result<RetainedQueue, RuntimeError> {
        self.runtime.queued_intent(context, id).await
    }
    async fn queue_request(
        &self,
        context: &SessionContext,
        id: &QueueId,
    ) -> Result<Request, RuntimeError> {
        let retained = self.runtime.queued_intent(context, id).await?;
        let data = &retained.input.request;
        let mut request = if data.selection_policy.is_explicit() {
            self.request_at(
                Some(context.session_id().clone()),
                data.text.clone(),
                context.workspace_id(),
                context.directory(),
                &crabber_providers::Selection {
                    provider_id: data.provider_id.clone(),
                    model_id: data.model_id.clone(),
                },
            )
        } else {
            self.default_request_in(context, data.text.clone())
        };
        request.input_envelope = Some(retained.input.input);
        Ok(request)
    }
    /// Freeze the head's prepared binding under its exact live claim and latest CAS.
    /// # Errors
    /// Reconciles the original key before readiness/default reads or factories.
    pub async fn prepare_queue_in(
        &self,
        context: &SessionContext,
        control: QueueControl,
    ) -> Result<QueueReceipt, RuntimeError> {
        let request = self
            .queue_request(context, &control.authority.queue_id)
            .await?;
        if let Some(saved) = self
            .runtime
            .replay_queue_prepare(&request, &control)
            .await?
        {
            return Ok(saved);
        }
        self.prepare_request(&request).await?;
        self.runtime.prepare_queue(request, control).await
    }
    /// Admit a prepared FIFO head and return its one freshly owned execution handle.
    /// # Errors
    /// Unknown commit acknowledgement grants no handle; reconcile or recover explicitly.
    pub async fn admit_queue_in(
        &self,
        context: &SessionContext,
        control: QueueControl,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let request = self
            .queue_request(context, &control.authority.queue_id)
            .await?;
        if let Some(saved) = self.runtime.replay_queue_admit(&request, &control).await? {
            return Ok(Admission::Replayed(saved));
        }
        self.prepare_request(&request).await?;
        Ok(Admission::from_runtime(
            self.runtime.admit_queue(request, control, trace).await?,
        ))
    }
    /// Convenience for a freshly claimed, unprepared head. Keep both operation IDs
    /// and the returned prepare revision to reconcile an unknown admission ACK.
    /// # Errors
    /// A raced mutation causes revision conflict; the host decides when to retry.
    pub async fn dispatch_queue_in(
        &self,
        context: &SessionContext,
        prepare: QueueControl,
        admission_id: ControlId,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let receipt = self.prepare_queue_in(context, prepare.clone()).await?;
        self.admit_queue_in(
            context,
            QueueControl {
                id: admission_id,
                expected_revision: receipt.revision,
                authority: prepare.authority,
            },
            trace,
        )
        .await
    }
    /// Explicit recovery of a committed queue admission with positive Unstarted evidence.
    /// # Errors
    /// Refuses live, started or drifted execution; uses original selection and input.
    pub async fn recover_queue_in(
        &self,
        context: &SessionContext,
        id: &QueueId,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let request = self.queue_request(context, id).await?;
        // Native admission recovery performs original-receipt reconciliation first.
        let retained = self.runtime.queued_intent(context, id).await?;
        self.recover_request(request, retained.input.admission, trace)
            .await
    }
}
