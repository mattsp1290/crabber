//! The host schedules each queue claim, preparation and admission explicitly.
use super::{Admission, Agent, ExecutionContext, RuntimeError};
use crabber_core::{
    AdmissionOptions, ControlId, InputEnvelope, QueueId, SessionContext, SessionId, TraceContext,
};
use crabber_runtime::QueueControl;
use crabber_session::{
    QueueMutation, QueueOutcome, QueuePage, QueuePageRequest, QueueReceipt, QueueSummary,
    RetainedQueue,
};
impl Agent {
    /// Read the lowest live queue sequence per session in one workspace.
    /// # Errors
    /// Returns unsupported, validation or store read errors.
    pub async fn list_queue_heads(
        &self,
        request: crabber_session::QueueHeadsRequest,
    ) -> Result<crabber_session::QueueHeadsPage, crabber_session::StoreError> {
        self.runtime.list_queue_heads(request).await
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
        self.runtime
            .enqueue_queue(request, queue_id, id, options)
            .await
    }
    /// Claim or cancel a future item explicitly; replay never restores authority.
    /// # Errors
    /// Returns native queue scope, state, revision and storage failures.
    pub async fn mutate_queue(&self, request: QueueMutation) -> Result<QueueOutcome, RuntimeError> {
        self.runtime.mutate_queue(request).await
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
    /// Freeze the head's prepared binding under its exact live claim and latest CAS.
    /// # Errors
    /// Reconciles the original key before readiness/default reads or factories.
    pub async fn prepare_queue_in(
        &self,
        context: &SessionContext,
        control: QueueControl,
    ) -> Result<QueueReceipt, RuntimeError> {
        self.runtime
            .prepare_queue_from_context(
                self.default_request_in(context, String::new()),
                context.clone(),
                control,
            )
            .await
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
        Ok(Admission::from_runtime(
            self.runtime
                .admit_queue_from_context(
                    self.default_request_in(context, String::new()),
                    context.clone(),
                    control,
                    trace,
                )
                .await?,
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
        Ok(Admission::from_runtime(
            self.runtime
                .recover_queue_from_context(
                    self.default_request_in(context, String::new()),
                    context.clone(),
                    id.clone(),
                    trace,
                )
                .await?,
        ))
    }
}
