//! Concrete owned Queue prelude; callers provide values, never futures or gates.
use super::{Admission, Orchestrator, QueueControl, Request, RuntimeError};
use crabber_core::{QueueId, SessionContext, TraceContext};
use crabber_session::{QueueReceipt, StoreError};
impl Orchestrator {
    async fn request_from_queue(
        &self,
        mut fallback: Request,
        context: &SessionContext,
        id: &QueueId,
    ) -> Result<(Request, crabber_core::AdmissionOptions), RuntimeError> {
        if fallback.session_id.as_ref() != Some(context.session_id())
            || fallback.workspace_id != context.workspace_id()
            || fallback.directory != context.directory()
        {
            return Err(StoreError::SessionIdentityMismatch.into());
        }
        let retained = self.queued_intent(context, id).await?;
        let data = &retained.input.request;
        fallback.text.clone_from(&data.text);
        fallback.model_controls.clone_from(&data.model_controls);
        if data.selection_policy.is_explicit() {
            fallback.selection_policy = data.selection_policy.clone();
            fallback.selection.provider_id.clone_from(&data.provider_id);
            fallback.selection.model_id.clone_from(&data.model_id);
        }
        fallback.input_envelope = Some(retained.input.input);
        Ok((fallback, retained.input.admission))
    }
    /// Reconstruct and prepare the actual retained head inside the owned admission.
    /// # Errors
    /// Refuses closed fresh intake, changed context, or invalid Queue authority.
    pub async fn prepare_queue_from_context(
        &self,
        fallback: Request,
        context: SessionContext,
        control: QueueControl,
    ) -> Result<QueueReceipt, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            let (request, _) = self
                .request_from_queue(fallback, &context, &control.authority.queue_id)
                .await?;
            return self
                .replay_queue_prepare(&request, &control)
                .await?
                .ok_or(refusal);
        }
        self.own_preparation(context.session_id().clone(), move |runtime| async move {
            let (request, _) = runtime
                .request_from_queue(fallback, &context, &control.authority.queue_id)
                .await?;
            Box::pin(runtime.prepare_queue_owned(request, control)).await
        })
        .await
    }
    /// Reconstruct and admit only the original retained head under ownership.
    /// # Errors
    /// Refuses closed fresh intake or invalid original Queue and admission data.
    pub async fn admit_queue_from_context(
        &self,
        fallback: Request,
        context: SessionContext,
        control: QueueControl,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            let (request, _) = self
                .request_from_queue(fallback, &context, &control.authority.queue_id)
                .await?;
            return self
                .replay_queue_admit(&request, &control)
                .await?
                .map(Admission::Replayed)
                .ok_or(refusal);
        }
        self.own_preparation(context.session_id().clone(), move |runtime| async move {
            let runtime = runtime.with_context(trace);
            let (request, _) = runtime
                .request_from_queue(fallback, &context, &control.authority.queue_id)
                .await?;
            Box::pin(runtime.admit_queue_owned(request, control)).await
        })
        .await
    }
    /// Recover an original Queue winner after owned reconstruction and validation.
    /// # Errors
    /// Refuses closed fresh recovery or missing and inconsistent original data.
    pub async fn recover_queue_from_context(
        &self,
        fallback: Request,
        context: SessionContext,
        queue: QueueId,
        trace: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if let Some(refusal) = self.preparation_entry_refusal() {
            let (request, options) = self.request_from_queue(fallback, &context, &queue).await?;
            return self
                .replay_started_admission(&request, &options)
                .await?
                .map(Admission::Replayed)
                .ok_or(refusal);
        }
        self.own_preparation(context.session_id().clone(), move |runtime| async move {
            let (request, options) = runtime
                .request_from_queue(fallback, &context, &queue)
                .await?;
            runtime
                .recover_retained_admission(request, options, trace)
                .await
        })
        .await
    }
}
