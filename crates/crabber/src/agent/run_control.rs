use super::{Agent, RuntimeError};
use crabber_core::{ControlId, SessionId};
use crabber_session::{
    RunControlAction, RunControlError, RunControlOutcome, RunControlPage, RunControlReceipt,
    RunControlRequest, RunControlTarget,
};
impl Agent {
    /// Complete an original accepted cancellation after an authentic joined pause.
    /// Available after close; neither metadata reconciliation nor progress grants dispatch.
    /// # Errors
    /// Returns unsupported, conflict, stale revision, busy or unknown persistence.
    pub async fn settle_waiting_cancellation(
        &self,
        request: crabber_session::WaitingCancellation,
    ) -> Result<crabber_session::RunControlSummary, RunControlError> {
        self.runtime.settle_waiting_cancellation(request).await
    }

    /// Capture exact host target metadata and revision without reading private history.
    /// The host still authorizes the supplied session/run scope.
    /// # Errors
    /// Returns unsupported, missing target or bounded metadata failures.
    pub async fn run_control_binding(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
    ) -> Result<crabber_session::RunControlBinding, RunControlError> {
        self.runtime.run_control_binding(session, run).await
    }

    /// Persist exactly targeted cancellation or ordered next-turn user input.
    /// Cancellation remains an administrative action after close; fresh steer
    /// requires open ingress. Exact retries reconcile before ingress and CAS.
    /// # Errors
    /// Returns native control errors or closed ingress without running factories.
    pub async fn control_run(
        &self,
        request: RunControlRequest,
    ) -> Result<RunControlOutcome, RuntimeError> {
        if let Some(saved) = self
            .runtime
            .lookup_run_control(&request.target.session_id, &request.id)
            .await?
        {
            if saved.semantic_digest != request.fingerprint()? {
                return Err(RunControlError::SemanticConflict.into());
            }
            return Ok(RunControlOutcome::Replayed(saved));
        }
        if matches!(request.action, RunControlAction::Steer { .. })
            && (self.closed.load(std::sync::atomic::Ordering::SeqCst)
                || self
                    .contextual
                    .as_ref()
                    .is_some_and(|provider| provider.is_closed()))
        {
            return Err(RuntimeError::HostClosed);
        }
        Ok(self.runtime.control_run(request).await?)
    }
    /// Immutable metadata reconciliation, available after close.
    /// # Errors
    /// Returns unsupported or corrupt storage evidence.
    pub async fn lookup_run_control(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<RunControlReceipt>, RunControlError> {
        self.runtime.lookup_run_control(session, id).await
    }
    /// Coherent bounded safe metadata, available after close.
    /// # Errors
    /// Returns unsupported, scope or storage failures.
    pub async fn run_controls(
        &self,
        target: &RunControlTarget,
    ) -> Result<RunControlPage, RunControlError> {
        self.runtime.run_controls(target).await
    }
}
