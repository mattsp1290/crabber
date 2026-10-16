//! Worker-owned durable control observation. No detached control dispatcher.
use super::{Orchestrator, RuntimeError};
use crabber_core::{ControlId, SessionId};
use crabber_session::{
    ExecutionStore, RunControlError, RunControlOutcome, RunControlPage, RunControlReceipt,
    RunControlRequest, RunControlTarget,
};
impl Orchestrator {
    /// Complete an original accepted cancellation after an authentic joined pause.
    /// Available after close; neither metadata reconciliation nor progress grants dispatch.
    /// # Errors
    /// Returns unsupported, conflict, stale revision, busy or unknown persistence.
    pub async fn settle_waiting_cancellation(
        &self,
        request: crabber_session::WaitingCancellation,
    ) -> Result<crabber_session::RunControlSummary, RunControlError> {
        self.store.settle_waiting_cancellation(request).await
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
        self.store.run_control_binding(session, run).await
    }

    /// Persist targeted cancel/steer acceptance before the worker can observe it.
    /// # Errors
    /// Returns unsupported, scope, revision, conflict or unknown storage outcomes.
    pub async fn control_run(
        &self,
        request: RunControlRequest,
    ) -> Result<RunControlOutcome, RunControlError> {
        self.store.control_run(request).await
    }
    /// Read immutable original acceptance evidence without execution authority.
    /// # Errors
    /// Returns unsupported or corrupt storage evidence.
    pub async fn lookup_run_control(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<RunControlReceipt>, RunControlError> {
        self.store.lookup_run_control(session, id).await
    }
    /// Read coherent bounded statuses; accepted/observed do not imply settlement.
    /// # Errors
    /// Returns unsupported, missing target, binding or storage failures.
    pub async fn run_controls(
        &self,
        target: &RunControlTarget,
    ) -> Result<RunControlPage, RunControlError> {
        self.store.run_controls(target).await
    }
    /// The caller has reached a durable joined pause; the Store independently
    /// authenticates readiness rather than trusting the host cancellation claim.
    pub(super) async fn finish_paused_cancellation(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Option<crabber_core::RunStatus>, RuntimeError> {
        if !self.store.supports_run_controls() {
            return Ok(None);
        }
        let target = self.store.run_control_binding(session, run).await?.target;
        for _ in 0..4 {
            let page = self.store.run_controls(&target).await?;
            let cancel = page
                .controls
                .iter()
                .find(|c| c.receipt.kind == crabber_session::RunControlKind::Cancel);
            let Some(cancel) = cancel else {
                return Ok(None);
            };
            if let crabber_session::RunControlStatus::TerminalCommitted { status } = cancel.status {
                return Ok(Some(status));
            }
            let request = crabber_session::WaitingCancellation {
                target: target.clone(),
                original_cancel_id: cancel.receipt.operation_id.clone(),
                expected_revision: page.revision,
            };
            match self.store.settle_waiting_cancellation(request).await {
                Ok(summary) => {
                    if let crabber_session::RunControlStatus::TerminalCommitted { status } =
                        summary.status
                    {
                        return Ok(Some(status));
                    }
                    return Err(RunControlError::UnknownStoreOutcome.into());
                }
                Err(RunControlError::RevisionConflict) => (),
                Err(RunControlError::Busy) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        Err(RunControlError::RevisionConflict.into())
    }
    pub(super) async fn wait_durable_cancel(
        &self,
        execution: &dyn ExecutionStore,
    ) -> Result<(), RuntimeError> {
        if !self.store.supports_run_controls() {
            return std::future::pending().await;
        }
        let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            match execution.observe_run_controls(None).await {
                Ok(boundary) if boundary.cancel_observed => return Ok(()),
                Ok(_) => (),
                Err(RunControlError::StaleOwner) => return Err(RuntimeError::LeaseLost),
                Err(error) => return Err(error.into()),
            }
        }
    }
}
