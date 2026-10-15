//! Phase reconciliation controls remain available after extension shutdown.
use super::Agent;
use crabber_core::{ControlId, PhaseId, RunId, SessionId};
use crabber_session::{
    PhaseError, PhaseReceipt, PhaseResolution, PhaseResolutionOutcome, VersionedPhase,
};

impl Agent {
    /// Record the host's verified external outcome and final protected output.
    /// Does not execute tools or result middleware, renew ownership, or assert
    /// resource cleanup. Refusal keeps the phase uncertain; retries replay.
    /// # Errors
    /// Returns unsupported, scope, revision, state, semantic or storage failures.
    pub async fn resolve_uncertain_phase(
        &self,
        resolution: PhaseResolution,
    ) -> Result<PhaseResolutionOutcome, PhaseError> {
        self.runtime.resolve_uncertain_phase(resolution).await
    }
    /// Read phase identity, state and cleanup without creating extension instances.
    /// # Errors
    /// Returns unsupported, missing scope or inconsistent storage evidence.
    pub async fn tool_phase(
        &self,
        run: &RunId,
        phase: &PhaseId,
    ) -> Result<VersionedPhase, PhaseError> {
        self.runtime.tool_phase(run, phase).await
    }
    /// Reconcile the original operation key without granting execution authority.
    /// # Errors
    /// Returns unsupported, key-domain conflict or inconsistent storage evidence.
    pub async fn lookup_phase_mutation(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PhaseReceipt>, PhaseError> {
        self.runtime.lookup_phase_mutation(session, id).await
    }
}
