//! Host controls use Store transactions without initializing execution plans.
use super::Orchestrator;
use crabber_core::{ControlId, PhaseId, RunId, SessionId};
use crabber_session::{
    PhaseError, PhaseReceipt, PhaseResolution, PhaseResolutionOutcome, VersionedPhase,
};

impl Orchestrator {
    /// Resolve an uncertain outcome after host verification of the resolver and
    /// supplied protected output. Preserves cleanup; never dispatches work.
    /// # Errors
    /// Returns unsupported, scope, revision, state, semantic or storage failures.
    pub async fn resolve_uncertain_phase(
        &self,
        resolution: PhaseResolution,
    ) -> Result<PhaseResolutionOutcome, PhaseError> {
        self.store.resolve_uncertain_phase(resolution).await
    }
    /// Read native phase metadata without acquiring worker authority.
    /// # Errors
    /// Returns unsupported, missing scope or inconsistent storage evidence.
    pub async fn tool_phase(
        &self,
        run: &RunId,
        phase: &PhaseId,
    ) -> Result<VersionedPhase, PhaseError> {
        self.store.tool_phase(run, phase).await
    }
    /// Reconcile an original operation key without dispatch or factory setup.
    /// # Errors
    /// Returns unsupported, key-domain conflict or inconsistent storage evidence.
    pub async fn lookup_phase_mutation(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PhaseReceipt>, PhaseError> {
        self.store.lookup_phase_mutation(session, id).await
    }
}
