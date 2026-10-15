//! Host controls use Store transactions without initializing execution plans.
use super::Orchestrator;
use crabber_core::{ControlId, PhaseId, RunId, SessionId};
use crabber_session::{
    PhaseError, PhaseReceipt, PhaseResolution, PhaseResolutionOutcome, VersionedPhase,
};

impl Orchestrator {
    /// Cancel or expire a waiting approval batch after its worker has joined.
    /// Remains available without initializing execution resources.
    /// # Errors
    /// Returns unsupported, scope, revision, state or storage errors.
    pub async fn terminate_approval(
        &self,
        request: crabber_session::ApprovalTermination,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.store.terminate_approval(request).await
    }
    /// Record lost or host-asserted stopped ownership without dispatch.
    /// # Errors
    /// Returns unsupported, invalid scope, revision, state or storage errors.
    pub async fn mark_phase_uncertain(
        &self,
        request: crabber_session::PhaseUncertainty,
    ) -> Result<crabber_session::PhaseControlOutcome, PhaseError> {
        self.store.mark_phase_uncertain(request).await
    }
    /// Record exact cleanup evidence independently of outcome resolution.
    /// # Errors
    /// Returns unsupported, invalid scope, revision, state or storage errors.
    pub async fn record_phase_cleanup(
        &self,
        request: crabber_session::PhaseCleanup,
    ) -> Result<crabber_session::PhaseControlOutcome, PhaseError> {
        self.store.record_phase_cleanup(request).await
    }
    /// Read a bounded coherent phase page without tokens or output bodies.
    /// # Errors
    /// Returns unsupported, invalid scope, revision, state or storage errors.
    pub async fn list_tool_phases(
        &self,
        request: crabber_session::PhasePageRequest,
    ) -> Result<crabber_session::PhasePage, PhaseError> {
        self.store.list_tool_phases(request).await
    }

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
