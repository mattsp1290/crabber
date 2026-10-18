//! Separate retained registries share the root cut without substituting for it.
use super::preparation::retained::PreparationCloseSelection;
use super::{Orchestrator, RetainedPreparationProgress};
use crate::{
    session_invocations::{
        InvocationCloseSelection, SessionInvocationReport, SessionInvocationStatus,
    },
    session_services::{ServiceCloseSelection, SessionServiceReport, SessionServiceStatus},
};

/// Actual local dependent observations, independent of native settlement.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeDependentCloseReport {
    pub invocations: Vec<SessionInvocationReport>,
    pub services: Vec<SessionServiceReport>,
    pub preparations: Vec<RetainedPreparationProgress>,
}
impl RuntimeDependentCloseReport {
    /// True only when all selected original supervisors and resources joined.
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.preparations.is_empty()
            && self.invocations.iter().all(|report| {
                report.local_joined
                    && matches!(
                        report.status,
                        SessionInvocationStatus::Joined
                            | SessionInvocationStatus::Replayed
                            | SessionInvocationStatus::Rejected
                    )
                    && resources_drained(&report.resources)
            })
            && self.services.iter().all(|report| {
                report.local_joined
                    && !report.supervisor_failed
                    && matches!(
                        report.status,
                        SessionServiceStatus::Joined
                            | SessionServiceStatus::Replayed
                            | SessionServiceStatus::Cancelled
                            | SessionServiceStatus::Rejected
                    )
                    && resources_drained(&report.resources)
            })
    }
}
fn resources_drained(report: &crabber_extension::ResourceDrain) -> bool {
    report.sealed && report.pending == 0 && report.failures.is_empty()
}
pub(super) struct CloseDependents {
    runtime: Orchestrator,
    invocations: InvocationCloseSelection,
    services: ServiceCloseSelection,
    preparations: PreparationCloseSelection,
}
impl Orchestrator {
    pub(super) fn begin_dependent_close(&self) -> CloseDependents {
        CloseDependents {
            runtime: self.clone(),
            invocations: self.session_invocations.begin_close(),
            services: self.session_services.begin_close(),
            preparations: self.retained_preparations.begin_close(),
        }
    }
    pub(super) fn is_current_dependent_owner(&self) -> bool {
        self.session_invocations.is_current_owner()
            || self.session_services.is_current_owner()
            || self.retained_preparations.is_current_callback_parent()
    }
}
impl CloseDependents {
    pub(super) fn signal_all(&self) {
        self.invocations.signal_all();
        self.services.signal_all();
        self.preparations.signal_all();
    }
    pub(super) fn snapshot(&self) -> RuntimeDependentCloseReport {
        RuntimeDependentCloseReport {
            invocations: self.runtime.session_invocations.snapshot_close(),
            services: self.runtime.session_services.snapshot_close(),
            preparations: self.preparations.snapshot(),
        }
    }
    pub(super) async fn observe_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> RuntimeDependentCloseReport {
        self.signal_all();
        let (invocations, services, preparations) = tokio::join!(
            self.invocations.observe_until(deadline),
            self.services.observe_until(deadline),
            self.preparations.observe_until(&self.runtime, deadline),
        );
        RuntimeDependentCloseReport {
            invocations,
            services,
            preparations,
        }
    }
}
