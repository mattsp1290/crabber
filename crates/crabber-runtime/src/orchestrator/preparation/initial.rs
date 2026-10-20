//! Initial admission's fence remains inside its original parked main.
use super::{DurablePreparation, Orchestrator, Request, RuntimeError, pending};
use crate::orchestrator::{InitialEntry, InitialRequest, RunSession};
use crabber_extension::RunPlan;
use crabber_providers::BoundProvider;
use crabber_session::{
    KeyedAdmitOutcome, StoreError, session_owner::preparation::PreparationCompletion,
};
use std::future::Future;

impl Orchestrator {
    pub(in crate::orchestrator) async fn finish_initial_preparation<F, W>(
        &self,
        prepared: DurablePreparation,
        request: Request,
        work: F,
    ) -> Result<pending::PendingOutcome, RuntimeError>
    where
        F: FnOnce(Option<super::scope::PlanScope>) -> W + Send + 'static,
        W: Future<
                Output = Result<
                    ((RunPlan, Option<BoundProvider>), super::PreparedOperation),
                    RuntimeError,
                >,
            > + Send
            + 'static,
    {
        let context = crabber_core::SessionContext::new(
            prepared.grant.owner_authority().session_id().clone(),
            request.workspace_id.clone(),
            request.directory.clone(),
        );
        self.finish_pending_preparation(
            prepared,
            context,
            work,
            move |(plan, bound), runtime, cancellation, completion| async move {
                let (admitted, entry) = match completion {
                    PreparationCompletion::Admitted(admitted) => (admitted, InitialEntry::Unkeyed),
                    PreparationCompletion::Keyed(KeyedAdmitOutcome::Started {
                        admitted, ..
                    })
                    | PreparationCompletion::Queue(crabber_session::QueueOutcome::Applied {
                        admitted: Some(admitted),
                        authority: None,
                        ..
                    }) => (*admitted, InitialEntry::Keyed),
                    _ => return Err(StoreError::AdmissionConflict.into()),
                };
                Box::pin(runtime.run(
                    admitted.fence,
                    RunSession::from(&admitted.session),
                    InitialRequest {
                        request,
                        entry,
                        bound,
                    },
                    plan,
                    cancellation,
                ))
                .await
            },
        )
        .await
    }
}
