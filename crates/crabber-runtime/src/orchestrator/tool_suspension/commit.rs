//! Retry only a proven rolled-back revision conflict; never infer ACK loss rollback.
use super::{
    Batch, ControlId, Error, Orchestrator, RuntimeError, ToolContinuationSuspension,
    ToolSuspensionAuthority,
};
impl Orchestrator {
    pub(super) async fn persist_tool_suspension(
        &self,
        scope: &Batch<'_>,
        mut batch: ToolContinuationSuspension,
        authority: ToolSuspensionAuthority,
    ) -> Result<crabber_core::EventRecord, RuntimeError> {
        // The caller has joined every phase/run resource and this path either
        // returns a genuine pause or a typed boundary error, never more effects.
        let _observation = self.observation_lifetime.quiescent_commit();
        for retry in 0..3 {
            let Ok(Some(stored)) = self.store.get_run(scope.run).await else {
                return Err(self.fail_tool_suspensions(scope).await);
            };
            let pause = batch.conclusion(&stored)?.materialize(&stored)?;
            let digest = batch.fingerprint(&scope.session.id, scope.run)?;
            let key = batch.id.clone();
            let unknown = || RuntimeError::ToolContinuationBoundaryUnknown {
                run_id: scope.run.clone(),
                operation_id: key.clone(),
                reason: Error::UnknownStoreOutcome,
            };
            let result = scope
                .execution
                .suspend_tool_continuation(batch.clone(), authority.clone())
                .await;
            let receipt = match result {
                Ok(receipt) => receipt,
                Err(Error::RevisionConflict) if retry < 2 => {
                    let Ok(revision) = self.store.session_revision(&scope.session.id).await else {
                        return Err(self.fail_tool_suspensions(scope).await);
                    };
                    batch.id = ControlId::new();
                    batch.expected_revision = revision;
                    continue;
                }
                Err(Error::UnknownStoreOutcome) => match self
                    .store
                    .lookup_tool_continuation(&scope.session.id, &key)
                    .await
                {
                    Ok(Some(receipt)) => receipt,
                    Ok(None) | Err(_) => return Err(unknown()),
                },
                // Finite adapter rejections guarantee that this operation did not
                // commit. Actual local owners already joined all proposal resources.
                Err(_) => return Err(self.fail_tool_suspensions(scope).await),
            };
            if receipt.semantic_digest != digest
                || receipt.operation_id != key
                || receipt.session_id != scope.session.id
                || receipt.run_id != *scope.run
            {
                return Err(unknown());
            }
            return Ok(pause);
        }
        unreachable!("bounded retry returns on its final iteration")
    }
}
