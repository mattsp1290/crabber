//! Reconcile complete ACKs against independent native metadata, before notice.
use super::{
    Error, Origin, PendingSuccessorError, PendingSuccessorReplay, Source, receipt::Receipt,
};
use crate::{
    Store,
    session_owner::{
        driver::RegisterSuccessorExecutionDriver,
        preparation::{
            InspectPreparedApprovalDriver, InspectPreparedToolDriver, PreparationCommitError,
        },
    },
};
use crabber_core::SessionContext;
impl Source {
    fn approval(&self) -> bool {
        matches!(
            self.origin,
            Origin::ApprovalClaim | Origin::ApprovalRecovery
        )
    }
    pub(super) async fn lookup(
        &self,
        store: &dyn Store,
        context: &SessionContext,
    ) -> Result<Option<PendingSuccessorReplay>, PendingSuccessorError> {
        let metadata = if self.approval() {
            store
                .lookup_approval(context.session_id(), &self.id)
                .await
                .map_err(PreparationCommitError::from)?
                .map(|r| self.metadata(Receipt::Approval(&r), context))
        } else {
            store
                .lookup_tool_continuation(context.session_id(), &self.id)
                .await
                .map_err(PreparationCommitError::from)?
                .map(|r| self.metadata(Receipt::Tool(&r), context))
        };
        Ok(metadata.transpose()?)
    }
    pub(super) async fn inspect(
        &self,
        store: &dyn Store,
        context: &SessionContext,
        request: &RegisterSuccessorExecutionDriver,
    ) -> Result<PendingSuccessorReplay, PendingSuccessorError> {
        if self.approval() {
            let inspected = store
                .inspect_prepared_approval_driver(InspectPreparedApprovalDriver {
                    request: request.clone(),
                    origin: self.origin,
                    operation_id: self.id.clone(),
                    semantic_digest: self.digest.clone(),
                })
                .await?;
            if inspected.claim != request.claim {
                return Err(Error::SemanticConflict.into());
            }
            Ok(self.metadata(Receipt::Approval(&inspected.receipt), context)?)
        } else {
            let inspected = store
                .inspect_prepared_tool_driver(InspectPreparedToolDriver {
                    request: request.clone(),
                    origin: self.origin,
                    operation_id: self.id.clone(),
                    semantic_digest: self.digest.clone(),
                })
                .await?;
            if inspected.claim != request.claim {
                return Err(Error::SemanticConflict.into());
            }
            Ok(self.metadata(Receipt::Tool(&inspected.receipt), context)?)
        }
    }
}
