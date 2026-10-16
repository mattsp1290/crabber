use super::{Store, fixture};
use crate::*;
use crabber_core::{
    ControlId, FrontendResultStatus, FrontendToolResult, ManualClock, MessageContent, ToolOutput,
};
use std::sync::Arc;
use time::OffsetDateTime;
pub(crate) async fn run<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    let (f, batch, paused) = fixture::paused(&store).await;
    let mut claim = fixture::external_claim(&f.request, &batch, paused.revision);
    claim.results = vec![
        FrontendToolResult::new(
            f.request.call.id.clone(),
            FrontendResultStatus::Completed,
            ToolOutput::new(
                vec![MessageContent::Text {
                    text: "x".repeat(2_200_000),
                }],
                false,
            )
            .unwrap(),
        )
        .unwrap(),
    ];
    let ToolContinuationClaimOutcome::Applied { receipt, .. } = store
        .claim_frontend_continuation(claim.clone())
        .await
        .unwrap()
    else {
        panic!("fresh large claim")
    };
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(2));
    let recovery = FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: claim,
        attempt: batch.next_attempt,
        owner: "large-result-recovery".into(),
        lease_ms: 30000,
    };
    let ToolContinuationClaimOutcome::Applied {
        receipt: recovered,
        grant,
    } = store
        .recover_frontend_continuation(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("fresh large recovery")
    };
    assert_eq!(grant.frontend[0].result, recovery.original.results[0]);
    assert!(
        matches!(store.recover_frontend_continuation(recovery).await.unwrap(),ToolContinuationClaimOutcome::Replayed(saved) if saved==recovered)
    );
}
