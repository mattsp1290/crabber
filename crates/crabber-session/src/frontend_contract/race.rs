use super::{Store, fixture};
use crate::*;
use crabber_core::ControlId;
pub(crate) async fn run(store: &dyn Store) {
    let (f, batch, paused) = fixture::paused(store).await;
    let claim = fixture::external_claim(&f.request, &batch, paused.revision);
    let (a, b) = tokio::join!(
        store.claim_frontend_continuation(claim.clone()),
        store.claim_frontend_continuation(claim.clone())
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(
        [&a, &b]
            .iter()
            .filter(|o| matches!(o, ToolContinuationClaimOutcome::Applied { .. }))
            .count(),
        1
    );
    assert_eq!(
        [&a, &b]
            .iter()
            .filter(|o| matches!(o, ToolContinuationClaimOutcome::Replayed(_)))
            .count(),
        1
    );
    let (f, batch, paused) = fixture::paused(store).await;
    let claim = fixture::external_claim(&f.request, &batch, paused.revision);
    let mut other = claim.clone();
    other.claim.id = ControlId::new();
    let (a, b) = tokio::join!(
        store.claim_frontend_continuation(claim),
        store.claim_frontend_continuation(other)
    );
    assert_eq!(
        [a, b]
            .iter()
            .filter(|o| matches!(o, Ok(ToolContinuationClaimOutcome::Applied { .. })))
            .count(),
        1
    );
    assert_eq!(
        store.session_revision(&f.request.session_id).await.unwrap(),
        paused.revision.next().unwrap()
    );
}
