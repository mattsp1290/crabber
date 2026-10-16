use super::{Store, ToolContinuationClaimOutcome, ToolContinuationError, fixture};
pub(super) async fn run(store: &impl Store) {
    let f = fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        let answer = fixture::answer(store, &f, index).await;
        let (a, b) = futures::join!(
            store.answer_tool_interaction(answer.clone()),
            store.answer_tool_interaction(answer)
        );
        assert_eq!(a.unwrap(), b.unwrap());
    }
    let claim = fixture::claim(store, &f).await;
    let (a, b) = futures::join!(
        store.claim_tool_continuation(claim.clone()),
        store.claim_tool_continuation(claim)
    );
    assert!(matches!(
        (a.unwrap(), b.unwrap()),
        (
            ToolContinuationClaimOutcome::Applied { .. },
            ToolContinuationClaimOutcome::Replayed(_)
        ) | (
            ToolContinuationClaimOutcome::Replayed(_),
            ToolContinuationClaimOutcome::Applied { .. }
        )
    ));
    let f = fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_tool_interaction(fixture::answer(store, &f, index).await)
            .await
            .unwrap();
    }
    let first = fixture::claim(store, &f).await;
    let mut second = first.clone();
    second.id = crabber_core::ControlId::new();
    let (a, b) = futures::join!(
        store.claim_tool_continuation(first),
        store.claim_tool_continuation(second)
    );
    assert!(matches!(
        (a, b),
        (
            Ok(ToolContinuationClaimOutcome::Applied { .. }),
            Err(ToolContinuationError::RevisionConflict)
        ) | (
            Err(ToolContinuationError::RevisionConflict),
            Ok(ToolContinuationClaimOutcome::Applied { .. })
        )
    ));
}
