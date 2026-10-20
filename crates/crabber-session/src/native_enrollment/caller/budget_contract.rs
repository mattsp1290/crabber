//! Real wrapper precharge, including authenticated private barrier bodies.
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError, AttemptMutation, AttemptMutationOutcome,
};
use crabber_core::{ControlId, ManualClock, RunFence};

pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    let admitted =
        crate::public_state_contract::admit(store, time::OffsetDateTime::UNIX_EPOCH).await;
    let request = crate::attempt_contract::reserve(
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    limited(store, admitted.fence, request, None, 0).await;
    Box::pin(tool(store)).await;
    Box::pin(approval(store, clock)).await;
}
async fn limited(
    store: &impl Harness,
    fence: RunFence,
    request: AttemptMutation,
    authority: Option<AttemptAuthority>,
    remaining: usize,
) {
    let run = store.get_run(&fence.run_id).await.unwrap().unwrap();
    let before = store.retained(&run.session_id, &run.id, false).await;
    store.probe().arm(&run.session_id, &request.id, false);
    let mut proof = Proof::bounded(32 << 20);
    proof.charge((32 << 20) - remaining).unwrap();
    assert_eq!(
        store
            .attempt_with_proof(
                fence.clone(),
                request.clone(),
                authority.clone(),
                &mut proof
            )
            .await
            .unwrap_err(),
        AttemptError::UnknownStoreOutcome
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(
        store.retained(&run.session_id, &run.id, false).await,
        before
    );
    // Identical genuine operation works with the full caller allowance.
    assert!(matches!(
        store
            .attempt_with_proof(fence, request, authority, &mut Proof::bounded(32 << 20))
            .await
            .unwrap(),
        AttemptMutationOutcome::Applied { .. }
    ));
    assert_eq!(store.probe().hits().len(), 1);
}
async fn tool(store: &impl Harness) {
    use crate::tool_continuation_contract::fixture;
    let mut f = Box::pin(fixture::fixture(store)).await;
    f.suspension.phases[0].checkpoint = crabber_core::ToolCheckpoint::new(
        "fixture",
        "1",
        serde_json::json!({"private": "x".repeat(48 << 10)}),
    )
    .unwrap();
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
    let claim = fixture::claim(store, &f).await;
    let crate::ToolContinuationClaimOutcome::Applied { receipt, grant } =
        store.claim_tool_continuation(claim).await.unwrap()
    else {
        panic!("genuine fresh tool claim");
    };
    let crate::ToolContinuationOutcome::Claimed { attempt, .. } = receipt.outcome else {
        panic!("actual tool claim receipt");
    };
    let request = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: attempt.target,
        },
    };
    limited(store, grant.fence, request, Some(grant.authority), 32 << 10).await;
}
async fn approval(store: &impl Harness, clock: &ManualClock) {
    use crate::approval_contract::fixture;
    let mut f = Box::pin(fixture::staged(store, clock)).await;
    for call in &mut f.batch.calls {
        if let crate::CallApproval::Ask { description, .. } = &mut call.decision {
            *description = crabber_core::InteractionDescription::new(
                "approve",
                crabber_core::InteractionKind::Approval,
                "p".repeat(8192),
                serde_json::json!({"type":"boolean"}),
                description.authority().clone(),
                Some(1000),
            )
            .unwrap();
        }
    }
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_approval(fixture::answer(store, &f, index, crate::ApprovalReply::Grant).await)
            .await
            .unwrap();
    }
    let claim = fixture::claim(store, &f).await;
    let crate::ApprovalClaimOutcome::Applied { receipt, grant } =
        store.claim_approval_continuation(claim).await.unwrap()
    else {
        panic!("genuine fresh approval claim");
    };
    let crate::ApprovalOutcome::Claimed { attempt, .. } = receipt.outcome else {
        panic!("actual approval claim receipt");
    };
    let request = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: attempt.target,
        },
    };
    limited(store, grant.fence, request, Some(grant.authority), 32 << 10).await;
}
