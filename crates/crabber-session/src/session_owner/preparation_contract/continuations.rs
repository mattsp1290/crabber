//! Real native waiting sources, original control CAS and exact recovery lineage.
use super::super::preparation::{ExistingWorkTarget, PreparationKey, PreparationSource};
use super::*;
use crabber_core::{Clock, ManualClock, Run};
pub(crate) async fn request(
    store: &impl Store,
    run: &Run,
    source: PreparationSource,
    key: PreparationKey,
    work: ExistingWorkTarget,
    intent: InputFingerprint,
) -> ReservePreparation {
    let binding = store
        .execution_binding(&run.session_id, &run.id)
        .await
        .unwrap()
        .unwrap()
        .capsule;
    let mut owner = crate::session_owner_contract::reservation(run.session_id.clone());
    owner.binding.workspace_id = binding.request.workspace_id;
    owner.binding.directory = binding.request.directory;
    owner.binding.behavior_fingerprint = binding.behavior_fingerprint;
    ReservePreparation {
        owner,
        key: key.clone(),
        source: Some(source),
        resource_contract: None,
        expected_revision: store.session_revision(&run.session_id).await.unwrap(),
        purpose: PreparationPurpose::ExistingRun {
            run_id: run.id.clone(),
            work,
        },
        intent,
    }
    .bind_key(key)
    .unwrap()
}
pub(super) async fn complete(
    store: &impl Store,
    request: &ReservePreparation,
    operation: PreparedOperation,
) -> PreparationCompletion {
    let grant = begin(store, request).await;
    let ((), joined) = JoinedPreparation::observe(
        grant.clone(),
        tokio::spawn(async move { Ok::<_, ()>(((), operation)) }),
    )
    .await
    .unwrap()
    .unwrap();
    let outcome = store.complete_preparation(joined.clone()).await.unwrap();
    assert_eq!(
        store
            .get_session_owner(
                &request.owner.binding.session_id,
                &grant.owner_authority().target().id
            )
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    let revision = store
        .session_revision(&request.owner.binding.session_id)
        .await
        .unwrap();
    let replay = store.complete_preparation(joined).await.unwrap();
    assert!(replay.is_replayed());
    assert_eq!(
        store
            .session_revision(&request.owner.binding.session_id)
            .await
            .unwrap(),
        revision
    );
    outcome
}
pub(crate) async fn approval(store: &impl Store, clock: &ManualClock) {
    use crate::approval_contract::fixture::{answer, claim, staged};
    let f = staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let original = claim(store, &f).await;
    let pending = request(
        store,
        &f.run,
        PreparationSource::ApprovalClaim(original.clone()),
        PreparationKey::Approval(original.id.clone()),
        ExistingWorkTarget::ApprovalClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    assert_eq!(
        store
            .reserve_preparation(pending.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::ApprovalSource(crate::ApprovalError::Pending)
    );
    assert!(
        store
            .lookup_preparation(&f.run.session_id, &pending.key)
            .await
            .unwrap()
            .is_none()
    );
    for index in 0..2 {
        store
            .answer_approval(answer(store, &f, index, crate::ApprovalReply::Grant).await)
            .await
            .unwrap();
    }
    let mut original = claim(store, &f).await;
    original.lease_ms = 1;
    let reserve = request(
        store,
        &f.run,
        PreparationSource::ApprovalClaim(original.clone()),
        PreparationKey::Approval(original.id.clone()),
        ExistingWorkTarget::ApprovalClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied { receipt, .. }) =
        complete(
            store,
            &reserve,
            PreparedOperation::ApprovalClaim(original.clone()),
        )
        .await
    else {
        panic!("fresh claim")
    };
    assert_eq!(receipt.semantic_digest, original.fingerprint().unwrap());
    assert_eq!(
        receipt.revision,
        store.session_revision(&f.run.session_id).await.unwrap()
    );
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let recovery = crate::ApprovalContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: original.clone(),
        attempt: f.batch.next_attempt.clone(),
        owner: "recovered".into(),
        lease_ms: 30000,
    };
    let reserve = request(
        store,
        &f.run,
        PreparationSource::ApprovalRecovery(recovery.clone()),
        PreparationKey::Approval(recovery.id.clone()),
        ExistingWorkTarget::ApprovalRecovery {
            continuation: original.continuation.clone(),
            attempt: recovery.attempt.clone(),
        },
        recovery.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied { receipt, .. }) =
        complete(
            store,
            &reserve,
            PreparedOperation::ApprovalRecovery(recovery.clone()),
        )
        .await
    else {
        panic!("fresh recovery")
    };
    recovery.verify_receipt(&receipt).unwrap();
    assert_eq!(
        receipt.revision,
        store.session_revision(&f.run.session_id).await.unwrap()
    );
}
pub(crate) async fn tools(store: &impl Store, clock: &ManualClock) {
    use crate::tool_continuation_contract::fixture::{claim, fixture};
    let f = fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    let run = &f.admitted.run;
    let pending = claim(store, &f).await;
    let reserve = request(
        store,
        run,
        PreparationSource::ToolClaim(pending.clone()),
        PreparationKey::ToolContinuation(pending.id.clone()),
        ExistingWorkTarget::ToolClaim(pending.continuation.clone()),
        pending.fingerprint().unwrap(),
    )
    .await;
    assert_eq!(
        store
            .reserve_preparation(reserve.clone())
            .await
            .unwrap_err(),
        SessionOwnerError::ToolSource(crate::ToolContinuationError::Pending)
    );
    assert!(
        store
            .lookup_preparation(&run.session_id, &reserve.key)
            .await
            .unwrap()
            .is_none()
    );
    answer_tools(store, &f).await;
    let mut original = claim(store, &f).await;
    original.lease_ms = 1;
    let reserve = request(
        store,
        run,
        PreparationSource::ToolClaim(original.clone()),
        PreparationKey::ToolContinuation(original.id.clone()),
        ExistingWorkTarget::ToolClaim(original.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(crate::ToolContinuationClaimOutcome::Applied {
        receipt,
        ..
    }) = complete(
        store,
        &reserve,
        PreparedOperation::ToolClaim(original.clone()),
    )
    .await
    else {
        panic!("fresh claim")
    };
    assert_eq!(receipt.semantic_digest, original.fingerprint().unwrap());
    assert_eq!(
        receipt.revision,
        store.session_revision(&run.session_id).await.unwrap()
    );
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let recovery = crate::ToolContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: original.clone(),
        attempt: f.suspension.next_attempt.clone(),
        owner: "recovered".into(),
        lease_ms: 30000,
    };
    let reserve = request(
        store,
        run,
        PreparationSource::ToolRecovery(recovery.clone()),
        PreparationKey::ToolContinuation(recovery.id.clone()),
        ExistingWorkTarget::ToolRecovery {
            continuation: original.continuation.clone(),
            attempt: recovery.attempt.clone(),
        },
        recovery.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(crate::ToolContinuationClaimOutcome::Applied {
        receipt,
        ..
    }) = complete(
        store,
        &reserve,
        PreparedOperation::ToolRecovery(recovery.clone()),
    )
    .await
    else {
        panic!("fresh recovery")
    };
    recovery.verify_receipt(&receipt).unwrap();
    assert_eq!(
        receipt.revision,
        store.session_revision(&run.session_id).await.unwrap()
    );
}

async fn answer_tools(store: &impl Store, f: &crate::tool_continuation_contract::fixture::Fixture) {
    for index in 0..2 {
        store
            .answer_tool_interaction(
                crate::tool_continuation_contract::fixture::answer(store, f, index).await,
            )
            .await
            .unwrap();
    }
}
