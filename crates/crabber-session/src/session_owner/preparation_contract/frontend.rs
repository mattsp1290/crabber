//! Compound frontend results remain original semantics across owned preparation.
use super::super::preparation::{ExistingWorkTarget, PreparationKey, PreparationSource};
use super::*;
use crabber_core::{Clock, ManualClock};

pub(crate) async fn run(store: &dyn Store, clock: &ManualClock) {
    let fixture = crate::frontend_contract::fixture::fixture(store).await;
    fixture
        .execution
        .stage_frontend_call(fixture.request.clone(), fixture.authority.clone())
        .await
        .unwrap();
    let batch = crate::frontend_contract::fixture::suspension(store, &fixture.request).await;
    let paused = fixture
        .execution
        .suspend_tool_continuation(
            batch.clone(),
            crate::ToolSuspensionAuthority {
                attempt: fixture.authority,
                phases: Vec::new(),
            },
        )
        .await
        .unwrap();
    let run = store
        .get_run(&fixture.request.run_id)
        .await
        .unwrap()
        .unwrap();
    let mut original = crate::frontend_contract::fixture::external_claim(
        &fixture.request,
        &batch,
        paused.revision,
    );
    original.claim.lease_ms = 1;
    let reserve = super::continuations::request(
        store,
        &run,
        PreparationSource::FrontendClaim(original.clone()),
        PreparationKey::ToolContinuation(original.claim.id.clone()),
        ExistingWorkTarget::FrontendClaim(original.claim.continuation.clone()),
        original.fingerprint().unwrap(),
    )
    .await;
    let mut changed = reserve.clone();
    let PreparationSource::FrontendClaim(source) = changed.source.as_mut().unwrap() else {
        unreachable!()
    };
    source.resolver.authority_claim.push_str(" changed");
    assert_eq!(
        store.reserve_preparation(changed).await.unwrap_err(),
        SessionOwnerError::InvalidRequest
    );
    assert!(
        store
            .lookup_preparation(&run.session_id, &reserve.key)
            .await
            .unwrap()
            .is_none()
    );
    let PreparationCompletion::ToolContinuation(crate::ToolContinuationClaimOutcome::Applied {
        receipt,
        ..
    }) = super::continuations::complete(
        store,
        &reserve,
        PreparedOperation::FrontendClaim(original.clone()),
    )
    .await
    else {
        panic!("fresh compound frontend claim")
    };
    assert_eq!(receipt.semantic_digest, original.fingerprint().unwrap());
    assert_eq!(
        receipt.revision,
        store.session_revision(&run.session_id).await.unwrap()
    );
    recover(store, clock, &run, original, receipt, batch.next_attempt).await;
}

async fn recover(
    store: &dyn Store,
    clock: &ManualClock,
    run: &crabber_core::Run,
    original: crate::FrontendContinuationClaim,
    receipt: crate::ToolContinuationReceipt,
    attempt: crate::AttemptTarget,
) {
    clock.set(clock.now() + time::Duration::milliseconds(2));
    let recovery = crate::FrontendContinuationRecovery {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        original: original.clone(),
        attempt,
        owner: "recovered frontend owner".into(),
        lease_ms: 30000,
    };
    let reserve = super::continuations::request(
        store,
        run,
        PreparationSource::FrontendRecovery(recovery.clone()),
        PreparationKey::ToolContinuation(recovery.id.clone()),
        ExistingWorkTarget::FrontendRecovery {
            continuation: original.claim.continuation.clone(),
            attempt: recovery.attempt.clone(),
        },
        recovery.fingerprint().unwrap(),
    )
    .await;
    let PreparationCompletion::ToolContinuation(crate::ToolContinuationClaimOutcome::Applied {
        receipt,
        ..
    }) = super::continuations::complete(
        store,
        &reserve,
        PreparedOperation::FrontendRecovery(recovery.clone()),
    )
    .await
    else {
        panic!("fresh compound frontend recovery")
    };
    recovery.verify_receipt(&receipt).unwrap();
    assert_eq!(
        receipt.revision,
        store.session_revision(&run.session_id).await.unwrap()
    );
}
