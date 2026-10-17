//! Real compound grants, complete private targets and unchanged Raw birth.
use super::Harness;
use crate::native_enrollment::caller::tool::ToolInput;
use crate::public_session::v2::proof::Proof;
use crate::{Store, ToolContinuationClaimOutcome, ToolContinuationError};
use crabber_core::{ControlId, ManualClock};
use time::OffsetDateTime;
pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    collision(store).await;
    for frontend in [false, true] {
        for recovery in [false, true] {
            for fault in [0, 1, 2, 3] {
                clock.set(OffsetDateTime::UNIX_EPOCH);
                if frontend {
                    let claim = frontend_claim(store).await;
                    if recovery {
                        let ToolContinuationClaimOutcome::Applied { receipt, grant } = store
                            .claim_frontend_continuation(claim.clone())
                            .await
                            .unwrap()
                        else {
                            panic!("fresh frontend")
                        };
                        let request = crate::FrontendContinuationRecovery {
                            id: ControlId::new(),
                            expected_revision: receipt.revision,
                            original: claim,
                            attempt: grant.frontend[0].target.attempt.clone(),
                            owner: "raw-recovery".into(),
                            lease_ms: 30_000,
                        };
                        clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(2));
                        cell(store, ToolInput::FrontendRecovery(&request), fault).await;
                    } else {
                        cell(store, ToolInput::FrontendClaim(&claim), fault).await;
                    }
                } else {
                    let claim = tool_claim(store).await;
                    if recovery {
                        let ToolContinuationClaimOutcome::Applied { receipt, grant } =
                            store.claim_tool_continuation(claim.clone()).await.unwrap()
                        else {
                            panic!("fresh tool")
                        };
                        let request = crate::ToolContinuationRecovery {
                            id: ControlId::new(),
                            expected_revision: receipt.revision,
                            original: claim,
                            attempt: grant.phases[0].target.attempt.clone(),
                            owner: "raw-recovery".into(),
                            lease_ms: 30_000,
                        };
                        clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(2));
                        cell(store, ToolInput::Recovery(&request), fault).await;
                    } else {
                        cell(store, ToolInput::Claim(&claim), fault).await;
                    }
                }
            }
        }
    }
}
async fn cell(store: &impl Harness, input: ToolInput<'_>, fault: u8) {
    if fault == 3 {
        prepared_path(store, input).await;
    } else {
        exercise(store, input, fault).await;
    }
}
async fn tool_claim(store: &impl Store) -> crate::ToolContinuationClaim {
    let f = crate::tool_continuation_contract::fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for i in 0..2 {
        store
            .answer_tool_interaction(
                crate::tool_continuation_contract::fixture::answer(store, &f, i).await,
            )
            .await
            .unwrap();
    }
    let mut claim = crate::tool_continuation_contract::fixture::claim(store, &f).await;
    claim.lease_ms = 1;
    claim
}
async fn frontend_claim(store: &impl Store) -> crate::FrontendContinuationClaim {
    let f = crate::frontend_contract::fixture::fixture(store).await;
    f.execution
        .stage_frontend_call(f.request.clone(), f.authority.clone())
        .await
        .unwrap();
    let batch = crate::frontend_contract::fixture::suspension(store, &f.request).await;
    let paused = f
        .execution
        .suspend_tool_continuation(
            batch.clone(),
            crate::ToolSuspensionAuthority {
                attempt: f.authority,
                phases: Vec::new(),
            },
        )
        .await
        .unwrap();
    let mut claim =
        crate::frontend_contract::fixture::external_claim(&f.request, &batch, paused.revision);
    claim.claim.lease_ms = 1;
    claim
}
async fn exercise(store: &impl Harness, input: ToolInput<'_>, fault: u8) {
    let original = input.original();
    let session = &original.session_id;
    let run = &original.run_id;
    let before = store.retained(session, run, false).await;
    let birth = store.retained(session, run, true).await;
    store.probe().arm(session, input.id(), true);
    let mut measured = Proof::bounded(32 << 20);
    assert_eq!(
        store
            .tool_with_proof(input, &mut measured)
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, false).await, before);
    assert_eq!(store.retained(session, run, true).await, birth);
    let exact = measured.used();
    assert!(exact > 0);
    // All newly selected records, actual outputs and original source reads must
    // precede the hook. One byte less refuses before exposing a fresh result.
    store.probe().arm(session, input.id(), false);
    assert_eq!(
        store
            .tool_with_proof(input, &mut Proof::bounded(exact - 1))
            .await
            .unwrap_err(),
        ToolContinuationError::UnknownStoreOutcome
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, before);
    store.tool_fault(fault);
    let result = store
        .tool_with_proof(input, &mut Proof::bounded(exact))
        .await;
    store.tool_fault(0);
    assert_eq!(store.probe().hits().len(), 1);
    if fault == 1 {
        assert_eq!(
            result.unwrap_err(),
            ToolContinuationError::UnknownStoreOutcome
        );
        assert_eq!(store.retained(session, run, false).await, before);
        assert!(
            store
                .lookup_tool_continuation(session, input.id())
                .await
                .unwrap()
                .is_none()
        );
        store.probe().arm(session, input.id(), false);
        assert!(matches!(
            store
                .tool_with_proof(input, &mut Proof::bounded(exact))
                .await
                .unwrap(),
            ToolContinuationClaimOutcome::Applied { .. }
        ));
    } else if fault == 2 {
        assert_eq!(
            result.unwrap_err(),
            ToolContinuationError::UnknownStoreOutcome
        );
    } else {
        assert!(matches!(
            result.unwrap(),
            ToolContinuationClaimOutcome::Applied { .. }
        ));
    }
    let saved = store
        .lookup_tool_continuation(session, input.id())
        .await
        .unwrap()
        .unwrap();
    let committed = store.retained(session, run, false).await;
    assert_ne!(committed, before);
    assert_eq!(store.retained(session, run, true).await, birth);
    store.probe().arm(session, input.id(), true);
    assert!(
        matches!(store.tool_with_proof(input,&mut Proof::bounded(32 << 20)).await.unwrap(),ToolContinuationClaimOutcome::Replayed(receipt) if receipt==saved)
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, committed);
    stale(store, input).await;
}

async fn stale(store: &impl Harness, input: ToolInput<'_>) {
    match input {
        ToolInput::Claim(r) => {
            let mut r = r.clone();
            r.id = crabber_core::ControlId::new();
            refused(store, ToolInput::Claim(&r)).await;
        }
        ToolInput::Recovery(r) => {
            let mut r = r.clone();
            r.id = crabber_core::ControlId::new();
            refused(store, ToolInput::Recovery(&r)).await;
        }
        ToolInput::FrontendClaim(r) => {
            let mut r = r.clone();
            r.claim.id = crabber_core::ControlId::new();
            refused(store, ToolInput::FrontendClaim(&r)).await;
        }
        ToolInput::FrontendRecovery(r) => {
            let mut r = r.clone();
            r.id = crabber_core::ControlId::new();
            refused(store, ToolInput::FrontendRecovery(&r)).await;
        }
    }
}
async fn refused(store: &impl Harness, input: ToolInput<'_>) {
    let original = input.original();
    let before = store
        .retained(&original.session_id, &original.run_id, false)
        .await;
    store.probe().arm(&original.session_id, input.id(), true);
    assert_eq!(
        store
            .tool_with_proof(input, &mut Proof::bounded(32 << 20))
            .await
            .unwrap_err(),
        ToolContinuationError::RevisionConflict
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(
        store
            .retained(&original.session_id, &original.run_id, false)
            .await,
        before
    );
}
async fn prepared_path(store: &impl Harness, input: ToolInput<'_>) {
    use crate::session_owner::preparation::{
        ExistingWorkTarget, JoinedPreparation, PreparationKey, PreparationSource, PreparedOperation,
    };
    let original = input.original();
    let run = store.get_run(&original.run_id).await.unwrap().unwrap();
    let (source, work, operation) = match input {
        ToolInput::Claim(r) => (
            PreparationSource::ToolClaim(r.clone()),
            ExistingWorkTarget::ToolClaim(r.continuation.clone()),
            PreparedOperation::ToolClaim(r.clone()),
        ),
        ToolInput::Recovery(r) => (
            PreparationSource::ToolRecovery(r.clone()),
            ExistingWorkTarget::ToolRecovery {
                continuation: r.original.continuation.clone(),
                attempt: r.attempt.clone(),
            },
            PreparedOperation::ToolRecovery(r.clone()),
        ),
        ToolInput::FrontendClaim(r) => (
            PreparationSource::FrontendClaim(r.clone()),
            ExistingWorkTarget::FrontendClaim(r.claim.continuation.clone()),
            PreparedOperation::FrontendClaim(r.clone()),
        ),
        ToolInput::FrontendRecovery(r) => (
            PreparationSource::FrontendRecovery(r.clone()),
            ExistingWorkTarget::FrontendRecovery {
                continuation: r.original.claim.continuation.clone(),
                attempt: r.attempt.clone(),
            },
            PreparedOperation::FrontendRecovery(r.clone()),
        ),
    };
    let reserve = crate::session_owner::preparation_contract::continuations::request(
        store,
        &run,
        source,
        PreparationKey::ToolContinuation(input.id().clone()),
        work,
        input.fingerprint().unwrap(),
    )
    .await;
    let grant = crate::session_owner::preparation_contract::begin(store, &reserve).await;
    let ((), joined) = JoinedPreparation::observe(
        grant,
        tokio::spawn(async move { Ok::<_, ()>(((), operation)) }),
    )
    .await
    .unwrap()
    .unwrap();
    let before = prepared_snapshot(store, &run).await;
    store.probe().arm(&run.session_id, input.id(), true);
    let mut measured = Proof::bounded(32 << 20);
    assert!(
        store
            .complete_with_proof(joined.clone(), &mut measured)
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(prepared_snapshot(store, &run).await, before);
    store.probe().arm(&run.session_id, input.id(), false);
    assert!(
        store
            .complete_with_proof(joined.clone(), &mut Proof::bounded(measured.used() - 1))
            .await
            .is_err()
    );
    assert_eq!(
        store.probe().hits(),
        [] as [crabber_core::InputFingerprint; 0]
    );
    assert_eq!(prepared_snapshot(store, &run).await, before);
    assert!(
        !store
            .complete_with_proof(joined.clone(), &mut Proof::bounded(measured.used()))
            .await
            .unwrap()
            .is_replayed()
    );
    assert_eq!(store.probe().hits().len(), 1);
    let committed = prepared_snapshot(store, &run).await;
    assert_ne!(committed, before);
    store.probe().arm(&run.session_id, input.id(), true);
    assert!(
        store
            .complete_preparation(joined)
            .await
            .unwrap()
            .is_replayed()
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(prepared_snapshot(store, &run).await, committed);
}
async fn prepared_snapshot(
    store: &impl Harness,
    run: &crabber_core::Run,
) -> crabber_core::InputFingerprint {
    super::snapshot_digest(&(
        store.retained(&run.session_id, &run.id, false).await,
        store.initial_retained(&run.session_id).await,
    ))
}

async fn collision(store: &impl Harness) {
    let f = crate::tool_continuation_contract::fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    let mut request = crate::tool_continuation_contract::fixture::claim(store, &f).await;
    request.id =
        crate::tool_continuation::derived_id("tool-continuation-suspend-attempt", &f.suspension.id);
    let before = store
        .retained(&request.session_id, &request.run_id, false)
        .await;
    store.probe().arm(&request.session_id, &request.id, true);
    assert_eq!(
        store
            .tool_with_proof(ToolInput::Claim(&request), &mut Proof::bounded(32))
            .await
            .unwrap_err(),
        ToolContinuationError::SemanticConflict
    );
    assert_eq!(
        store.probe().hits(),
        [] as [crabber_core::InputFingerprint; 0]
    );
    assert_eq!(
        store
            .retained(&request.session_id, &request.run_id, false)
            .await,
        before
    );
}
