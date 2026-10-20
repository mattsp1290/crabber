//! Genuine compound effects, original source immutability and exact shared budget.
use super::super::ApprovalInput;
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crate::{ApprovalClaimOutcome, ApprovalError, ApprovalReply};
use crabber_core::ManualClock;
use time::OffsetDateTime;

pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    for recovery in [false, true] {
        for fault in [0, 1, 2, 3] {
            clock.set(OffsetDateTime::UNIX_EPOCH);
            if recovery {
                let prepared =
                    Box::pin(crate::approval_contract::recovery::prepare(store, clock)).await;
                clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31));
                let input = ApprovalInput::Recovery(&prepared.request);
                if fault == 3 {
                    prepared_path(store, input).await;
                } else {
                    exercise(store, input, fault).await;
                }
            } else {
                let f = Box::pin(crate::approval_contract::fixture::staged(store, clock)).await;
                f.execution
                    .suspend_for_approval(f.batch.clone(), f.owner.clone())
                    .await
                    .unwrap();
                for (index, reply) in [ApprovalReply::Grant, ApprovalReply::Deny]
                    .into_iter()
                    .enumerate()
                {
                    store
                        .answer_approval(
                            crate::approval_contract::fixture::answer(store, &f, index, reply)
                                .await,
                        )
                        .await
                        .unwrap();
                }
                let claim = crate::approval_contract::fixture::claim(store, &f).await;
                if fault == 3 {
                    prepared_path(store, ApprovalInput::Claim(&claim)).await;
                } else {
                    exercise(store, ApprovalInput::Claim(&claim), fault).await;
                }
            }
        }
    }
}
async fn exercise(store: &impl Harness, input: ApprovalInput<'_>, fault: u8) {
    let original = input.original();
    let session = &original.session_id;
    let run = &original.run_id;
    let before = store.retained(session, run, false).await;
    let birth = store.retained(session, run, true).await;
    store.probe().arm(session, input.id(), true);
    let mut measured = Proof::bounded(32 << 20);
    assert_eq!(
        store
            .approval_with_proof(input, &mut measured)
            .await
            .unwrap_err(),
        ApprovalError::UnknownStoreOutcome
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
            .approval_with_proof(input, &mut Proof::bounded(exact - 1))
            .await
            .unwrap_err(),
        ApprovalError::UnknownStoreOutcome
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, before);
    store.approval_fault(fault);
    let result = store
        .approval_with_proof(input, &mut Proof::bounded(exact))
        .await;
    store.approval_fault(0);
    assert_eq!(store.probe().hits().len(), 1);
    if fault == 1 {
        assert_eq!(result.unwrap_err(), ApprovalError::UnknownStoreOutcome);
        assert_eq!(store.retained(session, run, false).await, before);
        assert!(
            store
                .lookup_approval(session, input.id())
                .await
                .unwrap()
                .is_none()
        );
        store.probe().arm(session, input.id(), false);
        assert!(matches!(
            store
                .approval_with_proof(input, &mut Proof::bounded(exact))
                .await
                .unwrap(),
            ApprovalClaimOutcome::Applied { .. }
        ));
    } else if fault == 2 {
        assert_eq!(result.unwrap_err(), ApprovalError::UnknownStoreOutcome);
    } else {
        assert!(matches!(
            result.unwrap(),
            ApprovalClaimOutcome::Applied { .. }
        ));
    }
    let saved = store
        .lookup_approval(session, input.id())
        .await
        .unwrap()
        .unwrap();
    let committed = store.retained(session, run, false).await;
    assert_ne!(committed, before);
    assert_eq!(store.retained(session, run, true).await, birth);
    store.probe().arm(session, input.id(), true);
    assert!(
        matches!(store.approval_with_proof(input,&mut Proof::bounded(32 << 20)).await.unwrap(),ApprovalClaimOutcome::Replayed(receipt) if receipt==saved)
    );
    assert_eq!(
        store.probe().hits(),
        Vec::<crabber_core::InputFingerprint>::new()
    );
    assert_eq!(store.retained(session, run, false).await, committed);
    stale(store, input).await;
}

async fn stale(store: &impl Harness, input: ApprovalInput<'_>) {
    match input {
        ApprovalInput::Claim(r) => {
            let mut r = r.clone();
            r.id = crabber_core::ControlId::new();
            refused(store, ApprovalInput::Claim(&r)).await;
        }
        ApprovalInput::Recovery(r) => {
            let mut r = r.clone();
            r.id = crabber_core::ControlId::new();
            refused(store, ApprovalInput::Recovery(&r)).await;
        }
    }
}
async fn refused(store: &impl Harness, input: ApprovalInput<'_>) {
    let original = input.original();
    let before = store
        .retained(&original.session_id, &original.run_id, false)
        .await;
    store.probe().arm(&original.session_id, input.id(), true);
    assert_eq!(
        store
            .approval_with_proof(input, &mut Proof::bounded(32 << 20))
            .await
            .unwrap_err(),
        ApprovalError::RevisionConflict
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
async fn prepared_path(store: &impl Harness, input: ApprovalInput<'_>) {
    use crate::session_owner::preparation::{
        ExistingWorkTarget, JoinedPreparation, PreparationKey, PreparationSource, PreparedOperation,
    };
    let original = input.original();
    let run = store.get_run(&original.run_id).await.unwrap().unwrap();
    let (source, work, operation) = match input {
        ApprovalInput::Claim(r) => (
            PreparationSource::ApprovalClaim(r.clone()),
            ExistingWorkTarget::ApprovalClaim(r.continuation.clone()),
            PreparedOperation::ApprovalClaim(r.clone()),
        ),
        ApprovalInput::Recovery(r) => (
            PreparationSource::ApprovalRecovery(r.clone()),
            ExistingWorkTarget::ApprovalRecovery {
                continuation: r.original.continuation.clone(),
                attempt: r.attempt.clone(),
            },
            PreparedOperation::ApprovalRecovery(r.clone()),
        ),
    };
    let reserve = crate::session_owner::preparation_contract::continuations::request(
        store,
        &run,
        source,
        PreparationKey::Approval(input.id().clone()),
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
    assert!(store.complete_preparation(joined.clone()).await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(prepared_snapshot(store, &run).await, before);
    store.probe().arm(&run.session_id, input.id(), false);
    assert!(
        !store
            .complete_preparation(joined.clone())
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
