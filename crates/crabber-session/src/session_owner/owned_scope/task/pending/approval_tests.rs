//! Both actual Approval families use the same original gated main on all stores.
mod faults;
use super::*;
use crate::Store;
use crate::session_owner::{
    driver::{RegisterSuccessorExecutionDriver, SuccessorExecutionDriverOrigin as Origin},
    preparation::{InspectPreparedApprovalDriver, PreparedOperation},
};
use crate::successor_driver_contract::approval::pending::prepared;
use crabber_core::ManualClock;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn contract<S: Store + 'static>(store: Arc<S>, clock: Arc<ManualClock>) {
    let owner = PendingDriverTasks::default();
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let (context, original) = prepared(store.as_ref(), &clock, recovery, unkeyed).await;
        let retry = original.clone();
        let source = original.observed_operation();
        let (id, digest, origin) = match source {
            PreparedOperation::ApprovalClaim(v) => (
                v.id.clone(),
                v.fingerprint().unwrap(),
                Origin::ApprovalClaim,
            ),
            PreparedOperation::ApprovalRecovery(v) => (
                v.id.clone(),
                v.fingerprint().unwrap(),
                Origin::ApprovalRecovery,
            ),
            _ => panic!("actual Approval"),
        };
        let captured = store
            .complete_preparation_with_driver_source(original)
            .await
            .unwrap();
        let claim = captured.claim.unwrap();
        assert_eq!(claim.issuance, if recovery { 5 } else { 3 });
        let PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied {
            receipt,
            grant,
        }) = captured.completion
        else {
            panic!("fresh native");
        };
        let request = InspectPreparedApprovalDriver {
            request: RegisterSuccessorExecutionDriver {
                context: context.clone(),
                fence: grant.fence,
                claim: claim.clone(),
            },
            origin,
            operation_id: id,
            semantic_digest: digest,
        };
        let metadata = store
            .inspect_prepared_approval_driver(request.clone())
            .await
            .unwrap();
        assert_eq!(metadata.receipt, receipt);
        assert_eq!(metadata.claim, claim);
        let mut wrong = request.clone();
        wrong.request.claim.issuance += 1;
        assert!(store.inspect_prepared_approval_driver(wrong).await.is_err());
        let replay = store
            .complete_preparation_with_driver_source(retry)
            .await
            .unwrap();
        assert!(replay.completion.is_replayed());
        assert!(replay.claim.is_none());

        let (context, original) = prepared(store.as_ref(), &clock, recovery, unkeyed).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut task = PendingSuccessorTask::register(
            &owner,
            store.clone(),
            context,
            original,
            move |context, completion| async move {
                assert!(context.try_enter_main().is_ok());
                assert!(matches!(
                    completion,
                    PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied { .. })
                ));
                count.fetch_add(1, Ordering::SeqCst);
                context
            },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert!(observed.error().is_none(), "{:?}", observed.error());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(observed.bound_main().is_some());
        let PendingSuccessorCompletion::Fresh(metadata) = control.completion().await.unwrap()
        else {
            panic!("fresh metadata");
        };
        assert!(matches!(
            (recovery, metadata),
            (false, PendingSuccessorReplay::ApprovalClaim(_))
                | (true, PendingSuccessorReplay::ApprovalRecovery(_))
        ));
    }
    owner.wait_for_supervision().await;
    Box::pin(faults::contract(store.clone(), clock.clone())).await;
    faults::blocked_main(store, &clock).await;
}
#[tokio::test]
async fn memory_pending_approval_same_task() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(crate::MemoryStore::with_clock(clock.clone())),
        clock,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_pending_approval_same_task() {
    let path = tempfile::tempdir().unwrap();
    let database = path.path().join("pending-approval.sqlite");
    crate::SqliteStore::migrate(&database).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(
            crate::SqliteStore::connect(database)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        ),
        clock,
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_pending_approval_same_task() {
    let _guard = crate::postgres::pending_initial_test_lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(
            crate::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        ),
        clock,
    ))
    .await;
}
