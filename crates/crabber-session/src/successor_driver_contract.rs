//! Actual prepared native claims and distinct gated successor workers.
#[path = "successor_driver_contract/approval.rs"]
pub(crate) mod approval;
#[path = "successor_driver_contract/tools.rs"]
pub(crate) mod tools;
use crate::{
    Store,
    session_owner::{driver::*, owned_scope::*, preparation::*},
};
use crabber_core::ManualClock;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[path = "successor_driver_contract/faults.rs"]
mod faults;
#[path = "successor_driver_contract/fixture.rs"]
pub(crate) mod fixture;
#[path = "successor_driver_contract/guards.rs"]
mod guards;
pub(crate) use faults::run as faults;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "successor_driver_contract/process.rs"]
pub(crate) mod process;
pub(crate) async fn run<S: Store + 'static>(store: Arc<S>, clock: &ManualClock) {
    guards::run(store.as_ref(), clock).await;
    for admission in [true, false] {
        let request = fixture::request(store.as_ref(), clock, admission).await;
        assert!(store.supports_successor_execution_drivers());
        let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = store
            .register_successor_execution_driver(request.clone())
            .await
            .unwrap()
        else {
            panic!("fresh successor");
        };
        assert!(
            !serde_json::to_string(&receipt)
                .unwrap()
                .contains(&request.fence.claim_token)
        );
        assert!(
            matches!(store.register_successor_execution_driver(request.clone()).await.unwrap(),SuccessorExecutionDriverOutcome::Replayed(saved)if saved==receipt)
        );
        let mut changed = request.clone();
        changed.fence.claim_token.push('x');
        assert!(
            store
                .register_successor_execution_driver(changed)
                .await
                .is_err()
        );
        assert_ne!(
            request.identities().unwrap().owner,
            RegisterExecutionDriver {
                context: request.context.clone(),
                fence: request.fence.clone()
            }
            .identities()
            .unwrap()
            .owner
        );
        let effects = Arc::new(AtomicUsize::new(0));
        let count = effects.clone();
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
                count.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                    grant.clone(),
                )),
            )
            .await
            .unwrap()
        else {
            panic!("fresh permit");
        };
        task.dispatch(*permit).unwrap();
        let joined = task.join().await.unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let mut cleanup = OwnedCleanupTask::from_host_task(
            joined.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        store
            .join_owned_scope(
                joined.observe_resources(&mut cleanup).await.unwrap(),
                grant.authority().clone(),
            )
            .await
            .unwrap();
        if admission {
            store
                .execution(request.fence.clone())
                .await
                .unwrap()
                .begin_admission_execution()
                .await
                .unwrap();
        }
        let run = store.get_run(&request.fence.run_id).await.unwrap().unwrap();
        clock.set(run.lease_until);
        store
            .claim_expired_run(&run.id, "later-controller")
            .await
            .unwrap();
        assert!(
            matches!(store.register_successor_execution_driver(request.clone()).await.unwrap(),SuccessorExecutionDriverOutcome::Replayed(saved)if saved==receipt)
        );
        assert!(
            matches!(store.register_successor_execution_driver(request).await.unwrap(),SuccessorExecutionDriverOutcome::Replayed(saved)if saved==receipt)
        );
    }
}
#[tokio::test]
async fn memory_successor_driver_contract() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    run(
        Arc::new(crate::MemoryStore::with_clock(clock.clone())),
        &clock,
    )
    .await;
}
