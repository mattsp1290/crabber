use super::*;
use crate::{ResourceRegistrationError, RunPlanProvider, StaticPlanProvider, ToolResourceOwner};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

fn context(owner: &ToolResourceOwner) -> PreparationResourceContext {
    PreparationResourceContext::from_host(
        SessionContext::new(crabber_core::SessionId::new(), "workspace", "/workspace"),
        owner.resources(),
    )
    .unwrap()
}
#[tokio::test]
async fn lease_disposal_signals_but_never_closes_or_joins_resources() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    let disposed = CancellationToken::new();
    let plan = StaticPlanProvider::new(vec![], vec![])
        .acquire_plan(context.session().session_id())
        .await
        .unwrap()
        .with_preparation_lease(PreparationResourceLease::from_host(disposed.clone()))
        .unwrap();
    let retained = plan.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let release = Arc::new(tokio::sync::Notify::new());
    let wait = release.clone();
    context
        .cleanup()
        .defer(move || async move {
            counted.fetch_add(1, Ordering::SeqCst);
            wait.notified().await;
            Ok(())
        })
        .unwrap();
    plan.release();
    assert!(!disposed.is_cancelled());
    retained.release();
    assert!(disposed.is_cancelled());
    assert!(!owner.snapshot().sealed);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let pending = owner.drain(Duration::from_millis(10)).await;
    assert_eq!(pending.pending, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.joined, 1);
    assert_eq!(joined.failures, []);
}
#[tokio::test]
async fn registration_is_bounded_before_effects_and_late_clones_are_sealed() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    let calls = Arc::new(AtomicUsize::new(0));
    for _ in 0..crate::MAX_TOOL_RESOURCE_TASKS {
        let counted = calls.clone();
        context
            .cleanup()
            .defer(move || async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(
        context
            .cleanup()
            .defer(|| async { panic!("rejected action ran") }),
        Err(ResourceRegistrationError::Capacity)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.joined, crate::MAX_TOOL_RESOURCE_TASKS);
    assert_eq!(calls.load(Ordering::SeqCst), crate::MAX_TOOL_RESOURCE_TASKS);
    assert_eq!(
        context.cleanup().spawn(async { panic!("late effect ran") }),
        Err(ResourceRegistrationError::Sealed)
    );
}
#[tokio::test]
async fn failure_is_retained_and_legacy_provider_does_not_claim_a_bridge() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    context
        .cleanup()
        .defer(|| async { Err(ResourceFailure::CleanupFailed) })
        .unwrap();
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.failures, [ResourceFailure::CleanupFailed]);
    assert_eq!(owner.drain(Duration::ZERO).await, joined);
    let provider = StaticPlanProvider::new(vec![], vec![]);
    assert!(
        provider
            .preparation_resource_contract(context.session())
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        provider.acquire_plan_in_preparation(&context).await,
        Err(ExtensionError::PreparationResourcesUnsupported)
    ));
    assert!(
        PreparationResourceContext::from_host(context.session().clone(), ToolResources::default())
            .is_err()
    );
}
