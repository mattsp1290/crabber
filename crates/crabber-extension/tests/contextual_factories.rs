mod contextual_support;
use contextual_support::*;
use crabber_extension::{ExtensionError, FactoryScope, RunPlanProvider};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[tokio::test]
async fn factories_instantiate_workspace_and_session_state_with_deterministic_fingerprints() {
    let workspace = Arc::new(Observed::default());
    let session = Arc::new(Observed::default());
    let factories = || {
        vec![
            Arc::new(Factory {
                name: "workspace_counter",
                scope: FactoryScope::Workspace,
                observed: workspace.clone(),
            }) as Arc<dyn crabber_extension::ExtensionFactory>,
            Arc::new(Factory {
                name: "session_counter",
                scope: FactoryScope::Session,
                observed: session.clone(),
            }),
        ]
    };
    let provider = provider(factories());
    let a = context("a", "left");
    let b = context("b", "left");
    let c = context("c", "right");
    let (a_plan, b_plan) = tokio::join!(provider.acquire_plan_in(&a), provider.acquire_plan_in(&b));
    let a_plan = a_plan.unwrap();
    let b_plan = b_plan.unwrap();
    let c_plan = provider.acquire_plan_in(&c).await.unwrap();
    assert_eq!(count(&a_plan, "workspace_counter").await, 1);
    assert_eq!(count(&b_plan, "workspace_counter").await, 2);
    assert_eq!(count(&c_plan, "workspace_counter").await, 1);
    assert_eq!(count(&a_plan, "session_counter").await, 1);
    assert_eq!(count(&b_plan, "session_counter").await, 1);
    assert_eq!(count(&a_plan, "session_counter").await, 2);
    assert_eq!(workspace.created.load(Ordering::SeqCst), 2);
    assert_eq!(session.created.load(Ordering::SeqCst), 3);
    assert!(
        workspace
            .bindings
            .lock()
            .unwrap()
            .iter()
            .all(|(_, id)| id.is_none())
    );
    assert!(
        session
            .bindings
            .lock()
            .unwrap()
            .iter()
            .all(|(_, id)| id.is_some())
    );
    let fingerprint = a_plan.fingerprint().clone();
    drop((a_plan, b_plan, c_plan));
    let restored = contextual_support::provider(factories());
    drop(restored.acquire_plan_in(&c).await.unwrap());
    let restored_a = restored.acquire_plan_in(&a).await.unwrap();
    assert_eq!(*restored_a.fingerprint(), fingerprint);
    drop(restored_a);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    restored.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn targeted_close_retains_uncertain_cleanup_without_stopping_other_owners() {
    let observed = Arc::new(Observed::default());
    let provider = provider(vec![Arc::new(Factory {
        name: "counter",
        scope: FactoryScope::Session,
        observed: observed.clone(),
    })]);
    let a = context("a", "left");
    let b = context("b", "right");
    let a_plan = provider.acquire_plan_in(&a).await.unwrap();
    let b_plan = provider.acquire_plan_in(&b).await.unwrap();
    assert!(matches!(
        provider.close_session(&a, Duration::from_millis(1)).await,
        Err(ExtensionError::ContextCleanupIncomplete | ExtensionError::MountCloseTimeout { .. })
    ));
    assert!(matches!(
        provider.acquire_plan_in(&a).await,
        Err(ExtensionError::ContextOwnerClosed)
    ));
    assert_eq!(count(&b_plan, "counter").await, 1);
    assert!(
        !observed
            .stopped
            .lock()
            .unwrap()
            .iter()
            .any(|id| id.contains(":right:"))
    );
    drop(a_plan);
    provider
        .close_session(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        observed
            .shutdown
            .lock()
            .unwrap()
            .iter()
            .any(|id| id == "counter:left:a")
    );
    assert!(
        !observed
            .shutdown
            .lock()
            .unwrap()
            .iter()
            .any(|id| id == "counter:right:b")
    );
    drop(b_plan);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn root_drift_and_workspace_cleanup_are_checked_before_factory_recreation() {
    let observed = Arc::new(Observed::default());
    let provider = provider(vec![Arc::new(Factory {
        name: "counter",
        scope: FactoryScope::Workspace,
        observed: observed.clone(),
    })]);
    let a = context("a", "left");
    let b = context("b", "left");
    let c = context("c", "right");
    drop(provider.acquire_plan_in(&a).await.unwrap());
    let other = provider.acquire_plan_in(&c).await.unwrap();
    let drift = crabber_core::SessionContext::new("b".into(), "left", "/admitted/left/");
    assert!(matches!(
        provider.acquire_plan_in(&drift).await,
        Err(ExtensionError::ContextIdentityMismatch)
    ));
    assert!(matches!(
        provider
            .close_workspace(&drift, Duration::from_secs(1))
            .await,
        Err(ExtensionError::ContextIdentityMismatch)
    ));
    assert_eq!(observed.created.load(Ordering::SeqCst), 2);
    provider
        .close_session(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        !observed
            .stopped
            .lock()
            .unwrap()
            .iter()
            .any(|id| id.contains(":left:"))
    );
    drop(provider.acquire_plan_in(&b).await.unwrap());
    provider
        .close_workspace(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(
        provider.acquire_plan_in(&b).await,
        Err(ExtensionError::ContextOwnerClosed)
    ));
    assert_eq!(count(&other, "counter").await, 1);
    drop(other);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}
