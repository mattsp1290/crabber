mod contextual_support;
use contextual_support::*;
use crabber_core::SessionContext;
use crabber_extension::*;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

#[tokio::test]
async fn session_close_joins_only_its_acquisitions_without_closing_shared_workspace() {
    for workspace_factory in [false, true] {
        let observed = Arc::new(Observed::default());
        let factories: Vec<Arc<dyn ExtensionFactory>> = if workspace_factory {
            vec![Arc::new(Factory {
                name: "workspace_counter",
                scope: FactoryScope::Workspace,
                observed: observed.clone(),
            })]
        } else {
            vec![]
        };
        let provider = provider(factories);
        let a = context("a", "workspace");
        let b = context("b", "workspace");
        let a_plan = provider.acquire_plan_in(&a).await.unwrap();
        let a_clone = a_plan.clone();
        let b_plan = provider.acquire_plan_in(&b).await.unwrap();
        for released in [Some(a_plan), None] {
            drop(released);
            assert_eq!(
                provider.close_session(&a, Duration::ZERO).await,
                Err(ExtensionError::ContextCleanupIncomplete)
            );
        }
        a_clone.release();
        provider
            .close_session(&a, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(observed.shutdown.lock().unwrap().len(), 0);
        assert!(matches!(
            provider.acquire_plan_in(&a).await,
            Err(ExtensionError::ContextOwnerClosed)
        ));
        provider.acquire_plan_in(&b).await.unwrap().release();
        if workspace_factory {
            assert_eq!(count(&b_plan, "workspace_counter").await, 1);
        }
        assert_eq!(
            provider.close_workspace(&b, Duration::ZERO).await,
            Err(ExtensionError::ContextCleanupIncomplete)
        );
        b_plan.release();
        provider
            .close_workspace(&b, Duration::from_secs(1))
            .await
            .unwrap();
        provider.close_all(Duration::from_secs(1)).await.unwrap();
    }
}

#[tokio::test]
async fn session_identity_is_frozen_before_workspace_factory_or_close_effects() {
    let observed = Arc::new(Observed::default());
    let provider = provider(vec![Arc::new(Factory {
        name: "workspace_counter",
        scope: FactoryScope::Workspace,
        observed: observed.clone(),
    })]);
    let original = context("a", "workspace");
    provider.acquire_plan_in(&original).await.unwrap().release();
    let changed = SessionContext::new(original.session_id().clone(), "other", "/other");
    assert!(matches!(
        provider.acquire_plan_in(&changed).await,
        Err(ExtensionError::ContextIdentityMismatch)
    ));
    assert_eq!(observed.created.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.check_preparation_context(&changed),
        Err(ExtensionError::ContextIdentityMismatch)
    );
    assert_eq!(
        provider
            .close_session(&changed, Duration::from_secs(1))
            .await,
        Err(ExtensionError::ContextIdentityMismatch)
    );
    let plan = provider.acquire_plan_in(&original).await.unwrap();
    assert_eq!(count(&plan, "workspace_counter").await, 1);
    plan.release();
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}
