use super::*;

#[tokio::test]
async fn cross_family_cross_scope_duplicate_ids_refuse_before_work() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let a = Factory::new("duplicate", FactoryScope::Workspace);
        let b = Factory::new("duplicate", scope);
        let host = Arc::new(Host::default());
        assert!(
            ContextualPlanProvider::new_with_owned_products(
                Registry::new(),
                vec![a.entry(Family::Tool), b.entry(Family::Guard)],
                BOUND,
                host.clone()
            )
            .is_err()
        );
        assert_eq!(
            a.seen.made.load(Ordering::SeqCst) + b.seen.made.load(Ordering::SeqCst),
            0
        );
        assert!(host.0.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn aggregate_scope_capacity_is_64_not_64_per_family_or_64_per_provider() {
    let host = Arc::new(Host::default());
    let mut factories = Vec::new();
    let mut seen = Vec::new();
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        for i in 0..64 {
            let f = Factory::new(&format!("{scope:?}-{i}"), scope);
            seen.push(f.seen.clone());
            factories.push(f.entry(Family::Root));
        }
    }
    let plans = ContextualPlanProvider::new_with_owned_products(
        Registry::new(),
        factories,
        BOUND,
        host.clone(),
    )
    .unwrap();
    drop(plans.acquire_plan_in(&context()).await.unwrap());
    let graphs = host.0.lock().unwrap().clone();
    assert_eq!(graphs.len(), 2);
    for graph in &graphs {
        assert!(graph.registered_graph_join(Duration::ZERO).await.is_err());
    }
    plans.close_all(BOUND).await.unwrap();
    for s in seen {
        assert_eq!(s.made.load(Ordering::SeqCst), 1);
        assert_eq!(s.drops.load(Ordering::SeqCst), 1);
    }
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let host = Arc::new(Host::default());
        let mut factories = Vec::new();
        let mut seen = Vec::new();
        for i in 0..65 {
            let f = Factory::new(&format!("overflow-{i}"), scope);
            seen.push(f.seen.clone());
            factories.push(f.entry(if i == 64 { Family::Tool } else { Family::Root }));
        }
        let plans = ContextualPlanProvider::new_with_owned_products(
            Registry::new(),
            factories,
            BOUND,
            host.clone(),
        )
        .unwrap();
        assert!(plans.acquire_plan_in(&context()).await.is_err());
        assert!(seen.iter().all(|s| s.made.load(Ordering::SeqCst) == 0));
        // Empty Workspace setup can precede overflowing Session selection;
        // the overflowing generation itself never registers Work.
        assert!(host.0.lock().unwrap().iter().all(|g| !matches!(
            (g.generation().scope(), scope),
            (
                ServiceGenerationScope::Workspace { .. },
                FactoryScope::Workspace
            ) | (
                ServiceGenerationScope::Session { .. },
                FactoryScope::Session
            )
        )));
        let _closed = plans.close_all(BOUND).await;
    }
}
