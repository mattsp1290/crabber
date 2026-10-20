use super::*;

#[tokio::test]
async fn every_single_family_preserves_frozen_and_actual_hashes_and_metadata_capture() {
    for family in FAMILIES {
        for scope in [FactoryScope::Workspace, FactoryScope::Session] {
            for config in ["opaque\nconfig", r#"{"z":2,"a":"escaped\\value"}"#] {
                let mut f = Factory::new("single", scope);
                Arc::get_mut(&mut f).unwrap().config = config.into();
                let old_host = Arc::new(Host::default());
                let mixed_host = Arc::new(Host::default());
                let prior = old(f.clone(), family, old_host.clone());
                let mixed = ContextualPlanProvider::new_with_owned_products(
                    Registry::new(),
                    vec![f.entry(family)],
                    BOUND,
                    mixed_host.clone(),
                )
                .unwrap();
                let c = context();
                assert_eq!(
                    prior.registration_fingerprint(&c).unwrap(),
                    mixed.registration_fingerprint(&c).unwrap()
                );
                assert_eq!(f.seen.metadata.load(Ordering::SeqCst), 6);
                let a = prior.acquire_plan_in(&c).await.unwrap();
                let b = mixed.acquire_plan_in(&c).await.unwrap();
                assert_eq!(a.fingerprint, b.fingerprint);
                assert_eq!(a.components, b.components);
                assert_eq!(f.seen.metadata.load(Ordering::SeqCst), 6);
                drop((a, b));
                prior.close_all(BOUND).await.unwrap();
                mixed.close_all(BOUND).await.unwrap();
                for host in [&old_host, &mixed_host] {
                    let graph = host
                        .0
                        .lock()
                        .unwrap()
                        .iter()
                        .find(|g| {
                            matches!(
                                (g.generation().scope(), scope),
                                (
                                    ServiceGenerationScope::Workspace { .. },
                                    FactoryScope::Workspace
                                ) | (
                                    ServiceGenerationScope::Session { .. },
                                    FactoryScope::Session
                                )
                            )
                        })
                        .cloned();
                    if let Some(g) = graph {
                        disposed(&g, family, 0).await;
                    }
                }
                assert_eq!(f.seen.drops.load(Ordering::SeqCst), 2);
            }
        }
    }
    let c = context();
    let host = Arc::new(Host::default());
    let prior = ContextualPlanProvider::new_with_generation_host(
        Registry::new(),
        vec![],
        vec![],
        BOUND,
        host.clone(),
    )
    .unwrap();
    let mixed =
        ContextualPlanProvider::new_with_owned_products(Registry::new(), vec![], BOUND, host)
            .unwrap();
    assert_eq!(
        prior.registration_fingerprint(&c).unwrap(),
        mixed.registration_fingerprint(&c).unwrap()
    );
}
