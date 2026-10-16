//! Lost native activation acknowledgment cannot create a replacement permit.
use super::*;
#[tokio::test]
async fn unkeyed_activation_fault_retry_observes_actual_main_or_no_dispatch() {
    for point in [1, 2] {
        let (store, request) = setup().await;
        let grant = register(&store, &request).await;
        let effects = Arc::new(AtomicUsize::new(0));
        let task_effects = effects.clone();
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
                task_effects.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        let parent = Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone()));
        store.session_owner_fault.store(point, Ordering::SeqCst);
        assert!(matches!(
            store
                .activate_owned_scope(
                    task.activation().clone(),
                    grant.authority().clone(),
                    parent.clone()
                )
                .await,
            Err(Error::UnknownStoreOutcome)
        ));
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        store.session_owner_fault.store(0, Ordering::SeqCst);
        match store
            .activate_owned_scope(task.activation().clone(), grant.authority().clone(), parent)
            .await
            .unwrap()
        {
            OwnedScopeActivationOutcome::Applied { permit, .. } => {
                assert_eq!(point, 1);
                task.dispatch(*permit).unwrap();
            }
            OwnedScopeActivationOutcome::Replayed(_) => assert_eq!(point, 2),
        }
        let joined = task.join().await.unwrap();
        assert_eq!(
            joined.disposition(),
            if point == 1 {
                crate::session_owner::owned_scope::OwnedMainDisposition::Returned
            } else {
                crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
            }
        );
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(point == 1));
        let mut cleanup = OwnedCleanupTask::from_host_task(
            joined.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        let proof = joined.observe_resources(&mut cleanup).await.unwrap();
        Store::join_owned_scope(&store, proof, grant.authority().clone())
            .await
            .unwrap();
    }
}
