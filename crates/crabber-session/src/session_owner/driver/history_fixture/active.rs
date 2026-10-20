//! A genuinely dispatched main that remains waiting until its retained owner releases it.
use super::*;
pub(crate) struct Active {
    pub request: RegisterExecutionDriver,
    pub grant: Box<super::super::ExecutionDriverGrant>,
    pub main: OwnedTaskRegistration<()>,
    release: tokio::sync::oneshot::Sender<()>,
}
pub(crate) async fn start<S: Store + 'static>(store: Arc<S>, unkeyed: bool) -> Active {
    let admitted = if unkeyed {
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(
            store.as_ref(),
        )
        .await
    } else {
        crate::session_owner::preparation_contract::resources::initial_driver(store.as_ref()).await
    };
    let request = RegisterExecutionDriver {
        context: SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    };
    let ExecutionDriverOutcome::Applied { grant, .. } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver");
    };
    let (release, wait) = tokio::sync::oneshot::channel();
    let (started, observed) = tokio::sync::oneshot::channel();
    let mut main =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            started.send(()).unwrap();
            wait.await.unwrap();
        })
        .unwrap();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            main.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone())),
        )
        .await
        .unwrap()
    else {
        panic!("fresh activation");
    };
    main.dispatch(*permit).unwrap();
    observed.await.unwrap();
    Active {
        request,
        grant,
        main,
        release,
    }
}
impl Active {
    pub(crate) async fn finish(mut self, store: &impl Store) -> SessionOwnerOperationId {
        self.release.send(()).unwrap();
        let joined = self.main.join().await.unwrap();
        let mut cleanup = OwnedCleanupTask::from_host_task(
            joined.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        let proof = joined.observe_resources(&mut cleanup).await.unwrap();
        let join = proof.request().unwrap().id;
        store
            .join_owned_scope(proof, self.grant.authority().clone())
            .await
            .unwrap();
        join
    }
}
