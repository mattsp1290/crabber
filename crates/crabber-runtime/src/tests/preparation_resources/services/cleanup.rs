use super::*;
pub(super) fn decorate(
    context: &PreparationResourceContext,
    seen: Arc<Seen>,
) -> PreparationResourceContext {
    context.clone().with_service_dependencies(
        crabber_extension::SessionServiceDependencies::from_host(Arc::new(CleanupRegistrar {
            inner: context.service_dependencies().clone(),
            seen,
        })),
    )
}
struct CleanupRegistrar {
    inner: crabber_extension::SessionServiceDependencies,
    seen: Arc<Seen>,
}
#[async_trait]
impl crabber_extension::SessionServiceRegistrar for CleanupRegistrar {
    async fn register(
        &self,
        generation: crabber_extension::ServiceGenerationDescriptor,
        task: crabber_extension::SessionServiceDependencyTask,
    ) -> Result<
        crabber_extension::SessionServiceDependencyAdmission,
        crabber_extension::SessionServiceDependencyError,
    > {
        let seen = self.seen.clone();
        self.inner
            .register(generation, move |context| async move {
                let closing = context.cleanup.closing();
                context
                    .cleanup
                    .spawn_checked(async move {
                        closing.await;
                        seen.resource_started.fetch_add(1, Ordering::SeqCst);
                        if seen.resource_wait.load(Ordering::SeqCst) {
                            seen.resource_release.acquire().await.unwrap().forget();
                        }
                        seen.resource_finished.fetch_add(1, Ordering::SeqCst);
                        match seen.resource_failure.load(Ordering::SeqCst) {
                            1 => Err(crabber_extension::ResourceFailure::CleanupFailed),
                            2 => panic!("actual service cleanup failed"),
                            _ => Ok(()),
                        }
                    })
                    .unwrap();
                task(context).await
            })
            .await
    }
}
