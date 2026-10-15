use async_trait::async_trait;
use crabber_core::SessionContext;
use crabber_extension::{
    ContextAssemble, Extension, ExtensionError, Point, Registrar, Registry, Scope,
};
use std::sync::Arc;

struct Fixture(&'static str);
#[async_trait]
impl Extension for Fixture {
    fn id(&self) -> &'static str {
        self.0
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        // The same local handler ID in different extensions still has distinct
        // ordering identity in the frozen plan.
        registrar.on_transform(
            ContextAssemble::ID,
            0,
            "local-handler",
            Arc::new(|value| Box::pin(async move { Ok(value) })),
        );
        Ok(())
    }
}

#[tokio::test]
async fn unrelated_context_mount_order_does_not_change_recovery_fingerprint() {
    let context = SessionContext::new("session-a".into(), "workspace-a", "/a");
    let mut fingerprints = vec![];
    for unrelated_first in [true, false] {
        let registry = Registry::new();
        let target = (
            Arc::new(Fixture("target")) as Arc<dyn Extension>,
            Scope::Workspace("workspace-a".into()),
        );
        let unrelated = (
            Arc::new(Fixture("unrelated")) as Arc<dyn Extension>,
            Scope::Session("other".into()),
        );
        let mounts = if unrelated_first {
            [unrelated, target]
        } else {
            [target, unrelated]
        };
        for (extension, scope) in mounts {
            registry.mount(extension, scope).await.unwrap();
        }
        let plan = registry.try_acquire_in(&context).unwrap();
        assert_eq!(plan.components.len(), 2);
        fingerprints.push(plan.fingerprint().clone());
        drop(plan);
        registry.close_all().await.unwrap();
    }
    assert_eq!(fingerprints[0], fingerprints[1]);
}

#[tokio::test]
async fn relevant_handler_order_and_workspace_session_binding_remain_distinct() {
    let context = SessionContext::new("session-a".into(), "workspace-a", "/a");
    let mut fingerprints = vec![];
    for names in [["first", "second"], ["second", "first"]] {
        let registry = Registry::new();
        for name in names {
            registry
                .mount(
                    Arc::new(Fixture(name)),
                    Scope::Workspace("workspace-a".into()),
                )
                .await
                .unwrap();
        }
        registry
            .mount(
                Arc::new(Fixture("other-workspace-session")),
                Scope::SessionIn {
                    session_id: "session-a".into(),
                    workspace_id: "workspace-b".into(),
                },
            )
            .await
            .unwrap();
        let plan = registry.try_acquire_in(&context).unwrap();
        assert_eq!(plan.components.len(), 4);
        fingerprints.push(plan.fingerprint().clone());
        drop(plan);
        assert_eq!(
            registry
                .try_acquire(context.session_id())
                .unwrap()
                .components
                .len(),
            0
        );
        registry.close_all().await.unwrap();
    }
    assert_ne!(fingerprints[0], fingerprints[1]);
}
