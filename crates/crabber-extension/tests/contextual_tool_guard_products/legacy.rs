use super::*;
struct Legacy(Arc<dyn ToolGuard>);
#[async_trait]
impl Extension for Legacy {
    fn id(&self) -> &'static str {
        "legacy-guard"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.guard(self.0.clone());
        Ok(())
    }
}
#[tokio::test]
async fn old_owned_arc_guard_join_is_not_original_reference_disposal() {
    let seen = Seen::new();
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Legacy(Arc::new(Body(seen.clone())))),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let guard = plan.guards[0].clone();
    drop(plan);
    mount.close(BOUND).await.unwrap();
    let before = seen.drops.load(Ordering::SeqCst);
    assert_eq!(guard.check("tool", &Value::Null), GuardDecision::Deny);
    drop(guard);
    registry.close_all().await.unwrap();
    drop(mount);
    drop(registry);
    assert_eq!(before, 0);
    assert_eq!(seen.ordinary.load(Ordering::SeqCst), 0);
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
}
