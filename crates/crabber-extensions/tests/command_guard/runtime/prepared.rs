use super::*;

// The same fixture is mounted on both sides of resume so handler identity stays fixed.
pub(crate) struct PrepareProbe {
    pub count: Arc<AtomicUsize>,
    pub rewrite: bool,
    pub second_guard: Option<Arc<AtomicUsize>>,
}
struct OtherGuard(Arc<AtomicUsize>);
impl crabber::extension::ToolGuard for OtherGuard {
    fn id(&self) -> &str {
        "fixture/other-guard"
    }
    fn check(&self, _: &str, _: &Value) -> crabber::extension::GuardDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        crabber::extension::GuardDecision::Abstain
    }
}
#[async_trait]
impl Extension for PrepareProbe {
    fn id(&self) -> &str {
        "fixture/prepare"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        format!("{}", self.rewrite)
    }
    async fn install(&self, r: &mut crabber::extension::Registrar) -> Result<(), ExtensionError> {
        use crabber::extension::{Point, ToolPrepare};
        let count = self.count.clone();
        let rewrite = self.rewrite;
        r.on_transform(
            ToolPrepare::ID,
            0,
            "probe",
            Arc::new(move |mut v| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    if rewrite {
                        v["input"] = json!({"cmd":"blocked PREPARED_MARKER"});
                    }
                    Ok(v)
                })
            }),
        );
        if let Some(count) = &self.second_guard {
            r.guard(Arc::new(OtherGuard(count.clone())));
        }
        Ok(())
    }
}
#[tokio::test]
async fn guard_sees_prepared_input_and_all_guards_run() {
    let g = guard();
    let store = Arc::new(MemoryStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let other = Arc::new(AtomicUsize::new(0));
    let prepared = Arc::new(AtomicUsize::new(0));
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let probe = Arc::new(PrepareProbe {
        count: prepared.clone(),
        rewrite: true,
        second_guard: Some(other.clone()),
    });
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call("shell", "cmd", "echo harmless"),
            done(),
        ])),
        p.clone(),
        vec![(g.clone(), Scope::Global), (probe, Scope::Global)],
        vec![tool("shell", "cmd", count.clone())],
        None,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_denied(&records[0]);
    assert_eq!(
        records[0].arguments,
        json!({"cmd":"blocked PREPARED_MARKER"})
    );
    assert_eq!(other.load(Ordering::SeqCst), 1);
    assert_eq!(prepared.load(Ordering::SeqCst), 1);
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(p.count(), 0);
    assert_eq!(g.stats().rule_match, 1);
    a.close_extensions().await.unwrap();
}
