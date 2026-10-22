use super::*;
use crabber::JsonToolExecutor;

#[tokio::test]
async fn unix_shell_canary() {
    struct Shell(std::path::PathBuf);
    #[async_trait]
    impl JsonToolExecutor for Shell {
        async fn execute(&self, args: Value) -> Result<Value, ExtensionError> {
            let status = std::process::Command::new("/bin/sh")
                .arg("-c")
                .arg(args["cmd"].as_str().unwrap())
                .current_dir(&self.0)
                .status()
                .unwrap();
            assert!(status.success());
            Ok(json!(true))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let g = guard_with(Options {
        rules: vec![Rule {
            id: "canary".into(),
            executable: "printf".into(),
            arg_prefix: vec!["DENY".into()],
        }],
        ..options()
    });
    let store = Arc::new(MemoryStore::new());
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let mut t = tool("shell", "cmd", Arc::new(AtomicUsize::new(0)));
    Arc::get_mut(&mut t).unwrap().executor = Arc::new(Shell(dir.path().into()));
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            calls(&[
                ("shell", "cmd", "printf DENY > denied"),
                ("shell", "cmd", "printf ALLOW > allowed"),
            ]),
            done(),
        ])),
        p.clone(),
        vec![(g, Scope::Global)],
        vec![t],
        None,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_denied(
        records
            .iter()
            .find(|r| r.arguments["cmd"] == "printf DENY > denied")
            .unwrap(),
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Completed)
            .count(),
        1
    );
    assert!(!dir.path().join("denied").exists());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("allowed")).unwrap(),
        "ALLOW"
    );
    assert_eq!(p.count(), 1);
    a.close_extensions().await.unwrap();
}
