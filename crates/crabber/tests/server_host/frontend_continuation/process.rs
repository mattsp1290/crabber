//! A mixed batch crosses a real process boundary with no repeated backend effect.
use super::*;
use std::{path::PathBuf, process::Command};
struct Effect(PathBuf);
#[async_trait]
impl JsonToolExecutor for Effect {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.0.join("effect"))
            .expect("backend effect executed twice");
        Ok(json!({"backend":"finished"}))
    }
}
struct EffectExtension(PathBuf);
#[async_trait]
impl Extension for EffectExtension {
    fn id(&self) -> &'static str {
        "frontend-process-effect"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "side_effect".into(),
                description: "Finish before the external wait".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: Vec::new(),
            },
            executor: Arc::new(Effect(self.0.clone())),
        }));
        Ok(())
    }
}
fn mixed_calls() -> Vec<StreamDelta> {
    let prefix = ToolCallId::new().0;
    let mut front = call(&format!("{prefix}-first"));
    front.pop();
    front.extend(call(&format!("{prefix}-second")));
    front.pop();
    let backend = call(&format!("{prefix}-completed-backend"))
        .into_iter()
        .map(|delta| match delta {
            StreamDelta::ToolCallStart { call_id, .. } => StreamDelta::ToolCallStart {
                call_id,
                name: "side_effect".into(),
            },
            other => other,
        });
    front.extend(backend);
    front
}
#[tokio::test]
#[allow(clippy::too_many_lines)] // Complete mixed-batch public process journey.
async fn child() {
    let Ok(directory) = std::env::var("CRABBER_FRONTEND_CHILD") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let step: u8 = std::env::var("CRABBER_FRONTEND_STEP")
        .unwrap()
        .parse()
        .unwrap();
    let store: Arc<dyn Store> = if std::env::var_os("CRABBER_FRONTEND_POSTGRES").is_some() {
        #[cfg(feature = "postgres")]
        {
            Arc::new(
                crabber::session::PostgresStore::connect(
                    &crabber_test_support::postgres::TestDatabase::attached_url()
                        .await
                        .unwrap(),
                )
                .await
                .unwrap(),
            )
        }
        #[cfg(not(feature = "postgres"))]
        {
            panic!("PostgreSQL feature required")
        }
    } else {
        let database = directory.join("frontend.sqlite");
        if step == 0 {
            crabber::session::SqliteStore::migrate(&database)
                .await
                .unwrap();
        }
        Arc::new(
            crabber::session::SqliteStore::connect(database)
                .await
                .unwrap(),
        )
    };
    let provider = FakeProvider::scripted(vec![if step == 0 { mixed_calls() } else { done() }]);
    let protected = Arc::new(AtomicUsize::new(0));
    let backend_dispatches = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .behavior_version("frontend-process.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(Arc::new(EffectExtension(directory.clone())), Scope::Global)
        .shared_extension(
            Arc::new(BackendBoundary(backend_dispatches.clone())),
            Scope::Global,
        )
        .shared_extension(
            Arc::new(super::super::tool_continuation::protection::Protection(
                protected.clone(),
            )),
            Scope::Global,
        )
        .build()
        .unwrap();
    if step == 0 {
        let session = SessionId::new();
        let run = start(&agent, &session).await;
        assert_eq!(backend_dispatches.load(Ordering::SeqCst), 1);
        assert_eq!(protected.load(Ordering::SeqCst), 2);
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.frontend_calls.len(), 2);
        assert_eq!(
            store.list_unfinished_tool_calls(&run).await.unwrap().len(),
            2
        );
        std::fs::write(
            directory.join("scope"),
            serde_json::to_vec(&(session, run)).unwrap(),
        )
        .unwrap();
    } else {
        let (session, run): (SessionId, crabber::core::RunId) =
            serde_json::from_slice(&std::fs::read(directory.join("scope")).unwrap()).unwrap();
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        let request = response(&snapshot);
        let ToolContinuation::Started { receipt, handle } =
            agent.continue_frontend(request.clone()).await.unwrap()
        else {
            panic!("fresh result grant")
        };
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        assert_eq!(backend_dispatches.load(Ordering::SeqCst), 0);
        assert_eq!(protected.load(Ordering::SeqCst), 4);
        assert_eq!(
            store.list_unfinished_tool_calls(&run).await.unwrap().len(),
            0
        );
        agent.close_extensions().await.unwrap();
        assert!(
            matches!(agent.continue_frontend(request).await.unwrap(),ToolContinuation::Replayed(saved) if saved==receipt)
        );
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(protected.load(Ordering::SeqCst), 4);
    }
}
fn journey(postgres_url: Option<&str>) {
    let directory = tempfile::tempdir().unwrap();
    for step in 0..2 {
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "frontend_continuation::process::child",
                "--nocapture",
            ])
            .env("CRABBER_FRONTEND_CHILD", directory.path())
            .env("CRABBER_FRONTEND_STEP", step.to_string());
        if let Some(url) = postgres_url {
            #[cfg(not(feature = "postgres"))]
            let _ = url;
            #[cfg(feature = "postgres")]
            child.envs(
                crabber_test_support::postgres::TestDatabase::child_environment(url).unwrap(),
            );
            child.env("CRABBER_FRONTEND_POSTGRES", "1");
        } else {
            child.env_remove("CRABBER_FRONTEND_POSTGRES");
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "frontend child {step}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(directory.path().join("effect").exists());
}
#[test]
fn sqlite_mixed_frontend_batch_survives_fresh_process() {
    journey(None);
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_mixed_frontend_batch_survives_fresh_process() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        journey(Some(&url));
    })
    .await
    .unwrap();
}
