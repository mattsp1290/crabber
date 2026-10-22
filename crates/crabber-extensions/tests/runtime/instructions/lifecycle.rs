use super::*;

struct PendingReader {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
    release: Arc<tokio::sync::Notify>,
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl WorkspaceReader for PendingReader {
    async fn read_limited(&self, _: &str, _: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        let _drop = Dropped(self.dropped.clone());
        self.entered.notify_one();
        self.release.notified().await;
        Err(WorkspaceReadError::new(Kind::NotFound))
    }
}
#[tokio::test]
async fn cancellation_and_deadline_retain_reader_and_capacity_until_actual_exit() {
    for interrupt in [true, false] {
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let reader = Arc::new(PendingReader {
            entered: entered.clone(),
            dropped: dropped.clone(),
            release: release.clone(),
        });
        let provider = Arc::new(FakeProvider::scripted(vec![done()]));
        let mut o = options(ReaderRoot::Workspace, vec![]);
        o.limits.max_in_flight = 1;
        o.limits.max_wait = if interrupt {
            Duration::from_secs(1)
        } else {
            Duration::from_millis(20)
        };
        let agent = builder(provider.clone(), o, fixed(reader)).build().unwrap();
        let handle = agent_support::first_prompt(&agent, &execution("ws", "/admitted"), "fixture")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        if interrupt {
            handle.interrupt();
        }
        let outcome = tokio::time::timeout(Duration::from_secs(2), handle.done())
            .await
            .unwrap();
        if interrupt {
            assert_eq!(outcome.unwrap().status, RunStatus::Interrupted);
        } else {
            assert!(outcome.is_err());
        }
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(provider.requests().is_empty());
        // The second owner cannot enter the reader while the first retains its
        // permit, even though the first caller's bounded wait has ended.
        assert!(run(&agent, "another", "/admitted").await.is_err());
        assert!(provider.requests().is_empty());
        let bounded = agent.close(Duration::from_millis(20)).await;
        assert!(!bounded.local_drained(), "{bounded:?}");
        release.notify_one();
        let joined = agent.close(Duration::from_secs(2)).await;
        assert!(joined.local_drained(), "{joined:?}");
        assert!(dropped.load(Ordering::SeqCst));
    }
}
struct RetryRewrite(PathBuf);
#[async_trait]
impl Extension for RetryRewrite {
    fn id(&self) -> &str {
        "fixture/retry"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        let file = self.0.clone();
        r.on_transform(
            ModelRequestError::ID,
            0,
            "rewrite",
            Arc::new(move |v| {
                let file = file.clone();
                Box::pin(async move {
                    std::fs::write(file, "second-payload").unwrap();
                    Ok(v)
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn instruction_changes_refresh_on_provider_retry_and_later_turn_without_accumulation() {
    use crabber::providers::{ProviderError, ProviderErrorKind};
    for retry in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("AGENTS.md");
        std::fs::write(&file, "first-payload").unwrap();
        let reader = Arc::new(WorkspaceRootReader::open(temp.path()).unwrap());
        let directory = reader.directory().to_str().unwrap().to_owned();
        let resolver = Arc::new(
            AllowListResolver::new(vec![("ws".into(), directory.clone(), reader)]).unwrap(),
        );
        let first = if retry {
            vec![StreamDelta::Error(ProviderError {
                kind: ProviderErrorKind::RateLimited,
                message: "fixture".into(),
                retryable: true,
            })]
        } else {
            call("synthetic")
        };
        let provider = Arc::new(FakeProvider::scripted(vec![first, done()]));
        let agent = builder(
            provider.clone(),
            options(ReaderRoot::Workspace, vec![]),
            Some(resolver),
        )
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .prompt_section(Arc::new(PromptSection {
            name: "static".into(),
            order: 0,
            text: "static".into(),
        }))
        .shared_tool(tool(false, Some(file.clone())))
        .shared_extension(Arc::new(RetryRewrite(file)), Scope::Global)
        .build()
        .unwrap();
        assert_eq!(
            run(&agent, "ws", &directory).await.unwrap().status,
            RunStatus::Completed
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 2);
        assert!(
            requests[0]
                .system
                .as_deref()
                .unwrap()
                .contains("first-payload")
        );
        let next = requests[1].system.as_deref().unwrap();
        assert!(
            next.starts_with("base\nstatic\n")
                && next.contains("second-payload")
                && !next.contains("first-payload")
        );
        assert_eq!(next.matches("<workspace_instructions").count(), 1);
        agent.close_extensions().await.unwrap();
    }
}
