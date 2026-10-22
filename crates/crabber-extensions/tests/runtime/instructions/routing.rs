use super::*;

#[tokio::test]
async fn no_workspace_or_unmatched_boundary_withholds_and_partial_context_fails_closed() {
    for (workspace, directory, accepted) in [
        ("", "", true),
        ("ws", "/outside", true),
        ("ws", "", false),
        ("", "/boundary/child", false),
    ] {
        let reader = MemoryReader::new(&[("AGENTS.md", b"must not read")]);
        let provider = Arc::new(FakeProvider::scripted(vec![done()]));
        let agent = builder(
            provider.clone(),
            options(ReaderRoot::Boundary, vec!["/boundary".into()]),
            fixed(reader.clone()),
        )
        .build()
        .unwrap();
        assert_eq!(run(&agent, workspace, directory).await.is_ok(), accepted);
        assert_eq!(provider.requests().len(), usize::from(accepted));
        assert!(reader.reads.lock().unwrap().is_empty());
        if accepted {
            assert_eq!(provider.requests()[0].system.as_deref(), Some("base"));
        }
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn forged_sibling_is_only_routing_and_wrong_host_rooting_can_silently_inject_wrong_content() {
    let reader = MemoryReader::new(&[("AGENTS.md", b"workspace-rooted-content")]);
    let provider = Arc::new(FakeProvider::scripted(vec![done(), done()]));
    let agent = builder(
        provider.clone(),
        options(ReaderRoot::Boundary, vec!["/boundary".into()]),
        fixed(reader.clone()),
    )
    .build()
    .unwrap();
    run(&agent, "ws", "/boundary/child").await.unwrap();
    // Deliberately mis-rooted custom reader: the child's root file is displayed
    // as a parent, while child/AGENTS.md silently returns NotFound. Hosts must
    // match ReaderRoot; the middleware cannot infer a custom capability's root.
    let frame = provider.requests()[0].system.clone().unwrap();
    assert!(frame.contains("path=\"../AGENTS.md\""));
    assert!(frame.contains("workspace-rooted-content"));
    assert!(!frame.contains("path=\"./AGENTS.md\""));
    reader.reads.lock().unwrap().clear();
    run(&agent, "forged", "/boundary/sibling").await.unwrap();
    assert_eq!(
        *reader.reads.lock().unwrap(),
        ["AGENTS.md", "sibling/AGENTS.md"]
    );
    agent.close_extensions().await.unwrap();
}
struct FailedReader(Kind);
#[async_trait]
impl WorkspaceReader for FailedReader {
    async fn read_limited(&self, _: &str, _: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        Err(WorkspaceReadError::new(self.0))
    }
}
struct FailedResolver(Kind);
#[async_trait]
impl WorkspaceReaderResolver for FailedResolver {
    async fn resolve(
        &self,
        _: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        Err(WorkspaceReadError::new(self.0))
    }
}
#[tokio::test]
async fn resolver_and_read_failures_are_closed_and_missing_files_are_withheld() {
    for kind in [Kind::Denied, Kind::Io, Kind::InvalidPath, Kind::NotFound] {
        for at_resolution in [true, false] {
            let resolver = if at_resolution {
                Some(Arc::new(FailedResolver(kind)) as Arc<dyn WorkspaceReaderResolver>)
            } else {
                fixed(Arc::new(FailedReader(kind)))
            };
            let provider = Arc::new(FakeProvider::scripted(vec![done()]));
            let agent = builder(
                provider.clone(),
                options(ReaderRoot::Workspace, vec![]),
                resolver,
            )
            .build()
            .unwrap();
            let result = run(&agent, "ws", "/admitted").await;
            assert_eq!(result.is_ok(), kind == Kind::NotFound);
            assert_eq!(
                provider.requests().len(),
                usize::from(kind == Kind::NotFound)
            );
            if let Err(e) = result {
                assert!(!e.to_string().contains("/admitted"));
            }
            agent.close_extensions().await.unwrap();
        }
    }
    let provider = Arc::new(FakeProvider::scripted(vec![done()]));
    let agent = builder(
        provider.clone(),
        options(ReaderRoot::Workspace, vec![]),
        None,
    )
    .build()
    .unwrap();
    assert!(run(&agent, "ws", "/admitted").await.is_err());
    assert!(provider.requests().is_empty());
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn one_agent_concurrently_uses_distinct_admitted_workspace_readers() {
    let temp = tempfile::tempdir().unwrap();
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    std::fs::create_dir(&a).unwrap();
    std::fs::create_dir(&b).unwrap();
    std::fs::write(a.join("AGENTS.md"), "workspace-a-only").unwrap();
    std::fs::write(b.join("AGENTS.md"), "workspace-b-only").unwrap();
    let ar = Arc::new(BoundaryReader::for_workspace(temp.path(), &a).unwrap());
    let br = Arc::new(BoundaryReader::for_workspace(temp.path(), &b).unwrap());
    let ad = ar.directory().to_str().unwrap().to_owned();
    let bd = br.directory().to_str().unwrap().to_owned();
    let resolver = Arc::new(
        AllowListResolver::new(vec![
            ("a".into(), ad.clone(), ar),
            ("b".into(), bd.clone(), br),
        ])
        .unwrap(),
    );
    let provider = Arc::new(FakeProvider::scripted(vec![done(), done()]));
    let agent = builder(
        provider.clone(),
        options(
            ReaderRoot::Boundary,
            vec![temp.path().canonicalize().unwrap().to_str().unwrap().into()],
        ),
        Some(resolver),
    )
    .build()
    .unwrap();
    let (a, b) = tokio::join!(run(&agent, "a", &ad), run(&agent, "b", &bd));
    a.unwrap();
    b.unwrap();
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let systems = requests
        .iter()
        .map(|r| r.system.clone().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        systems
            .iter()
            .filter(|s| s.contains("workspace-a-only") && !s.contains("workspace-b-only"))
            .count(),
        1
    );
    assert_eq!(
        systems
            .iter()
            .filter(|s| s.contains("workspace-b-only") && !s.contains("workspace-a-only"))
            .count(),
        1
    );
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn session_middleware_shadows_global_even_when_its_boundary_withholds() {
    let provider = Arc::new(FakeProvider::scripted(vec![done(), done()]));
    let local = SessionId::new();
    let reader = MemoryReader::new(&[("AGENTS.md", b"global-content")]);
    let agent = builder(
        provider.clone(),
        options(ReaderRoot::Workspace, vec![]),
        fixed(reader),
    )
    .shared_extension(
        Arc::new(
            WorkspaceInstructions::new(options(ReaderRoot::Boundary, vec!["/elsewhere".into()]))
                .unwrap(),
        ),
        Scope::Session(local.clone()),
    )
    .build()
    .unwrap();
    let context = ExecutionContext::new(
        SessionContext::new(local, "ws", "/admitted"),
        Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
    );
    agent_support::first_prompt(&agent, &context, "fixture")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    run(&agent, "ws", "/admitted").await.unwrap();
    let requests = provider.requests();
    assert_eq!(requests[0].system.as_deref(), Some("base"));
    assert!(
        requests[1]
            .system
            .as_deref()
            .unwrap()
            .contains("global-content")
    );
    agent.close_extensions().await.unwrap();
}
