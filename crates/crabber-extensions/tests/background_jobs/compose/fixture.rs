use super::*;

pub(super) const MARKER: &str = "ghp_0123456789abcdef";
pub(super) fn redactor() -> Arc<redact::ToolResultRedactor> {
    Arc::new(
        redact::ToolResultRedactor::new(redact::Options {
            order: -100,
            excluded_tools: vec![],
            additional_patterns: vec![],
            limits: redact::Limits {
                max_field_bytes: 4096,
                max_total_bytes: 16384,
                max_depth: 16,
                max_nodes: 100,
                max_matches_per_field: 16,
                max_patterns: 4,
                max_pattern_bytes: 256,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    )
}
pub(super) fn guard() -> Arc<guard::CommandGuard> {
    Arc::new(
        guard::CommandGuard::new(guard::Options {
            bindings: guard::default_bindings(),
            rules: vec![guard::Rule {
                id: "git-push".into(),
                executable: "git".into(),
                arg_prefix: vec!["push".into()],
            }],
            limits: guard::Limits {
                max_bindings: 8,
                max_rules: 32,
                max_rule_bytes: 2048,
                max_prefix_args: 16,
                max_json_depth: 16,
                max_json_nodes: 256,
                max_command_bytes: 4096,
                max_analysis_bytes: 8192,
                max_ast_nodes: 2048,
                max_ast_depth: 16,
                max_words: 512,
                max_word_bytes: 4096,
                max_wrapper_depth: 8,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    )
}
pub(super) fn composed(
    h: &Harness,
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    guard: Option<Arc<guard::CommandGuard>>,
    redact: bool,
    probe: Arc<ResultProbe>,
) -> Agent {
    let mut builder = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(provider)
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(h.ext.clone(), Scope::Global)
        .shared_extension(Arc::new(ProbeMount(probe)), Scope::Global);
    if let Some(guard) = guard {
        builder = builder.shared_extension(guard, Scope::Global);
    }
    if redact {
        builder = builder.shared_extension(redactor(), Scope::Global);
    }
    builder.build().unwrap()
}
pub(super) async fn seed(h: &Harness, store: &Arc<MemoryStore>) -> (Agent, SessionId, Value) {
    let a = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![done()])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = a.prompt(None, "seed").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    // Marker bytes live only in the frozen fixture environment, never arguments.
    let job = h.tools[START_TOOL]
        .execute_json_with_context(
            context_for(
                h.dir.path(),
                CancellationToken::new(),
                session.clone(),
                "workspace",
            ),
            json!({"command":command("printf '%s' \"$FIXTURE_SECRET\"")}),
        )
        .await
        .unwrap();
    let status = terminal_for(h, &session, job["id"].as_str().unwrap()).await;
    assert_eq!(status["stdout"]["text"], MARKER);
    (a, session, status)
}
pub(super) async fn protected(store: &MemoryStore, session: &SessionId, provider: &FakeProvider) {
    let messages = store.list_all_messages(session).await.unwrap();
    let events = store.list_events(session, None, 100).await.unwrap();
    for text in [
        format!("{messages:?}"),
        format!("{events:?}"),
        format!("{:?}", provider.requests()),
    ] {
        assert!(!text.contains(MARKER));
        assert!(text.contains("[REDACTED]"));
    }
}
