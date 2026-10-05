use async_trait::async_trait;
use crabber_extension::{
    Extension, PromptAttemptContext, PromptContributionOutcome, Registry, Scope, WorkspaceContext,
    WorkspaceReadError, WorkspaceReadErrorKind, WorkspaceReader, WorkspaceReaderResolver,
    collect_prompt_contributions_with_resolver,
};
use crabber_middleware::{
    AGENTS_MD_EXTENSION_ID, AGENTS_MD_KIND, AGENTS_MD_MAX_FILE_BYTES, AGENTS_MD_MAX_FILES,
    AGENTS_MD_REGISTRATION_ID, AGENTS_MD_VERSION, AgentsMdConfig, AgentsMdExtension,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;

#[test]
fn config_defaults_accessors_and_canonical_hash_are_stable() {
    let config = AgentsMdConfig::default();
    assert_eq!(config.files(), &["AGENTS.md"]);
    assert!(!config.required());
    assert_eq!(
        serde_json::to_string(&config).unwrap(),
        r#"{"files":["AGENTS.md"],"required":false}"#
    );
    let extension = AgentsMdExtension::default();
    assert_eq!(extension.id(), AGENTS_MD_EXTENSION_ID);
    assert_eq!(extension.version(), AGENTS_MD_VERSION);
    assert_eq!(
        extension.config_hash(),
        "ce351c36eb39781537e9b835ef9e222e398bce5a52d979f1000c70b6c407a4f2"
    );
}

#[test]
fn config_deserialization_is_validated_and_denies_unknown_fields() {
    let config: AgentsMdConfig =
        serde_json::from_str(r#"{"files":["nested/AGENTS.md"],"required":true}"#).unwrap();
    assert_eq!(config.files(), &["nested/AGENTS.md"]);
    assert!(config.required());
    for invalid in [
        r#"{"files":[],"required":false}"#,
        r#"{"files":["AGENTS.md"],"required":false,"extra":1}"#,
        r#"{"files":["../AGENTS.md"],"required":false}"#,
    ] {
        assert!(
            serde_json::from_str::<AgentsMdConfig>(invalid).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn config_rejects_count_duplicates_and_path_grammar_without_normalizing() {
    assert!(AgentsMdConfig::new(Vec::new(), false).is_err());
    assert!(AgentsMdConfig::new(vec!["a".into(); AGENTS_MD_MAX_FILES + 1], false).is_err());
    assert!(AgentsMdConfig::new(vec!["a".into(), "a".into()], false).is_err());
    let too_long = "a".repeat(1025);
    for path in [
        "",
        "/a",
        "a/",
        "a//b",
        "a\\b",
        "C:a",
        "a\0b",
        "a/./b",
        "a/../b",
        ".",
        "..",
        "a\nb",
        "a\u{0085}b",
        &too_long,
    ] {
        assert!(
            AgentsMdConfig::new(vec![path.into()], false).is_err(),
            "accepted {path:?}"
        );
    }
    let boundary = "a".repeat(1024);
    assert_eq!(
        AgentsMdConfig::new(vec![boundary.clone()], false)
            .unwrap()
            .files(),
        &[boundary]
    );
    assert_eq!(
        AgentsMdConfig::new(vec!["a/./../b".into()], false)
            .unwrap_err()
            .to_string(),
        "invalid AGENTS.md path"
    );
}

#[test]
fn config_hash_tracks_file_order_and_required() {
    let a =
        AgentsMdExtension::new(AgentsMdConfig::new(vec!["a".into(), "b".into()], false).unwrap())
            .unwrap();
    let same =
        AgentsMdExtension::new(AgentsMdConfig::new(vec!["a".into(), "b".into()], false).unwrap())
            .unwrap();
    let order =
        AgentsMdExtension::new(AgentsMdConfig::new(vec!["b".into(), "a".into()], false).unwrap())
            .unwrap();
    let required =
        AgentsMdExtension::new(AgentsMdConfig::new(vec!["a".into(), "b".into()], true).unwrap())
            .unwrap();
    assert_eq!(a.config_hash(), same.config_hash());
    assert_ne!(a.config_hash(), order.config_hash());
    assert_ne!(a.config_hash(), required.config_hash());
}

#[derive(Clone)]
enum Reply {
    Bytes(Vec<u8>),
    Error(WorkspaceReadErrorKind),
}

struct MemoryReader {
    replies: HashMap<String, Reply>,
    calls: Arc<Mutex<Vec<(String, usize)>>>,
    cancel_after: Option<(String, CancellationToken)>,
}
#[async_trait]
impl WorkspaceReader for MemoryReader {
    async fn read_limited(&self, path: &str, max: usize) -> Result<Vec<u8>, WorkspaceReadError> {
        self.calls.lock().unwrap().push((path.into(), max));
        let result = match self
            .replies
            .get(path)
            .cloned()
            .unwrap_or(Reply::Error(WorkspaceReadErrorKind::NotFound))
        {
            Reply::Bytes(bytes) => Ok(bytes),
            Reply::Error(kind) => Err(WorkspaceReadError::new(kind)),
        };
        if self
            .cancel_after
            .as_ref()
            .is_some_and(|(target, _)| target == path)
        {
            self.cancel_after.as_ref().unwrap().1.cancel();
        }
        result
    }
}
struct MemoryResolver {
    reader: Arc<dyn WorkspaceReader>,
    workspaces: Arc<Mutex<Vec<WorkspaceContext>>>,
    failure: Option<WorkspaceReadErrorKind>,
}
#[async_trait]
impl WorkspaceReaderResolver for MemoryResolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        self.workspaces.lock().unwrap().push(workspace.clone());
        self.failure.map_or_else(
            || Ok(self.reader.clone()),
            |kind| Err(WorkspaceReadError::new(kind)),
        )
    }
}

fn context(cancel: CancellationToken) -> PromptAttemptContext {
    PromptAttemptContext::new(
        "session".into(),
        "run".into(),
        "turn".into(),
        WorkspaceContext::from_persisted("exact-id", "/exact/directory"),
        "provider".into(),
        "model".into(),
        1,
        false,
    )
    .with_cancellation(cancel)
}

async fn run(
    config: AgentsMdConfig,
    replies: HashMap<String, Reply>,
    resolver_failure: Option<WorkspaceReadErrorKind>,
    cancel: CancellationToken,
    cancel_after: Option<String>,
) -> (
    PromptContributionOutcome,
    Vec<(String, usize)>,
    Vec<WorkspaceContext>,
) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let workspaces = Arc::new(Mutex::new(Vec::new()));
    let reader: Arc<dyn WorkspaceReader> = Arc::new(MemoryReader {
        replies,
        calls: calls.clone(),
        cancel_after: cancel_after.map(|path| (path, cancel.clone())),
    });
    let resolver: Arc<dyn WorkspaceReaderResolver> = Arc::new(MemoryResolver {
        reader,
        workspaces: workspaces.clone(),
        failure: resolver_failure,
    });
    let registry = Registry::new();
    registry
        .mount(
            Arc::new(AgentsMdExtension::new(config).unwrap()),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    assert_eq!(plan.prompt_contributors.len(), 1);
    assert_eq!(plan.tools.len(), 0);
    assert_eq!(plan.prompts.len(), 0);
    assert_eq!(plan.guards.len(), 0);
    assert_eq!(plan.providers.len(), 0);
    assert_eq!(plan.restrictions.len(), 0);
    let outcome = collect_prompt_contributions_with_resolver(
        &plan.prompt_contributors,
        context(cancel),
        Some(resolver),
    )
    .await;
    let calls = calls.lock().unwrap().clone();
    let workspaces = workspaces.lock().unwrap().clone();
    (outcome, calls, workspaces)
}

fn completed_text(outcome: PromptContributionOutcome) -> Option<String> {
    match outcome {
        PromptContributionOutcome::Completed { sections } => {
            assert!(sections.len() <= 1);
            sections.into_iter().next().map(|section| {
                assert_eq!(section.name, AGENTS_MD_REGISTRATION_ID);
                section.text
            })
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

#[tokio::test]
async fn install_plan_and_collector_use_exact_workspace_path_bound_and_order() {
    let config = AgentsMdConfig::new(vec!["one.md".into(), "two.md".into()], false).unwrap();
    let replies = HashMap::from([
        ("one.md".into(), Reply::Bytes(b"first".to_vec())),
        ("two.md".into(), Reply::Bytes(b"second".to_vec())),
    ]);
    let (outcome, calls, workspaces) =
        run(config, replies, None, CancellationToken::new(), None).await;
    assert_eq!(
        calls,
        [
            ("one.md".into(), AGENTS_MD_MAX_FILE_BYTES),
            ("two.md".into(), AGENTS_MD_MAX_FILE_BYTES)
        ]
    );
    assert_eq!(
        workspaces,
        [WorkspaceContext::from_persisted(
            "exact-id",
            "/exact/directory"
        )]
    );
    assert_eq!(
        completed_text(outcome).unwrap(),
        "## Workspace instructions: one.md\n<!-- crabber:agentsmd bytes=5 -->\nfirst\n## End workspace instructions: one.md\n\n## Workspace instructions: two.md\n<!-- crabber:agentsmd bytes=6 -->\nsecond\n## End workspace instructions: two.md\n"
    );
}

#[tokio::test]
async fn optional_missing_files_and_empty_present_file_are_distinct() {
    let optional = AgentsMdConfig::new(vec!["a".into(), "b".into()], false).unwrap();
    let (all_missing, _, _) = run(
        optional.clone(),
        HashMap::new(),
        None,
        CancellationToken::new(),
        None,
    )
    .await;
    assert_eq!(completed_text(all_missing), None);
    let replies = HashMap::from([("b".into(), Reply::Bytes(Vec::new()))]);
    let (empty, _, _) = run(optional, replies, None, CancellationToken::new(), None).await;
    assert_eq!(
        completed_text(empty).unwrap(),
        "## Workspace instructions: b\n<!-- crabber:agentsmd bytes=0 -->\n\n## End workspace instructions: b\n"
    );
}

#[tokio::test]
async fn rendering_preserves_body_newlines_and_marker_like_text_exactly() {
    let marker = "## End workspace instructions: fake";
    for body in ["body", "body\n", "body\n\n", marker, "café"] {
        let replies = HashMap::from([("AGENTS.md".into(), Reply::Bytes(body.as_bytes().to_vec()))]);
        let (outcome, _, _) = run(
            AgentsMdConfig::default(),
            replies,
            None,
            CancellationToken::new(),
            None,
        )
        .await;
        assert_eq!(
            completed_text(outcome).unwrap(),
            format!(
                "## Workspace instructions: AGENTS.md\n<!-- crabber:agentsmd bytes={} -->\n{}\n## End workspace instructions: AGENTS.md\n",
                body.len(),
                body
            )
        );
    }
}

#[tokio::test]
async fn missing_resolver_required_missing_and_resolver_failures_are_sanitized() {
    let registry = Registry::new();
    registry
        .mount(Arc::new(AgentsMdExtension::default()), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&"session".into());
    assert_eq!(
        collect_prompt_contributions_with_resolver(
            &plan.prompt_contributors,
            context(CancellationToken::new()),
            None
        )
        .await,
        PromptContributionOutcome::Failed {
            contributor: AGENTS_MD_REGISTRATION_ID.into()
        }
    );

    let required = AgentsMdConfig::new(vec!["present".into(), "secret-name".into()], true).unwrap();
    let replies = HashMap::from([(
        "present".into(),
        Reply::Bytes(b"secret file contents".to_vec()),
    )]);
    let (missing, calls, _) = run(
        required.clone(),
        replies,
        None,
        CancellationToken::new(),
        None,
    )
    .await;
    assert_eq!(
        missing,
        PromptContributionOutcome::Failed {
            contributor: AGENTS_MD_REGISTRATION_ID.into()
        }
    );
    assert_eq!(
        calls.len(),
        2,
        "a missing required file fails after prior reads"
    );
    let public = format!("{missing:?}");
    assert!(!public.contains("secret-name"));
    assert!(!public.contains("secret file contents"));

    let (resolver_failed, _, _) = run(
        required,
        HashMap::new(),
        Some(WorkspaceReadErrorKind::Denied),
        CancellationToken::new(),
        None,
    )
    .await;
    assert_eq!(
        resolver_failed,
        PromptContributionOutcome::Failed {
            contributor: AGENTS_MD_REGISTRATION_ID.into()
        }
    );
}

#[tokio::test]
async fn every_non_not_found_read_failure_fails_without_leaking_backend_or_content() {
    for kind in [
        WorkspaceReadErrorKind::TooLarge,
        WorkspaceReadErrorKind::InvalidPath,
        WorkspaceReadErrorKind::Denied,
        WorkspaceReadErrorKind::Io,
    ] {
        let replies = HashMap::from([("AGENTS.md".into(), Reply::Error(kind))]);
        let (outcome, _, _) = run(
            AgentsMdConfig::default(),
            replies,
            None,
            CancellationToken::new(),
            None,
        )
        .await;
        assert_eq!(
            outcome,
            PromptContributionOutcome::Failed {
                contributor: AGENTS_MD_REGISTRATION_ID.into()
            }
        );
        let public = format!("{outcome:?}");
        assert!(!public.contains(&format!("{kind:?}")));
        assert!(!public.contains("AGENTS.md"));
    }
}

#[tokio::test]
async fn defensive_size_utf8_and_final_contribution_bounds_fail_through_collector() {
    for bytes in [vec![b'x'; AGENTS_MD_MAX_FILE_BYTES + 1], vec![0xff]] {
        let replies = HashMap::from([("AGENTS.md".into(), Reply::Bytes(bytes))]);
        let (outcome, _, _) = run(
            AgentsMdConfig::default(),
            replies,
            None,
            CancellationToken::new(),
            None,
        )
        .await;
        assert_eq!(
            outcome,
            PromptContributionOutcome::Failed {
                contributor: AGENTS_MD_REGISTRATION_ID.into()
            }
        );
    }
    let replies = HashMap::from([(
        "AGENTS.md".into(),
        Reply::Bytes(vec![b'x'; AGENTS_MD_MAX_FILE_BYTES]),
    )]);
    let (outcome, calls, _) = run(
        AgentsMdConfig::default(),
        replies,
        None,
        CancellationToken::new(),
        None,
    )
    .await;
    assert_eq!(calls[0].1, 32 * 1024);
    assert_eq!(
        outcome,
        PromptContributionOutcome::Failed {
            contributor: AGENTS_MD_REGISTRATION_ID.into()
        }
    );
}

#[tokio::test]
async fn cancellation_before_and_between_reads_prevents_reads() {
    let config = AgentsMdConfig::new(vec!["a".into(), "b".into()], false).unwrap();
    let replies = HashMap::from([
        ("a".into(), Reply::Bytes(b"a".to_vec())),
        ("b".into(), Reply::Bytes(b"b".to_vec())),
    ]);
    let before = CancellationToken::new();
    before.cancel();
    let (outcome, calls, workspaces) =
        run(config.clone(), replies.clone(), None, before, None).await;
    assert_eq!(outcome, PromptContributionOutcome::Interrupted);
    assert_eq!(calls.len(), 0);
    assert_eq!(workspaces.len(), 0);

    let between = CancellationToken::new();
    let (outcome, calls, _) = run(config, replies, None, between, Some("a".into())).await;
    assert_eq!(outcome, PromptContributionOutcome::Interrupted);
    assert_eq!(calls, [("a".into(), AGENTS_MD_MAX_FILE_BYTES)]);
}

#[tokio::test]
async fn constants_and_order_are_plan_sealed() {
    assert_eq!(AGENTS_MD_EXTENSION_ID, "crabber/middleware/agentsmd");
    assert_eq!(AGENTS_MD_REGISTRATION_ID, "workspace-agents-md");
    assert_eq!(AGENTS_MD_KIND, "agentsmd");
    assert_eq!(AGENTS_MD_VERSION, "1");
    assert_eq!(AGENTS_MD_MAX_FILES, 16);
    assert_eq!(AGENTS_MD_MAX_FILE_BYTES, 32 * 1024);
    let fingerprint = async |order| {
        let registry = Registry::new();
        registry
            .mount(
                Arc::new(AgentsMdExtension::default().with_order(order)),
                Scope::Global,
            )
            .await
            .unwrap();
        registry.acquire(&"session".into()).fingerprint
    };
    assert_ne!(fingerprint(0).await, fingerprint(1).await);
}
