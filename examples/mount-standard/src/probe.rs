//! Consumer-shaped, credential-free scripted host integration.
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition,
    core::{ContentBlock, EventKind, ToolCallId, ToolCallStatus, ToolInfo},
    extension::{Extension, Registrar, Scope},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_tools_catalog::prelude::*;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[allow(
    clippy::needless_pass_by_value,
    reason = "Preserve the shared scripted-provider fixture signature used by the consumer."
)]
fn turn(name: &str, args: Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: args.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}
struct Record(Arc<Mutex<Vec<Value>>>);
#[async_trait]
impl crabber::JsonToolExecutor for Record {
    async fn execute(&self, args: Value) -> Result<Value, ExtensionError> {
        self.0.lock().unwrap().push(args);
        Ok(json!({"recorded":true}))
    }
}
struct HostExtension(Arc<Mutex<Vec<Value>>>);
fn record_tool(name: &str, record: Arc<Mutex<Vec<Value>>>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "Record a synthetic host payload".into(),
            parameters: json!({"type":"object","properties":{"label":{"type":"string"},"payload":{}},"additionalProperties":false}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(Record(record)),
    })
}
#[async_trait]
impl Extension for HostExtension {
    fn id(&self) -> &'static str {
        "example/host"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "host-v1".into()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        r.tool(record_tool("finish_job", self.0.clone()));
        Ok(())
    }
}
/// Mount a per-node subset beside a host extension and a plain host tool.
/// Optionally add a consumer extension registering `consumer_marker`.
#[allow(
    clippy::too_many_lines,
    reason = "Keep the public API scenario and its ordered assertions together."
)]
pub async fn run(
    deny_shell: bool,
    extra: Option<Arc<dyn Extension>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    std::fs::write(temp.path().join("README.md"), "known-line\n")?;
    std::fs::write(temp.path().join("AGENTS.md"), "Use the standard tools.\n")?;
    std::fs::write(temp.path().join(".env"), "host-deny-probe-needle\n")?;
    std::os::unix::fs::symlink(".env", temp.path().join("private-notes"))?;
    let root = WorkspaceRoot::open(temp.path())?;
    let tools = StandardTools::new(
        root.clone(),
        Policies {
            deny: PathDenyPolicy::new(
                [
                    "*.pem",
                    ".env",
                    ".env.*",
                    ".netrc",
                    "id_*",
                    "**/.git/config",
                ]
                .map(String::from),
            )?,
            enabled: EnabledSet::only([
                ToolId::FileRead,
                ToolId::FileWrite,
                ToolId::FileEdit,
                ToolId::FileList,
                ToolId::Search,
                ToolId::Shell,
                ToolId::UserInteract,
            ]),
            restrict_to_enabled: false,
            shell: ShellPolicy {
                shell_binary: "/bin/sh".into(),
                startup_mode: StartupMode::NonLogin,
                env: EnvPolicy::minimal_allowlist(),
                run_as: None,
                output_cap_bytes: 65536,
            },
            search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
            url_fetch: None,
            tracker: None,
            user_interact: Some(UserInteractPolicy::durable(
                crabber::core::AuthorityRequirement::new("example-operator", "1")?,
            )),
            limits: Limits {
                max_in_flight: 4,
                max_blocking_wait: Duration::from_secs(2),
            },
        },
    )?;
    let mut script = vec![
        turn(
            "file_write",
            json!({"path":"notes.txt","content":"hello world\n"}),
        ),
        turn("search", json!({"pattern":"known-line","path":"README.md"})),
        turn("shell", json!({"cmd":"cat notes.txt"})),
        turn(
            "file_edit",
            json!({"path":"notes.txt","anchor":"world","replacement":"Crabber"}),
        ),
        turn("glob", json!({"pattern":"**"})),
        turn("user_interact", json!({"question":"Proceed?"})),
        turn("finish_job", json!({"label":"done","payload":{"ok":true}})),
    ];
    if extra.is_some() {
        script.push(turn("consumer_marker", json!({})));
        script.push(turn("shell", json!({"cmd":"echo ok"})));
    }
    let denied_start = script.len();
    script.extend([
        turn("file_read", json!({"path":".env"})),
        turn("file_read", json!({"path":"private-notes"})),
        turn("file_list", json!({"recursive":true})),
        turn(
            "search",
            json!({"pattern":"host-deny-probe-needle","glob":["**"]}),
        ),
    ]);
    script.push(vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]);
    let record = Arc::new(Mutex::new(vec![]));
    let store = Arc::new(MemoryStore::new());
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "probe".into();
    config.directory = root.path().to_string_lossy().into_owned();
    let policy = if deny_shell {
        StaticPolicy::new(PermissionDecision::Allow).with_rule("shell", PermissionDecision::Deny)
    } else {
        StaticPolicy::new(PermissionDecision::Allow)
    };
    let has_extra = extra.is_some();
    let mut builder = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(script)))
        .config(config)
        .policy(Arc::new(policy))
        .shared_extension(Arc::new(tools), Scope::Global)
        .shared_extension(Arc::new(HostExtension(record.clone())), Scope::Global)
        .shared_tool(record_tool("fixture", Arc::new(Mutex::new(vec![]))));
    if let Some(extension) = extra {
        builder = builder.shared_extension(extension, Scope::Global);
    }
    let agent = builder.build()?;
    let mut run = agent.prompt(None, "Exercise the tools").await?;
    let session = run.session_id().clone();
    let run_id = run.run_id().clone();
    let mut settled = 0;
    let mut pauses = 0;
    loop {
        let mut events = run.events();
        while let Some(event) = events.recv().await? {
            if matches!(
                event.kind,
                EventKind::ToolCallStarted | EventKind::ToolCallSettled
            ) {
                println!("{}", serde_json::to_string(&*event)?);
                if event.kind == EventKind::ToolCallSettled {
                    settled += 1;
                }
            }
        }
        let status = run.done().await?.status;
        if status != crabber::core::RunStatus::Paused {
            assert_eq!(status, crabber::core::RunStatus::Completed);
            break;
        }
        pauses += 1;
        run = answer_questions(&agent, &session, &run_id, json!("yes")).await?;
    }
    assert_eq!(pauses, 1);
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session,
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 1_000_000,
                encoded_bytes: 2_000_000,
            },
            continuation: None,
        })
        .await?
    else {
        panic!("probe snapshot page required")
    };
    assert_eq!(page.tool_calls.len(), denied_start + 4);
    assert!(settled >= 7);
    for (i, call) in page.tool_calls.iter().enumerate() {
        let expected = if i == 4
            || i == denied_start
            || i == denied_start + 1
            || ((i == 2 || (has_extra && i == 8)) && deny_shell)
        {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        };
        assert_eq!(call.status, expected, "{}", call.name);
    }
    let value = |i: usize| -> Value {
        match &page.tool_calls[i].result.as_ref().unwrap().content[0] {
            ContentBlock::Text { text } => serde_json::from_str(text).unwrap(),
            v => panic!("unexpected {v:?}"),
        }
    };
    for index in [denied_start, denied_start + 1] {
        assert_eq!(value(index)["error"]["category"], "path_denied");
    }
    let listing = value(denied_start + 2);
    assert!(
        listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| entry["path"] != ".env" && entry["path"] != "private-notes")
    );
    assert_eq!(value(denied_start + 3)["match_count"], 0);
    assert_eq!(value(5)["answer"], "yes");
    assert_eq!(value(0)["bytes_written"], 12);
    assert_eq!(value(1)["match_count"], 1);
    assert_eq!(value(3)["anchor_occurrences"], 1);
    if !deny_shell {
        assert_eq!(value(2)["stdout"], "hello world\n");
        if has_extra {
            assert_eq!(value(8)["stdout"], "ok\n");
        }
    }
    assert_eq!(
        *record.lock().unwrap(),
        vec![json!({"label":"done","payload":{"ok":true}})]
    );
    assert_eq!(
        std::fs::read_to_string(temp.path().join("notes.txt"))?,
        "hello Crabber\n"
    );
    agent.close_extensions().await?;
    Ok(())
}

/// A scripted non-browser host records authenticated resolutions and continues.
pub async fn answer_questions(
    agent: &Agent,
    session: &crabber::core::SessionId,
    run: &crabber::core::RunId,
    value: Value,
) -> Result<crabber::RunHandle, Box<dyn std::error::Error>> {
    use crabber::{
        ContinueTool, ToolContinuation,
        core::ControlId,
        session::{ApprovalResolverClaim, ToolInteractionAnswer, ToolInteractionReply},
    };
    let snapshot = agent
        .tool_continuation_snapshot(session, run)
        .await?
        .ok_or("no tool continuation")?;
    let mut revision = snapshot.revision;
    for interaction in &snapshot.interactions {
        let receipt = agent
            .answer_tool_interaction(ToolInteractionAnswer {
                id: ControlId::new(),
                expected_revision: revision,
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: snapshot.continuation.clone(),
                interaction_id: interaction.id.clone(),
                resolver: ApprovalResolverClaim {
                    resolver_id: "example-host".into(),
                    // Synthetic fixture only: real hosts derive authority from the authenticated resolver.
                    authority: interaction.description.authority().clone(),
                    authority_claim: "scripted synthetic operator".into(),
                },
                reply: ToolInteractionReply::Answer {
                    value: value.clone(),
                },
            })
            .await?;
        revision = receipt.revision;
    }
    let ToolContinuation::Started { handle, .. } = agent
        .continue_tool(ContinueTool {
            id: ControlId::new(),
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation,
            expected_revision: revision,
        })
        .await?
    else {
        return Err("fresh continuation replayed".into());
    };
    Ok(handle)
}
