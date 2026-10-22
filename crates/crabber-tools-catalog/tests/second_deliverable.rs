//! All-tools mount and explicit catalog host policy contracts.
use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy,
    extension::{Extension, Scope},
    session::MemoryStore,
};
use crabber_tools_catalog::prelude::*;
use crabber_tools_testkit as support;
use serde_json::json;
use std::{collections::BTreeSet, path::Path, sync::Arc};
use support::*;
fn options(_root: &Path, hub: &Path) -> Policies {
    let binary = hub.join("fake-bn");
    script(
        &binary,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$BEANS_HUB/argv\"\n",
    );
    Policies {
        enabled: EnabledSet::all(),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 65536,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist()).unwrap(),
        url_fetch: Some(UrlFetchPolicy::default()),
        tracker: Some(TrackerPolicy {
            bn_binary: binary,
            project: "demo".into(),
            hub: hub.into(),
            actor: "test".into(),
            statuses: BTreeSet::from(["open".into()]),
            environment: vec![("PATH".into(), "/usr/bin:/bin".into())],
        }),
        user_interact: Some(UserInteractPolicy::pending()),
        limits: limits(),
    }
}
#[test]
fn explicit_policies_are_required_only_for_enabled_tools_and_hashed() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    for id in [ToolId::UrlFetch, ToolId::TrackerWrite, ToolId::UserInteract] {
        let mut o = options(root.path(), hub.path());
        o.enabled = EnabledSet::only([id]);
        o.url_fetch = None;
        o.tracker = None;
        o.user_interact = None;
        assert!(matches!(
            StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), o),
            Err(crabber::ExtensionError::Plan(_))
        ));
    }
    let mut subset = options(root.path(), hub.path());
    subset.enabled = EnabledSet::only([ToolId::FileRead]);
    subset.url_fetch = None;
    subset.tracker = None;
    subset.user_interact = None;
    assert_eq!(
        StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), subset)
            .unwrap()
            .definitions()
            .len(),
        1
    );
    for invalid in 0..3 {
        let mut o = options(root.path(), hub.path());
        o.enabled = EnabledSet::only([ToolId::FileRead]);
        if invalid == 0 {
            o.url_fetch
                .as_mut()
                .unwrap()
                .allow
                .push(HostPattern::Exact("EXAMPLE.COM".into()));
        } else if invalid == 1 {
            o.tracker.as_mut().unwrap().project = "--flag".into();
        } else {
            o.user_interact.as_mut().unwrap().max_question_bytes = 0;
        }
        assert!(matches!(
            StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), o),
            Err(crabber::ExtensionError::Plan(_))
        ));
    }
    let hash = StandardTools::new(
        WorkspaceRoot::open(root.path()).unwrap(),
        options(root.path(), hub.path()),
    )
    .unwrap()
    .config_hash();
    for change in 0..8 {
        let mut o = options(root.path(), hub.path());
        match change {
            0 => o.url_fetch.as_mut().unwrap().deny_private_ranges = false,
            1 => o
                .url_fetch
                .as_mut()
                .unwrap()
                .allow
                .push(HostPattern::exact("example.com").unwrap()),
            2 => o.tracker.as_mut().unwrap().actor = "other".into(),
            3 => o.tracker.as_mut().unwrap().project = "other".into(),
            4 => {
                o.tracker.as_mut().unwrap().statuses.insert("closed".into());
            }
            5 => o
                .tracker
                .as_mut()
                .unwrap()
                .environment
                .push(("LANG".into(), "C".into())),
            6 => o.user_interact.as_mut().unwrap().max_answer_bytes /= 2,
            _ => o.user_interact.as_mut().unwrap().max_question_bytes /= 2,
        }
        assert_ne!(
            hash,
            StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), o)
                .unwrap()
                .config_hash()
        );
    }
    let tools = StandardTools::new(
        WorkspaceRoot::open(root.path()).unwrap(),
        options(root.path(), hub.path()),
    )
    .unwrap();
    assert_eq!(tools.definitions().len(), 14);
    assert_eq!(
        tools.definitions().iter().map(|d| d.id).collect::<Vec<_>>(),
        ToolId::ALL
    );
    assert!(!ToolId::TrackerWrite.retry_safe());
    assert!(!ToolId::TrackerWrite.mutating());
    assert!(ToolId::ApplyPatch.mutating());
    assert!(!ToolId::UserInteract.retry_safe());
    assert!(!ToolId::UserInteract.mutating());
    let mut parity = options(root.path(), hub.path());
    parity.enabled = EnabledSet::eino_parity();
    assert_eq!(
        StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), parity)
            .unwrap()
            .definitions()
            .len(),
        11
    );
}
#[tokio::test]
async fn all_tools_execute_on_one_agent_without_network_or_real_hub() {
    let parent = tempfile::tempdir().unwrap();
    let real = parent.path().join("workspaces");
    std::fs::create_dir(&real).unwrap();
    let alias = parent.path().join("workspace-alias");
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let root = tempfile::tempdir_in(&alias).unwrap();
    let hub = tempfile::tempdir().unwrap();
    let mut o = options(root.path(), hub.path());
    o.user_interact = Some(UserInteractPolicy::durable(
        crabber::core::AuthorityRequirement::new("example-operator", "1").unwrap(),
    ));
    let admitted = WorkspaceRoot::open(root.path()).unwrap();
    let file_url = url_from_path(&admitted.path().join("x"));
    let store = Arc::new(MemoryStore::new());
    let agent=Agent::builder().behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION"))).store(store.clone()).provider(Arc::new(FakeProvider::scripted(vec![
        turn("file_write",json!({"path":"x","content":"hello"})),
        turn("file_read",json!({"path":"x"})),
        turn("file_edit",json!({"path":"x","anchor":"hello","replacement":"world"})),
        turn("file_list",json!({})),
        turn("search",json!({"pattern":"world"})),
        turn("shell",json!({"cmd":"cat x"})),
        turn("glob",json!({"pattern":"*"})),
        turn("apply_patch",json!({"patch_text":"*** Begin Patch\n*** Add File: patched.txt\n+patched\n*** End Patch"})),
        turn("url_fetch",json!({"url":file_url})),
        turn("tracker_write",json!({"op":"comment","id":"demo-ab12","body":"done"})),
        turn("user_interact",json!({"question":"Continue?"})),
        turn("file_write",json!({"path":"extra.txt","content":"alpha"})),
        turn("file_move",json!({"path":"extra.txt","new_path":"moved/extra.txt","create_dirs":true})),
        turn("multi_edit",json!({"path":"moved/extra.txt","edits":[{"anchor":"alpha","replacement":"beta"}]})),
        turn("file_delete",json!({"path":"moved/extra.txt"})),done(),
    ]))).config(config(root.path())).policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow))).shared_extension(Arc::new(StandardTools::new(admitted, o).unwrap()),Scope::Global).build().unwrap();
    let run = agent.prompt(None, "parity tools").await.unwrap();
    let session = run.session_id().clone();
    let run_id = run.run_id().clone();
    assert_eq!(
        run.done().await.unwrap().status,
        crabber::core::RunStatus::Paused
    );
    probe::answer_questions(&agent, &session, &run_id, json!("yes"))
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = calls(&store, &session).await;
    assert_eq!(records.len(), 15);
    assert!(!root.path().join("moved/extra.txt").exists());
    assert!(root.path().join("moved").is_dir());
    assert_eq!(
        EnabledSet::eino_parity(),
        EnabledSet::only(ToolId::EINO_PARITY)
    );
    for r in &records {
        assert_eq!(r.status, crabber::core::ToolCallStatus::Completed, "{r:?}");
        let result = value(r);
        if result["outcome"] == "pending" {
            assert_eq!(r.name, "user_interact", "{r:?}");
        } else {
            assert_eq!(result["outcome"], "succeeded", "{r:?}");
        }
    }
    assert_eq!(
        value(records.iter().find(|r| r.name == "url_fetch").unwrap())["content"],
        "world"
    );
    let questions: Vec<_> = records
        .iter()
        .filter(|r| r.name == "user_interact")
        .map(value)
        .collect();
    assert_eq!(
        questions,
        vec![json!({"outcome":"succeeded","answer":"yes"})]
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("patched.txt")).unwrap(),
        "patched\n"
    );
    assert_eq!(
        std::fs::read_to_string(hub.path().join("argv")).unwrap(),
        "--project\ndemo\nnote\n--\ndemo-ab12\ndone\n"
    );
    agent.close_extensions().await.unwrap();
}
fn url_from_path(path: &Path) -> String {
    url::Url::from_file_path(path).unwrap().into()
}

#[tokio::test]
async fn tracker_and_file_url_share_mount_capacity() {
    let root = tempfile::tempdir().unwrap();
    let hub = tempfile::tempdir().unwrap();
    let mut o = options(root.path(), hub.path());
    o.limits.max_blocking_wait = std::time::Duration::from_millis(30);
    script(
        &o.tracker.as_ref().unwrap().bn_binary,
        "#!/bin/sh\ntouch \"$BEANS_HUB/started\"\nexec sleep 60\n",
    );
    let admitted = WorkspaceRoot::open(root.path()).unwrap();
    std::fs::write(admitted.path().join("text"), "ok").unwrap();
    let tools = StandardTools::new(WorkspaceRoot::open(root.path()).unwrap(), o).unwrap();
    let find = |id| {
        tools
            .definitions()
            .iter()
            .find(|d| d.id == id)
            .unwrap()
            .definition
            .clone()
    };
    let tracker = find(ToolId::TrackerWrite);
    let url = find(ToolId::UrlFetch);
    let cancel = tokio_util::sync::CancellationToken::new();
    let ctx = context(&admitted, cancel.clone());
    let task = tokio::spawn(async move {
        tracker
            .executor
            .execute_owned_context(ctx, json!({"op":"close","id":"demo-ab12"}))
            .await
    });
    wait_file(&hub.path().join("started")).await;
    let args = json!({"url":url_from_path(&admitted.path().join("text"))});
    let result = url
        .executor
        .execute_owned_context(
            context(&admitted, tokio_util::sync::CancellationToken::new()),
            args.clone(),
        )
        .await
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["error"]["category"], "unavailable");
    cancel.cancel();
    assert!(task.await.unwrap().is_err());
    let result = url
        .executor
        .execute_owned_context(
            context(&admitted, tokio_util::sync::CancellationToken::new()),
            args,
        )
        .await
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["content"], "ok");
}

#[tokio::test]
async fn file_url_encodes_special_path_characters() {
    let parent = tempfile::tempdir().unwrap();
    let real = parent.path().join("percent%23path #space");
    std::fs::create_dir(&real).unwrap();
    let root = parent.path().join("workspace-alias");
    std::os::unix::fs::symlink(&real, &root).unwrap();
    let hub = tempfile::tempdir().unwrap();
    let mut o = options(&root, hub.path());
    o.enabled = EnabledSet::only([ToolId::UrlFetch]);
    let admitted = WorkspaceRoot::open(&root).unwrap();
    let path = admitted.path().join("text%23 #.txt");
    std::fs::write(&path, "encoded path").unwrap();
    let tools = StandardTools::new(admitted.clone(), o).unwrap();
    let result = tools.definitions()[0]
        .definition
        .executor
        .execute_owned_context(
            context(&admitted, tokio_util::sync::CancellationToken::new()),
            json!({"url":url_from_path(&path)}),
        )
        .await
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["content"], "encoded path");
}

#[allow(dead_code)]
#[path = "../../../examples/mount-standard/src/probe.rs"]
mod probe;
