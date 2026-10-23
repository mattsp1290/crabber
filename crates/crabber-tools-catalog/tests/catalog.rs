//! Catalog identities, independent parity fixtures, and mounting semantics.
#[path = "../../../examples/mount-standard/src/probe.rs"]
mod probe;
use crabber::{
    Agent, ExtensionError, FakeProvider, PermissionDecision, StaticPolicy, ToolDefinition,
    core::{ToolCallStatus, ToolInfo},
    extension::{Extension, Scope},
    session::MemoryStore,
};
use crabber_tools_catalog::prelude::*;
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
fn options(_path: &std::path::Path) -> Policies {
    Policies {
        deny: crabber_tools_catalog::prelude::PathDenyPolicy::none(),
        enabled: EnabledSet::only([
            ToolId::FileRead,
            ToolId::FileWrite,
            ToolId::FileEdit,
            ToolId::FileList,
            ToolId::Search,
            ToolId::Shell,
        ]),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 65536,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist()).unwrap(),
        url_fetch: None,
        tracker: None,
        user_interact: None,
        limits: Limits {
            max_in_flight: 4,
            max_blocking_wait: Duration::from_secs(2),
        },
    }
}
fn assert_deny_contract(name: &str, section: &str) {
    if matches!(
        name,
        "file_read"
            | "file_write"
            | "file_edit"
            | "file_list"
            | "search"
            | "glob"
            | "apply_patch"
            | "url_fetch"
            | "multi_edit"
            | "file_delete"
            | "file_move"
    ) {
        assert!(
            section.contains("path_denied"),
            "{name} must document path denial"
        );
    }
}
#[test]
fn metadata_parity_docs_hashes_and_prelude() {
    let first = metadata();
    assert_eq!(first, metadata());
    // Hashes follow ToolId::ALL order; deliberate metadata changes update the versioned entries.
    assert_eq!(ToolId::EINO_PARITY.len(), 11);
    assert!(
        ToolId::EINO_PARITY
            .iter()
            .all(|id| ToolId::ALL.contains(id))
    );
    let hashes: Vec<String> = serde_json::from_str(include_str!("hashes.json")).unwrap();
    assert_eq!(
        first.iter().map(|x| x.2.clone()).collect::<Vec<_>>(),
        hashes
    );
    let doc = include_str!("../../../docs/tool-contract.md");
    let allowed = include_str!("../../../fixtures/eino-tools/ALLOWED-DIFFS.md");
    let allowed: Vec<Value> = serde_json::from_str(
        allowed
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    let mut used = 0;
    for (id, info, _) in first {
        assert_eq!(info.name, id.name());
        assert_eq!(info.retry_safe, id.retry_safe());
        assert_eq!(
            id.mutating(),
            matches!(
                id,
                ToolId::FileWrite
                    | ToolId::FileEdit
                    | ToolId::Shell
                    | ToolId::ApplyPatch
                    | ToolId::MultiEdit
                    | ToolId::FileDelete
                    | ToolId::FileMove
            )
        );
        let dir = if ToolId::EINO_PARITY.contains(&id) {
            "eino-tools"
        } else {
            "crabber-tools"
        };
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("../../fixtures/{dir}/{}.json", info.name));
        let mut golden: Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        for d in allowed.iter().filter(|d| d["tool"] == info.name) {
            let slot = golden.pointer_mut(d["pointer"].as_str().unwrap()).unwrap();
            assert_eq!(*slot, d["upstream"]);
            *slot = d["rust"].clone();
            used += 1;
        }
        assert_eq!(
            golden,
            json!({"name":info.name,"description":info.description,"parameters":info.parameters})
        );
        let section = doc
            .split(&format!("## {}\n", info.name))
            .nth(1)
            .unwrap()
            .split("\n## ")
            .next()
            .unwrap();
        assert_deny_contract(&info.name, section);
        let schema: Value = serde_json::from_str(
            section
                .split("```json\n")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(schema, info.parameters);
    }
    assert_eq!(used, allowed.len());
    // These names compile from the prelude alone.
    let _: Option<PathDenyPolicy> = None;
    let _: Option<(
        Definition,
        RelPath,
        RunAs,
        HostPattern,
        UrlFetchPolicy,
        TrackerPolicy,
    )> = None;
    let _: Option<(UserInteractPolicy, Surface)> = None;
}
#[test]
fn hashes_cover_policies_root_enabled_and_restrictions() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let hash = StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), options(a.path()))
        .unwrap()
        .config_hash();
    assert_eq!(
        hash,
        StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), options(a.path()))
            .unwrap()
            .config_hash()
    );
    assert_ne!(
        hash,
        StandardTools::new(WorkspaceRoot::open(b.path()).unwrap(), options(b.path()))
            .unwrap()
            .config_hash()
    );
    for change in 0..5 {
        let mut o = options(a.path());
        match change {
            0 => o.shell.env = EnvPolicy::Replace(vec![]),
            1 => o.shell.run_as = Some(RunAs { uid: 123, gid: 456 }),
            2 => o.restrict_to_enabled = true,
            3 => o.enabled = EnabledSet::only([ToolId::FileRead]),
            _ => o.deny = PathDenyPolicy::new([".env".into()]).unwrap(),
        }
        assert_ne!(
            hash,
            StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), o)
                .unwrap()
                .config_hash()
        );
    }
    let mut first = options(a.path());
    first.deny = PathDenyPolicy::new([".env", "*.pem"].map(String::from)).unwrap();
    let mut reordered = options(a.path());
    reordered.deny = PathDenyPolicy::new(["*.pem", ".env", ".env"].map(String::from)).unwrap();
    assert_eq!(
        StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), first)
            .unwrap()
            .config_hash(),
        StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), reordered)
            .unwrap()
            .config_hash(),
    );
    let mut o = options(a.path());
    o.enabled = EnabledSet::only([]);
    assert!(StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), o).is_err());
}
struct Host;
#[async_trait::async_trait]
impl crabber::JsonToolExecutor for Host {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!({"host":true}))
    }
}
fn host() -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "finish_job".into(),
            description: "test host tool".into(),
            parameters: json!({"type":"object"}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(Host),
    })
}
#[tokio::test]
async fn allowlist_and_run_wide_restriction() {
    let tmp = tempfile::tempdir().unwrap();
    for restrict in [false, true] {
        let mut o = options(tmp.path());
        o.enabled = EnabledSet::only([ToolId::FileRead]);
        o.restrict_to_enabled = restrict;
        let tools = StandardTools::new(WorkspaceRoot::open(tmp.path()).unwrap(), o).unwrap();
        assert_eq!(tools.definitions().len(), 1);
        let store = Arc::new(MemoryStore::new());
        let agent = Agent::builder()
            .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
            .store(store.clone())
            .provider(Arc::new(FakeProvider::scripted(vec![
                turn("shell", json!({"cmd":"true"})),
                turn("finish_job", json!({})),
                done(),
            ])))
            .config(config(tmp.path()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .shared_extension(Arc::new(tools), Scope::Global)
            .shared_tool(host())
            .build()
            .unwrap();
        let run = agent.prompt(None, "allowlist").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        let records = calls(&store, &session).await;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(
            records[1].status,
            if restrict {
                ToolCallStatus::Failed
            } else {
                ToolCallStatus::Completed
            }
        );
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn collision_propagates() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![done()])))
        .config(config(a.path()))
        .shared_extension(
            Arc::new(
                StandardTools::new(WorkspaceRoot::open(a.path()).unwrap(), options(a.path()))
                    .unwrap(),
            ),
            Scope::Global,
        )
        .shared_extension(
            Arc::new(
                StandardTools::new(WorkspaceRoot::open(b.path()).unwrap(), options(b.path()))
                    .unwrap(),
            ),
            Scope::Global,
        )
        .build()
        .unwrap();
    let error = match agent.prompt(None, "collision").await {
        Ok(run) => match run.done().await {
            Ok(_) => panic!("collision accepted"),
            Err(e) => e,
        },
        Err(e) => e,
    };
    assert!(
        error.to_string().contains("context initialization failed"),
        "{error}"
    );
    agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn all_six_tools_execute_and_probe() {
    let tmp = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("file_write", json!({"path":"x","content":"hello"})),
            turn("file_read", json!({"path":"x"})),
            turn(
                "file_edit",
                json!({"path":"x","anchor":"hello","replacement":"world"}),
            ),
            turn("file_list", json!({})),
            turn("search", json!({"pattern":"world"})),
            turn("shell", json!({"cmd":"cat x"})),
            done(),
        ])))
        .config(config(tmp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(
                StandardTools::new(
                    WorkspaceRoot::open(tmp.path()).unwrap(),
                    options(tmp.path()),
                )
                .unwrap(),
            ),
            Scope::Global,
        )
        .build()
        .unwrap();
    let run = agent.prompt(None, "six tools").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = calls(&store, &session).await;
    assert_eq!(records.len(), 6);
    for r in &records {
        assert_eq!(r.status, ToolCallStatus::Completed);
        assert_eq!(value(r)["outcome"], "succeeded");
    }
    assert_eq!(value(&records[5])["stdout"], "world");
    agent.close_extensions().await.unwrap();
    probe::run(false, None).await.unwrap();
    probe::run(true, None).await.unwrap();
}

#[derive(serde::Deserialize)]
struct UpstreamRow {
    id: String,
    name: String,
    binding: String,
    retry_safe: bool,
    // Rust has no per-tool concurrency flag; docs/standard-tools-parity.md records this.
    #[serde(rename = "concurrent")]
    _concurrent: bool,
}
fn upstream_catalog() -> Vec<UpstreamRow> {
    serde_json::from_str(include_str!("../../../fixtures/eino-tools/catalog.json")).unwrap()
}
#[test]
fn catalog_matches_upstream_definitions() {
    use std::collections::BTreeSet;
    let rows = upstream_catalog();
    assert_eq!(rows.len(), 11);
    assert_eq!(
        rows.iter().map(|r| r.id.as_str()).collect::<BTreeSet<_>>(),
        ToolId::EINO_PARITY.map(ToolId::id).into_iter().collect()
    );
    for row in rows {
        let id = ToolId::EINO_PARITY
            .into_iter()
            .find(|id| id.id() == row.id)
            .unwrap();
        assert_eq!(id.name(), row.name);
        assert_eq!(id.retry_safe(), row.retry_safe);
        assert_eq!(id.mutating(), !row.retry_safe && row.binding == "workspace");
    }
}
#[test]
fn fixture_names_match_catalog_rows() {
    use std::collections::BTreeSet;
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/eino-tools");
    let names: BTreeSet<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.extension().is_some_and(|e| e == "json") && p.file_stem().unwrap() != "catalog"
        })
        .map(|p| p.file_stem().unwrap().to_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        names,
        upstream_catalog().into_iter().map(|r| r.name).collect()
    );
}
#[test]
fn parity_docs_cover_allowed_diffs_and_inventory() {
    let doc = include_str!("../../../docs/standard-tools-parity.md");
    let allowed = include_str!("../../../fixtures/eino-tools/ALLOWED-DIFFS.md");
    let allowed: Vec<Value> = serde_json::from_str(
        allowed
            .split("```json\n")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    for entry in allowed {
        assert!(doc.contains(entry["tool"].as_str().unwrap()));
        assert!(doc.contains(entry["pointer"].as_str().unwrap()));
    }
    for heading in [
        "Package map",
        "Deliberate deviations",
        "Per-tool contract matrix",
        "Beyond parity",
        "How parity is verified",
    ] {
        assert!(doc.contains(&format!("## {heading}\n")));
    }
    let matrix = doc
        .split("## Per-tool contract matrix\n")
        .nth(1)
        .unwrap()
        .split("\n## ")
        .next()
        .unwrap();
    for stem in [
        "fileops",
        "glob",
        "search",
        "shell",
        "applypatch",
        "urlfetch",
        "trackerwrite",
        "userinteract",
    ] {
        assert!(matrix.contains(&format!("### {stem}\n")));
    }
}

#[path = "../../../examples/mount-standard/src/contexts.rs"]
mod contexts;
#[tokio::test]
async fn contextual_host_probe() {
    contexts::run().await.unwrap();
}
