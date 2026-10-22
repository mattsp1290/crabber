use super::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
async fn require_real_rtk() -> Option<(PathBuf, String)> {
    let Some(path) = std::env::var_os("RTK_REDUCER_TEST_EXECUTABLE") else {
        assert_ne!(
            std::env::var("RTK_REDUCER_REQUIRE_RTK").as_deref(),
            Ok("1"),
            "required real RTK executable absent"
        );
        eprintln!(
            "real_rtk: skipped; set RTK_REDUCER_TEST_EXECUTABLE and optional RTK_REDUCER_TEST_SHA256 (extracted binary digest)"
        );
        return None;
    };
    let path = PathBuf::from(path);
    assert!(path.is_absolute());
    assert!(path.is_file());
    assert_ne!(path.metadata().unwrap().permissions().mode() & 0o111, 0);
    let digest = format!("{:x}", Sha256::digest(fs::read(&path).unwrap()));
    if let Ok(expected) = std::env::var("RTK_REDUCER_TEST_SHA256") {
        assert_eq!(
            digest, expected,
            "executable digest differs from host expectation"
        );
    } else {
        assert_ne!(
            std::env::var("RTK_REDUCER_REQUIRE_RTK").as_deref(),
            Ok("1"),
            "require mode needs RTK_REDUCER_TEST_SHA256"
        );
    }
    let private = tempfile::tempdir().unwrap();
    let mut command = tokio::process::Command::new(&path);
    command
        .arg("--version")
        .env_clear()
        .current_dir(private.path())
        .kill_on_drop(true);
    for (key, child) in [
        ("HOME", "home"),
        ("XDG_CONFIG_HOME", "config"),
        ("XDG_DATA_HOME", "data"),
        ("XDG_STATE_HOME", "state"),
        ("XDG_CACHE_HOME", "cache"),
        ("CLAUDE_CONFIG_DIR", "claude"),
        ("TMPDIR", "tmp"),
    ] {
        let dir = private.path().join(child);
        fs::create_dir(&dir).unwrap();
        command.env(key, dir);
    }
    command.envs([
        ("RTK_TELEMETRY_DISABLED", "1"),
        ("NO_COLOR", "1"),
        ("TERM", "dumb"),
        ("LANG", "C"),
        ("LC_ALL", "C"),
    ]);
    let output = tokio::time::timeout(Duration::from_secs(5), command.output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    let version = String::from_utf8(output.stdout).unwrap();
    assert_eq!(version.trim(), "rtk 0.51.0");
    eprintln!(
        "real_rtk version={} binary_sha256={digest} platform={}-{}",
        version.trim(),
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    Some((path, digest))
}
fn ambient_dirs() -> BTreeSet<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let mut dirs = BTreeSet::new();
    for (key, fallback) in [
        ("XDG_CONFIG_HOME", ".config"),
        ("XDG_DATA_HOME", ".local/share"),
    ] {
        if let Some(root) = std::env::var_os(key)
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|h| h.join(fallback)))
        {
            dirs.insert(root.join("rtk"));
        }
    }
    #[cfg(target_os = "macos")]
    if let Some(home) = home {
        dirs.insert(home.join("Library/Application Support/rtk"));
    }
    dirs
}
fn snapshot() -> BTreeMap<PathBuf, String> {
    fn visit(path: &Path, entries: &mut BTreeMap<PathBuf, String>) {
        let metadata = match fs::symlink_metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                entries.insert(path.into(), "absent".into());
                return;
            }
            Err(_) => panic!("cannot snapshot ambient RTK directory"),
        };
        let identity = if metadata.is_symlink() {
            format!("link:{:?}", fs::read_link(path).unwrap())
        } else if metadata.is_dir() {
            "directory".into()
        } else {
            format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
        };
        entries.insert(path.into(), identity);
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(&entry.unwrap().path(), entries);
            }
        }
    }
    let mut entries = BTreeMap::new();
    for dir in ambient_dirs() {
        visit(&dir, &mut entries);
    }
    entries
}
async fn verify(filter: Filter, fixture: &str, sentinel: &str) {
    let Some((executable, digest)) = require_real_rtk().await else {
        return;
    };
    let before = snapshot();
    let mut j = Journey::new("exit 0", json!(fixture), "fixture_text", json!({}), |_| {});
    let mut o = options(j.dir.path(), "exit 0");
    o.executable_path = executable;
    o.executable_sha256 = digest;
    o.bindings[0].selection = Selection::Text(filter);
    j.ext = Arc::new(RtkReducer::new(o).unwrap());
    j.agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .build()
        .unwrap();
    let session = j.run().await;
    let records = snapshot_tools(&j.store, &session).await;
    let value = result_value(&records[0]);
    let text = value.as_str().unwrap();
    let original_bytes = serde_json::to_string(fixture).unwrap().len();
    let reduced_bytes = serde_json::to_string(text).unwrap().len();
    eprintln!(
        "real_rtk {} encoded_bytes={original_bytes}->{reduced_bytes}",
        filter.cli_name()
    );
    assert!(
        text.starts_with(&marker(filter, fixture.len())),
        "real filter did not produce an accepted replacement"
    );
    assert!(text.contains(sentinel), "real filter lost the sentinel");
    assert!(reduced_bytes < original_bytes);
    observe::assert_three_surfaces(&j.store, &session, &j.provider, &records[0].id, &value).await;
    let roots: Vec<_> = fs::read_dir(j.dir.path().join("temporary"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    assert_eq!(roots.len(), 1);
    assert_eq!(
        fs::read_dir(&roots[0]).unwrap().count(),
        0,
        "invocation directory remains"
    );
    j.close().await;
    assert_eq!(before, snapshot(), "ambient RTK directories changed");
}
#[tokio::test(flavor = "multi_thread")]
async fn cargo_test_filter_preserves_failure_sentinel() {
    verify(
        Filter::CargoTest,
        include_str!("fixtures/cargo-test.txt"),
        "FAILURE_SENTINEL",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn go_test_json_filter() {
    verify(
        Filter::GoTest,
        include_str!("fixtures/go-test.jsonl"),
        "FAILURE_SENTINEL",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn git_diff_filter() {
    verify(
        Filter::GitDiff,
        include_str!("fixtures/git-diff.txt"),
        "REMOVED_SENTINEL",
    )
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn log_filter() {
    verify(
        Filter::Log,
        include_str!("fixtures/log.txt"),
        "ERROR_SENTINEL",
    )
    .await;
}
