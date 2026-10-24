//! Real ripgrep semantics and controlled process failure cases.
use crabber_tools_core::*;
use crabber_tools_search::*;
use crabber_tools_testkit as support;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use support::*;
use tokio_util::sync::CancellationToken;
fn options(path: &std::path::Path) -> Options {
    let limits = limits();
    Options {
        deny: crabber_tools_core::PathDenyPolicy::none(),
        root: WorkspaceRoot::open(path).unwrap(),
        policy: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())
            .expect("search tests require rg >=14 on PATH"),
        capacity: Capacity::new(&limits).unwrap(),
        limits,
    }
}
async fn call(o: Options, args: Value) -> Value {
    let root = o.root.clone();
    definition(Arc::new(o))
        .unwrap()
        .executor
        .execute_owned_context(context(&root, CancellationToken::new()), args)
        .await
        .unwrap()
        .json_value()
        .unwrap()
}
#[tokio::test]
async fn modes_context_globs_limits_and_runtime() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("one.txt"), "before\nabc\na.c\nABC\nafter\n").unwrap();
    std::fs::create_dir(tmp.path().join(".hidden")).unwrap();
    std::fs::write(tmp.path().join(".hidden/h"), "abc").unwrap();
    std::fs::create_dir(tmp.path().join(".git")).unwrap();
    std::fs::write(tmp.path().join(".git/h"), "abc").unwrap();
    let v = execute(
        tmp.path(),
        definition(Arc::new(options(tmp.path()))).unwrap(),
        json!({"pattern":"a.c"}),
    )
    .await;
    assert_eq!(v["match_count"], 2);
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"a.c","literal":true})).await["match_count"],
        1
    );
    assert_eq!(
        call(
            options(tmp.path()),
            json!({"pattern":"abc","ignore_case":true})
        )
        .await["match_count"],
        2
    );
    let v = call(options(tmp.path()), json!({"pattern":"abc","context":2})).await;
    assert_eq!(v["matches"][0]["before"][0]["line"], "before");
    assert_eq!(v["matches"][0]["after"].as_array().unwrap().len(), 2);
    for glob in [json!("*.txt"), json!(["*.txt"])] {
        assert_eq!(
            call(options(tmp.path()), json!({"pattern":"abc","glob":glob})).await["match_count"],
            1
        );
    }
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"abc","glob":"*.rs"})).await["match_count"],
        0
    );
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"a","limit":1})).await["truncation_reason"],
        "matches"
    );
    assert_eq!(
        call(options(tmp.path()), json!({"pattern":"("})).await["error"]["category"],
        "invalid_pattern"
    );
    for (path, cat) in [("../", "path_escape"), ("missing", "not_found")] {
        assert_eq!(
            call(options(tmp.path()), json!({"pattern":"a","path":path})).await["error"]["category"],
            cat
        );
    }
    assert_eq!(
        call(
            options(tmp.path()),
            json!({"pattern":"abc","limit":0,"timeout_seconds":0})
        )
        .await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn non_utf8_and_caps() {
    #[cfg(not(target_os = "macos"))]
    use std::os::unix::ffi::OsStringExt;
    let tmp = tempfile::tempdir().unwrap();
    // The macOS CI filesystem rejects invalid-UTF-8 names. Exercise raw
    // content here; parse.rs covers base64-encoded paths on every platform.
    #[cfg(not(target_os = "macos"))]
    let name = std::ffi::OsString::from_vec(b"bad\xff.txt".to_vec());
    #[cfg(target_os = "macos")]
    let name = std::ffi::OsString::from("bad.txt");
    std::fs::write(tmp.path().join(name), b"match \xff\n").unwrap();
    let v = call(options(tmp.path()), json!({"pattern":"match"})).await;
    assert_eq!(v["match_count"], 1);
    assert!(
        v["matches"][0]["line"]
            .as_str()
            .unwrap()
            .contains('\u{fffd}')
    );
    #[cfg(not(target_os = "macos"))]
    assert!(
        v["matches"][0]["path"]
            .as_str()
            .unwrap()
            .contains('\u{fffd}')
    );
    std::fs::write(
        tmp.path().join("long"),
        format!("match{}", "x".repeat(5000)),
    )
    .unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"match","path":"long"}),
    )
    .await;
    assert_eq!(v["matches"][0]["line_truncated"], true);
    assert_eq!(v["matches"][0]["line"].as_str().unwrap().len(), 4096);
    std::fs::write(
        tmp.path().join("many"),
        format!("match{}\n", "x".repeat(4000)).repeat(200),
    )
    .unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"match","path":"many","limit":1000}),
    )
    .await;
    assert_eq!(v["truncation_reason"], "bytes");
    assert!(serde_json::to_vec(&v["matches"]).unwrap().len() <= MAX_RESULT_BYTES);
}
#[tokio::test]
async fn fake_binary_errors_timeout_and_pipe_grace() {
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("fake-rg");
    for (body, cat, partial) in [
        ("echo failure >&2; exit 2".to_owned(), "exec_failed", false),
        (
            format!(
                "printf '%s\\n' '{}'; echo failure >&2; exit 2",
                match_line()
            ),
            "exec_failed",
            true,
        ),
        (
            "echo 'rg: regex parse error:' >&2; exit 2".to_owned(),
            "invalid_pattern",
            false,
        ),
    ] {
        script(&binary, &format!("#!/bin/sh\n{body}\n"));
        let mut o = options(tmp.path());
        o.policy.rg_binary = binary.clone();
        let v = call(o, json!({"pattern":"x"})).await;
        assert_eq!(v["error"]["category"], cat);
        assert_eq!(
            v.get("partial").and_then(Value::as_bool).unwrap_or(false),
            partial
        );
    }
    for grace in [false, true] {
        let end = if grace {
            format!("printf '%s\\n' '{}'; exit 0", match_line())
        } else {
            "wait".into()
        };
        script(
            &binary,
            &format!("#!/bin/sh\necho $$ > leader; sleep 60 & echo $! > child; {end}\n"),
        );
        let mut o = options(tmp.path());
        o.policy.rg_binary = binary.clone();
        let start = tokio::time::Instant::now();
        let v = call(
            o,
            json!({"pattern":"x","timeout_seconds":if grace{30}else{1}}),
        )
        .await;
        assert!(start.elapsed() < Duration::from_secs(7));
        assert_eq!(v["outcome"], if grace { "succeeded" } else { "failed" });
        if !grace {
            assert_eq!(v["timed_out"], true);
        }
        assert_gone(pid(&tmp.path().join("leader"))).await;
        assert_gone(pid(&tmp.path().join("child"))).await;
    }
    let mut o = options(tmp.path());
    o.policy.rg_binary = "/missing/rg".into();
    assert!(definition(Arc::new(o)).is_err());
}
#[tokio::test]
async fn config_disabled_and_environment_replaced() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("text"), "hello").unwrap();
    std::fs::write(tmp.path().join("rgconfig"), "--pre\n/bin/false\n").unwrap();
    let mut o = options(tmp.path());
    o.policy.env = EnvPolicy::Allowlist {
        keep: vec!["PATH".into()],
        set: vec![(
            "RIPGREP_CONFIG_PATH".into(),
            tmp.path().join("rgconfig").to_string_lossy().into_owned(),
        )],
    };
    assert_eq!(
        call(o, json!({"pattern":"hello","path":"text"})).await["match_count"],
        1
    );
    let binary = tmp.path().join("fake");
    script(
        &binary,
        &format!(
            "#!/bin/sh\n[ -z \"${{HOME+x}}\" ] || exit 2\nprintf '%s\\n' '{}'\n",
            match_line()
        ),
    );
    let mut o = options(tmp.path());
    o.policy.rg_binary = binary;
    o.policy.env = EnvPolicy::Replace(vec![]);
    assert_eq!(
        call(o, json!({"pattern":"x"})).await["outcome"],
        "succeeded"
    );
}
#[tokio::test]
async fn runtime_interrupt_reaps_group() {
    use crabber::{
        Agent, FakeProvider, PermissionDecision, StaticPolicy, core::ToolCallStatus,
        session::MemoryStore,
    };
    let tmp = tempfile::tempdir().unwrap();
    let binary = tmp.path().join("fake-rg");
    script(
        &binary,
        "#!/bin/sh\necho $$ > leader; sleep 60 & echo $! > child; wait\n",
    );
    let mut o = options(tmp.path());
    o.policy.rg_binary = binary;
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version(concat!("crabber-tools/", env!("CARGO_PKG_VERSION")))
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("search", json!({"pattern":"x"})),
            done(),
        ])))
        .config(config(tmp.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(definition(Arc::new(o)).unwrap())
        .build()
        .unwrap();
    let run = agent.prompt(None, "interrupt").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&tmp.path().join("child")).await;
    run.interrupt();
    support::assert_stopped_uncertain(run, &store).await;
    assert_eq!(
        calls(&store, &session).await[0].status,
        ToolCallStatus::Running
    );
    assert_gone(pid(&tmp.path().join("leader"))).await;
    assert_gone(pid(&tmp.path().join("child"))).await;
    agent.close_extensions().await.unwrap();
}

#[tokio::test]
async fn truncated_submatch_ranges_and_literal_parity() {
    let tmp = tempfile::tempdir().unwrap();
    for prefix in [4094, 4500] {
        std::fs::write(
            tmp.path().join("text"),
            format!("{}needle", "x".repeat(prefix)),
        )
        .unwrap();
        let v = call(
            options(tmp.path()),
            json!({"pattern":"needle","path":"text"}),
        )
        .await;
        assert_eq!(v["match_count"], 1);
        assert_eq!(v["matches"][0]["line_truncated"], true);
        assert_eq!(v["matches"][0]["submatches"], json!([]));
    }
    std::fs::write(tmp.path().join("text"), "needle").unwrap();
    let v = call(
        options(tmp.path()),
        json!({"pattern":"needle","path":"text","literal":true}),
    )
    .await;
    assert_eq!(v["matches"][0]["submatches"], json!([]));
}

#[tokio::test]
async fn deny_covers_search_corpus_and_explicit_aliases() {
    let temp = tempfile::tempdir().unwrap();
    for path in [
        ".env",
        "key.pem",
        "sub/id_rsa",
        "nested/.git/config",
        "ok.txt",
    ] {
        let path = temp.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "deny-corpus-needle\n").unwrap();
    }
    std::os::unix::fs::symlink(".env", temp.path().join("notes")).unwrap();
    std::os::unix::fs::symlink("nested/.git", temp.path().join("d")).unwrap();
    let policy =
        PathDenyPolicy::new([".env", "*.pem", "id_*", "**/.git/config"].map(String::from)).unwrap();
    let mut configured = options(temp.path());
    configured.deny = policy.clone();
    let result = support::execute(
        temp.path(),
        definition(Arc::new(configured)).unwrap(),
        json!({"pattern":"deny-corpus-needle","glob":["**"]}),
    )
    .await;
    assert_eq!(result["match_count"], 1, "{result}");
    assert_eq!(result["matches"][0]["path"], "ok.txt");
    for path in [".env", "notes", "d/config"] {
        let mut configured = options(temp.path());
        configured.deny = policy.clone();
        let result = support::execute(
            temp.path(),
            definition(Arc::new(configured)).unwrap(),
            json!({"pattern":"needle","path":path}),
        )
        .await;
        assert_eq!(result["error"]["category"], "path_denied");
        assert_eq!(result["error"]["message"], "path denied by host policy");
        assert_eq!(result["matches"], json!([]));
    }
}

#[test]
fn deny_dialect_agrees_with_ripgrep_exclusions() {
    use std::collections::BTreeSet;
    let temp = tempfile::tempdir().unwrap();
    let corpus = [
        "key.pem",
        "a/b/key.pem",
        ".env",
        ".env.local",
        ".netrc",
        "id_rsa",
        "a/id_x",
        ".git/config",
        "nested/.git/config",
        "ok.txt",
        "x",
        "a/x",
        "xdir/allowed",
    ];
    for name in corpus {
        let path = temp.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, "").unwrap();
    }
    for pattern in [
        "*.pem",
        ".env",
        ".env.*",
        ".netrc",
        "id_*",
        "**/.git/config",
        "**/x",
    ] {
        let deny = PathDenyPolicy::new([pattern.to_owned()]).unwrap();
        let translated = if pattern.contains('/') {
            format!("!/{pattern}")
        } else {
            format!("!{pattern}")
        };
        let output = std::process::Command::new("rg")
            .args([
                "--files",
                "--hidden",
                "--no-ignore",
                "--no-config",
                "-g",
                &translated,
            ])
            .current_dir(temp.path())
            .env_remove("RIPGREP_CONFIG_PATH")
            .output()
            .unwrap();
        assert!(output.status.success());
        let actual: BTreeSet<_> = String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .map(|name| name.strip_prefix("./").unwrap_or(name).to_owned())
            .collect();
        let expected: BTreeSet<_> = corpus
            .into_iter()
            .filter(|name| !deny.matches(std::path::Path::new(name)))
            .map(str::to_owned)
            .collect();
        assert_eq!(actual, expected, "{pattern}");
    }
}

#[tokio::test]
async fn directory_alias_cannot_expose_a_denied_descendant() {
    for (directory, leaf, patterns) in [
        ("real", "password", ["real/password", "**/password"]),
        (".git", "config", [".git/config", "**/.git/config"]),
        (
            "nested/.git",
            "config",
            ["nested/.git/config", "**/.git/config"],
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(temp.path().join(directory)).unwrap();
        std::fs::write(
            temp.path().join(directory).join(leaf),
            "before\nhidden-fixture-needle\nafter\n",
        )
        .unwrap();
        std::fs::write(
            temp.path().join(directory).join("zz-allowed.txt"),
            "public-before\nvisible-fixture-needle\npublic-after\n",
        )
        .unwrap();
        std::os::unix::fs::symlink(directory, temp.path().join("d")).unwrap();
        for pattern in patterns {
            for path in ["d", directory, "."] {
                let mut configured = options(temp.path());
                configured.deny = PathDenyPolicy::new([pattern.to_owned()]).unwrap();
                let result = support::execute(temp.path(), definition(Arc::new(configured)).unwrap(), json!({"pattern":"fixture-needle","path":path,"glob":["**"],"context":1,"limit":1})).await;
                assert_eq!(result["match_count"], 1, "{pattern}: {path}: {result}");
                assert_eq!(result["matches"][0]["line"], "visible-fixture-needle");
                assert!(!result.to_string().contains("hidden-fixture-needle"));
                assert!(!result.to_string().contains(&format!("/{leaf}")));
            }
        }
    }
}

#[tokio::test]
async fn fake_rg_denied_raw_records_do_not_consume_result_budget() {
    use base64::Engine as _;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("ok.txt"), "visible needle").unwrap();
    let binary = temp.path().join("fake-rg");
    let record = |kind: &str, path: Value, line: &str| json!({"type":kind,"data":{"path":path,"lines":{"text":line},"line_number":1,"submatches":[]}});
    let frames = [
        record("context", json!({"text":".env"}), "hidden context"),
        record("match", json!({"text":".env"}), "hidden text needle"),
        record(
            "match",
            json!({"bytes":base64::engine::general_purpose::STANDARD.encode(b"key-\xff.pem")}),
            "hidden byte needle",
        ),
        record("match", json!({"bytes":"%%%"}), "hidden malformed needle"),
        record("match", json!({"text":"ok.txt"}), "visible needle"),
    ];
    let output = frames
        .iter()
        .map(|frame| format!("'{frame}'"))
        .collect::<Vec<_>>()
        .join(" ");
    script(&binary, &format!("#!/bin/sh\nprintf '%s\\n' {output}\n"));
    let mut configured = options(temp.path());
    configured.policy.rg_binary = binary;
    configured.policy.env = EnvPolicy::Replace(vec![]);
    configured.deny = PathDenyPolicy::new([".env", "*.pem"].map(String::from)).unwrap();
    let result = support::execute(
        temp.path(),
        definition(Arc::new(configured)).unwrap(),
        json!({"pattern":"needle","context":1,"limit":1}),
    )
    .await;
    assert_eq!(result["match_count"], 1, "{result}");
    assert_eq!(result["matches"][0]["path"], "ok.txt");
    assert_eq!(result["matches"][0]["line"], "visible needle");
    assert!(!result.to_string().contains("hidden"));
}

#[tokio::test]
async fn public_agent_directory_alias_filters_resolved_denied_descendants() {
    use std::os::unix::fs::symlink;
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("real/private")).unwrap();
    std::fs::write(
        tmp.path().join("real/secret.txt"),
        "hidden before\nneedle secret\nhidden after\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("real/private/data.txt"), "needle private\n").unwrap();
    std::fs::write(
        tmp.path().join("real/visible.txt"),
        "visible before\nneedle visible\nvisible after\n",
    )
    .unwrap();
    symlink("real", tmp.path().join("alias")).unwrap();
    symlink("alias", tmp.path().join("chain")).unwrap();
    for path in ["real", "alias", "./alias", "chain"] {
        for patterns in [
            vec!["real/secret.txt", "real/private"],
            vec!["real/sec*.txt", "real/pri*"],
            vec!["real/**/secret.txt", "real/**/data.txt"],
        ] {
            let mut o = options(tmp.path());
            o.deny = PathDenyPolicy::new(patterns.into_iter().map(str::to_owned)).unwrap();
            let v = execute(
                tmp.path(),
                definition(Arc::new(o)).unwrap(),
                json!({"pattern":"needle", "path":path, "context":1, "limit":10}),
            )
            .await;
            assert_eq!(v["outcome"], "succeeded", "{path}: {v}");
            assert_eq!(v["match_count"], 1, "{path}: {v}");
            assert_eq!(v["matches"][0]["line"], "needle visible");
            assert_eq!(v["matches"][0]["before"][0]["line"], "visible before");
            assert_eq!(v["matches"][0]["after"][0]["line"], "visible after");
            let serialized = v.to_string();
            assert!(
                !serialized.contains("secret")
                    && !serialized.contains("private")
                    && !serialized.contains("hidden"),
                "{path}: {v}"
            );
        }
    }
}

#[tokio::test]
async fn public_agent_search_filters_denied_intermediate_route_descendant() {
    for target in ["route", "route/sub/.."] {
        use std::os::unix::fs::symlink;
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("real/sub")).unwrap();
        std::fs::write(
            tmp.path().join("real/secret"),
            "hidden before\nneedle hidden\nhidden after",
        )
        .unwrap();
        std::fs::write(tmp.path().join("real/visible"), "needle visible").unwrap();
        symlink("real", tmp.path().join("route")).unwrap();
        symlink(target, tmp.path().join("alias")).unwrap();
        let mut o = options(tmp.path());
        o.deny = PathDenyPolicy::new(["route/secret".to_owned()]).unwrap();
        let result = execute(
            tmp.path(),
            definition(Arc::new(o)).unwrap(),
            json!({"path":"alias","pattern":"needle","context":1}),
        )
        .await;
        assert_eq!(result["outcome"], "succeeded", "{result}");
        assert_eq!(result["match_count"], 1, "{result}");
        assert_eq!(result["matches"][0]["path"], "alias/visible");
        assert!(!result.to_string().contains("hidden") && !result.to_string().contains("secret"));
    }
}
