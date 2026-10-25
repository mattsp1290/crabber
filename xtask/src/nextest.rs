//! Pinned offline process scheduling; Cargo retains explicit doctest ownership.

use std::{path::Path, process::Command};

use crate::checks::Checks;

#[cfg(feature = "postgres-check")]
pub(crate) const CLUSTER_ROLE_TEST: &str =
    "postgres::isolation_tests::migration::migration_without_createrole_fails_clearly";

pub(crate) const WORKSPACE_FEATURES: &str =
    "server-host-probe/sqlite,server-host-probe/wasm,workspace-context-probe/sqlite";

pub(crate) fn group_gate(workspace: &Path, gates: &mut Checks) {
    let mut command = Command::new("cargo");
    command
        .args([
            "nextest",
            "list",
            "--workspace",
            "--features",
            WORKSPACE_FEATURES,
            "--profile",
            "postgres",
            "-E",
            "not group(postgres)",
            "--message-format",
            "json",
        ])
        .current_dir(workspace);
    gates.captured(
        None,
        workspace,
        command,
        "every discovered entrypoint is bounded by the required PostgreSQL group",
        |output| {
            let list: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert!(list["test-count"].as_u64().is_some_and(|count| count > 0));
            for suite in list["rust-suites"].as_object().unwrap().values() {
                for test in suite["testcases"].as_object().unwrap().values() {
                    assert_ne!(
                        test["filter-match"]["status"], "matches",
                        "a discovered test bypasses the PostgreSQL resource group"
                    );
                }
            }
        },
    );
}

pub(crate) fn version_gate(workspace: &Path, gates: &mut Checks) {
    let mut command = Command::new("cargo");
    command
        .args(["nextest", "--version"])
        .current_dir(workspace);
    gates.captured(
        None,
        workspace,
        command,
        "repository-pinned cargo-nextest version",
        |output| {
            let pin: serde_json::Value =
                serde_json::from_str(include_str!("../../.config/test-tools.json")).unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert_eq!(
                stdout.split_whitespace().nth(1),
                pin["nextest"]["version"].as_str(),
                "install the pinned runner with python3 -I -S .github/scripts/install-nextest.py target/test-tools and add target/test-tools to PATH"
            );
        },
    );
}

#[cfg(feature = "postgres-check")]
pub(crate) fn complete_group_gate(
    workspace: &Path,
    gates: &mut Checks,
    metadata: &Path,
    binaries: &Path,
    profile: &str,
) {
    scoped_group_gate(
        workspace,
        gates,
        metadata,
        binaries,
        profile,
        "native-resource-groups",
        metadata.with_file_name("resource-groups.json"),
    );
}

#[cfg(feature = "postgres-check")]
pub(crate) fn scoped_group_gate(
    workspace: &Path,
    gates: &mut Checks,
    metadata: &Path,
    binaries: &Path,
    profile: &str,
    id: &str,
    destination: std::path::PathBuf,
) {
    let postgres = if profile == "complete-ci" {
        "postgres-ci"
    } else {
        "postgres"
    };
    let config: toml::Value = std::fs::read_to_string(workspace.join(".config/nextest.toml"))
        .unwrap()
        .parse()
        .unwrap();
    let non_unit = other_weights(&config, profile, 1);
    let non_double = other_weights(&config, profile, 2);
    let filter = format!(
        "(test(/postgres/) & not group({postgres})) | \
         (test(/sqlite/) & not test(/postgres/) & not group(sqlite)) | \
         ((group({postgres}) | group(sqlite)) & ({non_unit})) | \
         (group(wasm) & ({non_double})) | \
         (not group(@global) & not group({postgres}) & not group(sqlite) & not group(wasm))"
    );
    let mut command = Command::new("cargo");
    command
        .args(["nextest", "list", "--cargo-metadata"])
        .arg(metadata)
        .arg("--binaries-metadata")
        .arg(binaries)
        .args([
            "--profile",
            profile,
            "-E",
            &filter,
            "--message-format",
            "json",
        ])
        .current_dir(workspace);
    gates.captured(
        Some(id),
        workspace,
        command,
        "backend cases retain service limits and each custom group has uniform worker weights",
        move |output| {
            let list: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            assert!(list["test-count"].as_u64().is_some_and(|count| count > 0));
            for suite in list["rust-suites"].as_object().unwrap().values() {
                for (name, case) in suite["testcases"].as_object().unwrap() {
                    assert_ne!(
                        case["filter-match"]["status"], "matches",
                        "case {name} bypasses its service group or has an unsafe grouped worker weight"
                    );
                }
            }
            std::fs::write(&destination, serde_json::to_vec(&list).unwrap())
                .expect("persist resolved resource group audit");
        },
    );
}

// future-queue 0.4.0 can strand a group's heavier queued work after its last
// lighter member completes. Uniform weights within each group avoid that
// defect. Let nextest resolve these filtersets against the actual manifest;
// preserve its first-setting precedence, including inherited default rules.
#[cfg(feature = "postgres-check")]
fn other_weights(config: &toml::Value, profile: &str, expected: i64) -> String {
    let mut preceding = vec!["none()".to_owned()];
    let mut different = Vec::new();
    for name in [profile, "default"] {
        let settings = &config["profile"][name];
        assert!(
            settings
                .get("inherits")
                .is_none_or(|value| value.as_str() == Some("default")),
            "weight audit requires the complete profile's default inheritance"
        );
        let Some(overrides) = settings.get("overrides").and_then(toml::Value::as_array) else {
            continue;
        };
        for rule in overrides {
            let Some(weight) = rule.get("threads-required") else {
                continue;
            };
            assert!(
                rule.get("platform").is_none(),
                "audit platform-specific worker weights explicitly"
            );
            let filter = rule
                .get("filter")
                .and_then(toml::Value::as_str)
                .unwrap_or("all()");
            if weight.as_integer() != Some(expected) {
                different.push(format!("(({filter}) & not ({}))", preceding.join(" | ")));
            }
            preceding.push(format!("({filter})"));
        }
    }
    // Nextest's implicit worker reservation is one.
    if expected != 1 {
        different.push(format!("not ({})", preceding.join(" | ")));
    }
    if different.is_empty() {
        "none()".into()
    } else {
        different.join(" | ")
    }
}

pub(crate) fn schedule(workspace: &Path, gates: &mut Checks, arguments: &[&str]) {
    let mut command = Command::new("cargo");
    command
        .args([
            "nextest",
            "run",
            "--profile",
            "default",
            "--retries",
            "0",
            "--no-fail-fast",
        ])
        .args(&arguments[1..])
        .current_dir(workspace);
    crate::standard_tools::require_subprocess_tools(&mut command);
    gates.command(workspace, command, "nextest exit status; retries disabled");
    // Explicit target selectors suppress Cargo's default doctests. Preserve
    // exactly the invocations that previously included them.
    if !arguments
        .iter()
        .any(|arg| matches!(*arg, "--test" | "--lib" | "--bin" | "--example" | "--tests"))
    {
        let mut docs = Command::new("cargo");
        docs.args(arguments).arg("--doc").current_dir(workspace);
        gates.command(workspace, docs, "Cargo doctest exit status");
    }
}

/// Exercise a real failing nextest test through the same aggregate executor.
/// This is an explicit tool qualification, not an ignored workspace test.
pub(crate) fn qualify_failure(workspace: &Path) -> i32 {
    use std::{fs, time::SystemTime};
    let target =
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| workspace.join("target"), Into::into);
    let target = if target.is_absolute() {
        target
    } else {
        workspace.join(target)
    };
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = target.join(format!(
        "test-performance/nextest-failure-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(directory.join("src")).expect("create failing runner fixture");
    fs::write(
        directory.join("Cargo.toml"),
        "[package]\nname='runner-failure-fixture'\nversion='0.0.0'\nedition='2024'\n[workspace]\n",
    )
    .unwrap();
    fs::write(
        directory.join("src/lib.rs"),
        "#[test] fn intentional_failure() { panic!(\"qualification failure\"); }\n#[test] fn positive_control() {}\n",
    )
    .unwrap();
    // This isolated workspace has no production packages or WASM binaries for
    // the repository's package/binary selectors to resolve against.
    let config = directory.join("nextest.toml");
    fs::write(
        &config,
        "[profile.default]\ntest-threads=2\nretries=0\nfail-fast=false\n",
    )
    .unwrap();
    let mut checks = Checks::offline();
    version_gate(workspace, &mut checks);
    let mut failure = Command::new("cargo");
    failure
        .args([
            "nextest",
            "run",
            "--retries",
            "0",
            "--no-fail-fast",
            "--config-file",
        ])
        .arg(config)
        .current_dir(&directory)
        .env("CARGO_TARGET_DIR", directory.join("target"));
    checks.command(&directory, failure, "deliberate real nextest test failure");
    checks.internal("must-be-cancelled", workspace, |_| {
        panic!("executor continued after a failed nextest test");
    });
    let report = directory.join("timings.json");
    let code = checks.execute(&crate::checks::Options {
        list: false,
        timings: Some(report.clone()),
        ..crate::checks::Options::default()
    });
    let recorded: serde_json::Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    // 100 is nextest's test-failure code; build/tool errors must not qualify.
    if code != 100
        || recorded["status"] != "failed"
        || recorded["gates"][1]["exit_code"] != 100
        || recorded["gates"][2]["status"] != "cancelled"
    {
        eprintln!("nextest failure propagation did not qualify");
        return 1;
    }
    println!("real nextest failure propagated as 100; later gate cancelled; report retained");
    0
}
