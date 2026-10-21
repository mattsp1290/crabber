//! One required behavior graph; feature absence remains a narrow build boundary.
use crate::checks::Checks;
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const FEATURES: &str = concat!(
    "xtask/postgres-check,crabber/wasm,crabber/sqlite,crabber/postgres,crabber/custom-http,",
    "crabber/codex,crabber/all-providers,crabber/otlp,crabber-runtime/postgres,",
    "server-host-probe/sqlite,server-host-probe/postgres,server-host-probe/wasm,",
    "workspace-context-probe/sqlite,workspace-context-probe/postgres,",
    "admission-receipt/sqlite,admission-receipt/postgres,host-trace/sqlite,host-trace/postgres,",
    "bounded-snapshot/sqlite,bounded-snapshot/postgres,fenced-abandon/sqlite,fenced-abandon/postgres"
);

pub(crate) fn output_directory() -> PathBuf {
    crate::workspace()
        .join("target/test-performance")
        .join(format!(
            "complete-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
}

fn capture_json(gates: &mut Checks, arguments: &[&str], destination: PathBuf) {
    let workspace = crate::workspace();
    let mut command = Command::new("cargo");
    command.args(arguments).current_dir(workspace);
    gates.captured(
        workspace,
        command,
        "successful JSON build metadata",
        move |output| {
            let value: Value =
                serde_json::from_slice(&output.stdout).expect("valid Cargo/nextest metadata");
            fs::write(&destination, serde_json::to_vec(&value).unwrap())
                .expect("persist build metadata");
        },
    );
}

pub(crate) fn schedule(environment: Vec<(String, String)>, directory: &Path) -> Checks {
    let workspace = crate::workspace();
    let mut gates = Checks::required(environment);
    let fixture_lease = preflight(&mut gates, directory);
    feature_boundaries(&mut gates);
    behavior_graph(&mut gates, directory);
    gates.script(
        workspace,
        "external embedding consumer",
        "testdata/external-consumer/check.sh",
    );
    cli_journeys(&mut gates);
    gates.internal("release-wasm-fixture-output", workspace, move |_| {
        fixture_lease.release();
    });
    gates
}

fn preflight(gates: &mut Checks, directory: &Path) -> crate::lease::Lease {
    let workspace = crate::workspace();
    let create = directory.to_owned();
    gates.internal("prepare-complete-manifests", workspace, move |_| {
        fs::create_dir_all(&create).expect("prepare complete-check manifests");
    });
    gates.cargo(workspace, &["fmt", "--all", "--", "--check"]);
    gates.internal(
        "telemetry-messages",
        workspace,
        crate::telemetry_messages::check,
    );
    gates.internal("agui-pins", workspace, crate::agui::check);
    gates.internal("wit-copies", workspace, crate::check_wit);
    let mut wit = Command::new("wasm-tools");
    wit.args(["component", "wit", "wit", "-o", "/dev/null"])
        .current_dir(workspace);
    gates.command(workspace, wit, "WIT contract validation");
    gates.internal("minimal-embed-glue", workspace, crate::check_glue);
    tool_versions(gates, directory);
    crate::nextest::version_gate(workspace, gates);
    let mut supervisor_tests = Command::new("python3");
    supervisor_tests
        .args(["-I", "-S", ".github/scripts/test_check.py"])
        .current_dir(workspace);
    gates.command(
        workspace,
        supervisor_tests,
        "outer deadline, cancellation and process ownership",
    );
    crate::probes::schedule(workspace, gates);
    crate::schedule_fixtures(workspace, gates)
}

fn feature_boundaries(gates: &mut Checks) {
    let workspace = crate::workspace();
    // Driver-free session/runtime compilation cannot borrow the facade's raw
    // SQLx PostgreSQL dev dependency. Broad execution does not prove absence.
    gates.cargo(
        workspace,
        &[
            "check",
            "-p",
            "crabber-session",
            "-p",
            "crabber-runtime",
            "--features",
            "sqlite",
            "--lib",
        ],
    );
    gates.cargo(
        workspace,
        &[
            "check",
            "-p",
            "crabber",
            "--no-default-features",
            "--features",
            "custom-http",
        ],
    );
    gates.script(
        workspace,
        "custom HTTP feature visibility and raw-escape boundaries",
        "testdata/custom-http-feature-probe/check.sh",
    );
    gates.cargo(
        workspace,
        &[
            "check",
            "-p",
            "crabber-obs",
            "--no-default-features",
            "--all-targets",
        ],
    );
    crate::check_server_host(workspace, gates, false);
    // The standalone external consumer builds and runs the genuine WASM-only
    // API. Combined behavior owns wasm_state/wasm_roles; rebuilding their full
    // test crates in two narrower feature graphs adds no distinct detector.
}

fn behavior_graph(gates: &mut Checks, directory: &Path) {
    let workspace = crate::workspace();
    gates.cargo(
        workspace,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--features",
            FEATURES,
            "--",
            "-D",
            "warnings",
        ],
    );
    // The same features own binaries, tests and docs; there are no per-package
    // behavior reruns merely to enable PostgreSQL or another provider/exporter.
    let cargo_metadata = directory.join("cargo-metadata.json");
    let binaries = directory.join("binaries.json");
    let manifest = directory.join("tests.json");
    capture_json(
        gates,
        &["metadata", "--format-version", "1", "--features", FEATURES],
        cargo_metadata.clone(),
    );
    capture_json(
        gates,
        &[
            "nextest",
            "list",
            "--workspace",
            "--features",
            FEATURES,
            "--list-type",
            "binaries-only",
            "--message-format",
            "json",
        ],
        binaries.clone(),
    );
    // Compile tests first so Cargo can schedule library and test targets
    // together. Building every normal CLI first would serialize those builds
    // unnecessarily when a workspace library changed. CLI-only outputs still
    // use the identical graph and are ready before the retained journeys run.
    gates.cargo(
        workspace,
        &["build", "--workspace", "--bins", "--features", FEATURES],
    );
    let profile = if std::env::var("CI").as_deref() == Ok("true") {
        "complete-ci"
    } else {
        "complete"
    };
    capture_json(
        gates,
        &[
            "nextest",
            "list",
            "--cargo-metadata",
            cargo_metadata.to_str().unwrap(),
            "--binaries-metadata",
            binaries.to_str().unwrap(),
            "--profile",
            profile,
            "--message-format",
            "json",
        ],
        manifest.clone(),
    );
    let inspect_metadata = cargo_metadata.clone();
    let inspect_manifest = manifest.clone();
    gates.internal("required-coverage-and-features", workspace, move |root| {
        validate_manifest(root, &inspect_metadata, &inspect_manifest);
    });
    let mut behavior = Command::new("cargo");
    behavior
        .args([
            "nextest",
            "run",
            "--cargo-metadata",
            cargo_metadata.to_str().unwrap(),
            "--binaries-metadata",
            binaries.to_str().unwrap(),
            "--profile",
            profile,
            "--retries",
            "0",
            "--no-fail-fast",
        ])
        .current_dir(workspace);
    gates.command(
        workspace,
        behavior,
        "all retained native behavior, including required real PostgreSQL",
    );
    gates.cargo(
        workspace,
        &["test", "--workspace", "--features", FEATURES, "--doc"],
    );
}

fn cli_journeys(gates: &mut Checks) {
    let workspace = crate::workspace();
    // These actual executables were built with the combined graph above.
    // Running Cargo again here would resolve a smaller graph and rebuild it.
    for (name, arguments) in [
        ("host-trace", vec!["memory"]),
        ("admission-receipt", vec!["--memory"]),
        ("bounded-snapshot", vec!["--memory", "--check"]),
        ("operational-telemetry", vec!["--check"]),
        ("host-trace", vec!["sqlite"]),
        ("admission-receipt", vec!["--sqlite"]),
        ("bounded-snapshot", vec!["--sqlite", "--check"]),
        ("fenced-abandon", vec!["--sqlite"]),
        ("server-host-probe", vec!["memory"]),
        ("agui-sse", vec!["--check"]),
    ] {
        let mut command = binary_command(name);
        command.args(arguments);
        gates.command(workspace, command, "actual CLI wiring and retained journey");
    }
    let mut exporter = binary_command("datadog-export");
    exporter
        .env("CRABBER_LOG_STDERR", "1")
        .env("RUST_LOG", "info");
    gates.captured(
        workspace,
        exporter,
        "offline exporter diagnostic and redaction",
        crate::check_datadog_offline,
    );
}

fn validate_manifest(workspace: &Path, metadata: &Path, manifest: &Path) {
    let metadata: Value = serde_json::from_slice(&fs::read(metadata).unwrap()).unwrap();
    let runtime = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "crabber-runtime")
        .unwrap();
    let runtime_id = &runtime["id"];
    let resolved = metadata["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| &node["id"] == runtime_id)
        .unwrap();
    assert!(
        resolved["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|feature| feature == "postgres"),
        "the facade's postgres feature does not enable runtime PostgreSQL tests"
    );
    let list: Value = serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
    let suites = list["rust-suites"].as_object().unwrap();
    let mut selected = Vec::new();
    for suite in suites.values() {
        for (name, case) in suite["testcases"].as_object().unwrap() {
            if case["filter-match"]["status"] == "matches" {
                selected.push((suite["package-name"].as_str().unwrap(), name.as_str()));
            }
        }
    }
    assert!(
        !selected.is_empty(),
        "the complete behavior manifest is empty"
    );
    let policy: Value = serde_json::from_slice(
        &fs::read(workspace.join("docs/test-coverage-policy.json")).unwrap(),
    )
    .unwrap();
    for family in policy["changes"].as_array().unwrap() {
        for name in family["retained_tests"].as_array().unwrap() {
            let name = name.as_str().unwrap();
            assert!(
                selected.iter().any(|(_, test)| test.ends_with(name)),
                "required detector {name} not selected"
            );
            if family["id"] == "sql-successor-tool-faults" {
                for backend in ["sqlite", "postgres"] {
                    assert!(
                        selected
                            .iter()
                            .any(|(package, test)| *package == "crabber-session"
                                && test.starts_with(&format!("{backend}::"))
                                && test.ends_with(name)),
                        "required {backend} detector {name} not selected"
                    );
                }
            }
        }
    }
    for behavior in policy["required_behaviors"].as_array().unwrap() {
        for detector in behavior["required_detectors"].as_array().unwrap() {
            let package = detector["package"].as_str().unwrap();
            let suffix = detector["suffix"].as_str().unwrap();
            assert!(
                selected
                    .iter()
                    .any(|(actual_package, test)| *actual_package == package
                        && test.ends_with(suffix)),
                "required behavior {} lacks selected detector {package}::{suffix}",
                behavior["behavior"]
            );
        }
    }
}

pub(crate) fn binary_command(name: &str) -> Command {
    let workspace = crate::workspace();
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace.join("target"), PathBuf::from);
    let target = if target.is_absolute() {
        target
    } else {
        workspace.join(target)
    };
    let mut command = Command::new(target.join("debug").join(name));
    command.current_dir(workspace);
    command
}

fn tool_versions(gates: &mut Checks, directory: &Path) {
    let pins: Value = serde_json::from_str(include_str!("../../.config/test-tools.json")).unwrap();
    for (program, arguments, expected) in [
        (
            "rustc",
            vec!["--version"],
            Some(format!(
                "rustc {}",
                pins["rust"]["version"].as_str().unwrap()
            )),
        ),
        (
            "wasm-tools",
            vec!["--version"],
            Some(format!(
                "wasm-tools {}",
                pins["wasm-tools"]["version"].as_str().unwrap()
            )),
        ),
        (
            "docker",
            vec!["version", "--format", "{{.Server.Version}}"],
            None,
        ),
    ] {
        let mut command = Command::new(program);
        command.args(arguments).current_dir(crate::workspace());
        let path = directory.join(format!("{program}-version.txt"));
        gates.captured(
            crate::workspace(),
            command,
            "pinned compiler/tool and server version",
            move |output| {
                let actual = String::from_utf8_lossy(&output.stdout);
                if let Some(expected) = &expected {
                    let printed = actual
                        .split_whitespace()
                        .take(2)
                        .collect::<Vec<_>>()
                        .join(" ");
                    assert_eq!(&printed, expected, "tool version differs from the pin");
                }
                assert!(!actual.trim().is_empty(), "missing tool version");
                fs::write(&path, actual.as_bytes()).expect("persist tool version");
            },
        );
    }
}
