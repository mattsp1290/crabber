//! Local development commands, invoked with `cargo xtask`.

use sha2::Digest;
use std::{env, fmt::Write, fs, path::Path, process::Command};

mod agui;
mod datadog;
mod probes;
mod telemetry_messages;

const GLUE_START: &str = "crabber:glue-start";
const GLUE_END: &str = "crabber:glue-end";
const GLUE_LIMIT: usize = 60;

fn main() {
    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (Some("check"), None) => check(),
        (Some("build-fixtures"), None) => build_fixtures(),
        (Some("verify-datadog"), None) => datadog::verify(),
        (Some("check-probes"), None) => probes::check(workspace()),
        _ => {
            eprintln!("usage: cargo xtask <check|build-fixtures|verify-datadog|check-probes>");
            std::process::exit(2);
        }
    }
}

fn workspace() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn check() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask belongs directly under the workspace root");

    telemetry_messages::check(workspace);
    agui::check(workspace);
    probes::check(workspace);
    check_wit(workspace);
    build_fixtures();
    run(workspace, &["fmt", "--all", "--", "--check"]);
    check_server_host(workspace);
    check_custom_http(workspace);
    check_provider_features(workspace);
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "wasm,sqlite,postgres,custom-http",
            "--test",
            "typed_input",
            "--test",
            "typed_output",
        ],
    );
    check_sqlite(workspace);
    run(
        workspace,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(workspace, &["test", "--workspace"]);
    run(
        workspace,
        &["run", "--quiet", "-p", "agui-sse", "--", "--check"],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "opencode-go",
            "--test",
            "opencode_go_chat",
        ],
    );
    check_anthropic(workspace);
    check_observability(workspace);
    check_journeys(workspace);
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "wasm",
            "--test",
            "wasm_state",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "wasm",
            "--test",
            "wasm_roles",
        ],
    );
    run_script(
        workspace,
        "external consumer check",
        "testdata/external-consumer/check.sh",
    );
    check_glue(workspace);
}

// This local adoption probe deliberately needs no network provider or AG-UI SDK.
fn check_server_host(workspace: &Path) {
    // check() builds the actual local component fixtures before this gate.
    run(
        workspace,
        &[
            "test",
            "-p",
            "server-host-probe",
            "--features",
            "wasm",
            "--test",
            "shared_wasm",
        ],
    );
    run(
        workspace,
        &["test", "-p", "server-host-probe", "--features", "sqlite"],
    );
    run(
        workspace,
        &["run", "--quiet", "-p", "server-host-probe", "--", "memory"],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "sqlite",
            "--test",
            "server_host",
            "--test",
            "durable_interactions",
            "--test",
            "durable_controls",
            "--test",
            "session_management",
        ],
    );
    run_script(
        workspace,
        "locked server-host consumer",
        "testdata/server-host-consumer/check.sh",
    );
}

fn check_sqlite(workspace: &Path) {
    run(
        workspace,
        &[
            "clippy",
            "-p",
            "crabber-session",
            "--features",
            "sqlite",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(
        workspace,
        &["test", "-p", "crabber-session", "--features", "sqlite"],
    );

    for package in ["crabber", "crabber-runtime"] {
        run(
            workspace,
            &[
                "clippy",
                "-p",
                package,
                "--features",
                "sqlite",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ],
        );
    }
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "sqlite",
            "--test",
            "admission_receipts",
            "--test",
            "bounded_snapshots",
            "--test",
            "fenced_abandon",
            "--test",
            "result_transform_interrupt",
            "--test",
            "unknown_tool_depth",
            "--test",
            "sqlite_builder",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "datadog,sqlite",
            "--test",
            "trace_context",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "wasm,sqlite",
            "--test",
            "wasm_roles",
        ],
    );
    for package in ["crabber-runtime", "workspace-context-probe"] {
        run(workspace, &["test", "-p", package, "--features", "sqlite"]);
    }
    run(
        workspace,
        &[
            "test",
            "-p",
            "minimal-embed",
            "sqlite_interrupt_then_fresh_process_continues_session",
        ],
    );
}

fn check_custom_http(workspace: &Path) {
    run(
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
    // This script includes the positive probe manifest check before exercising
    // the negative feature-boundary and raw-escape probes.
    run_script(
        workspace,
        "custom HTTP positive and negative feature-boundary probe",
        "testdata/custom-http-feature-probe/check.sh",
    );
    run(
        workspace,
        &[
            "clippy",
            "-p",
            "crabber-providers",
            "--all-targets",
            "--features",
            "all-providers,custom-http",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber-providers",
            "--no-default-features",
            "--features",
            "custom-http",
            "--test",
            "custom_http",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber-providers",
            "--features",
            "all-providers,custom-http",
        ],
    );
}

// Each provider feature alone must compile cleanly, so cfg-gated items stay used.
fn check_provider_features(workspace: &Path) {
    for feature in ["anthropic", "openai", "codex", "opencode-go"] {
        run(
            workspace,
            &[
                "clippy",
                "-p",
                "crabber-providers",
                "--all-targets",
                "--no-default-features",
                "--features",
                feature,
                "--",
                "-D",
                "warnings",
            ],
        );
    }
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber-providers",
            "--no-default-features",
            "--features",
            "anthropic",
        ],
    );
}

// The live suite is only compiled here; it runs solely on an operator's
// explicit request, so this gate never needs network access or credentials.
fn check_anthropic(workspace: &Path) {
    run(
        workspace,
        &[
            "clippy",
            "-p",
            "crabber",
            "--all-targets",
            "--features",
            "anthropic",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "anthropic",
            "--test",
            "anthropic_messages",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "anthropic",
            "--test",
            "anthropic_live",
            "--no-run",
        ],
    );
    check_live_tests_are_ignored(workspace);
}

fn check_live_tests_are_ignored(workspace: &Path) {
    for relative in [
        "crates/crabber/tests/anthropic_live.rs",
        "crates/crabber/tests/opencode_go_live.rs",
    ] {
        let source = fs::read_to_string(workspace.join(relative))
            .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"));
        let tests = source.matches("#[tokio::test").count();
        let ignored = source.matches("#[ignore").count();
        assert!(
            tests > 0 && tests == ignored,
            "{relative}: every live test must be #[ignore] ({tests} tests, {ignored} ignored)"
        );
    }
}

fn check_journeys(workspace: &Path) {
    run(
        workspace,
        &["run", "--quiet", "-p", "host-trace", "--", "memory"],
    );
    run(
        workspace,
        &[
            "run",
            "--quiet",
            "-p",
            "admission-receipt",
            "--",
            "--memory",
        ],
    );
    run(
        workspace,
        &[
            "run",
            "--quiet",
            "-p",
            "bounded-snapshot",
            "--",
            "--memory",
            "--check",
        ],
    );
    run(
        workspace,
        &[
            "run",
            "--quiet",
            "-p",
            "operational-telemetry",
            "--",
            "--check",
        ],
    );
    for (package, argument) in [
        ("host-trace", "sqlite"),
        ("admission-receipt", "--sqlite"),
        ("bounded-snapshot", "--sqlite"),
        ("fenced-abandon", "--sqlite"),
    ] {
        let mut arguments = vec![
            "run",
            "--quiet",
            "-p",
            package,
            "--features",
            "sqlite",
            "--",
            argument,
        ];
        if package == "bounded-snapshot" {
            arguments.push("--check");
        }
        run(workspace, &arguments);
    }
}

fn check_wit(workspace: &Path) {
    for relative in [
        "crabber-extensions.wit",
        "deps/crabber-host/log.wit",
        "deps/crabber-host/state.wit",
    ] {
        let contract = fs::read(workspace.join("wit").join(relative)).expect("root WIT");
        for copy in [
            "crates/crabber-guest/wit",
            "crates/crabber-guest-macros/wit",
        ] {
            assert_eq!(
                contract,
                fs::read(workspace.join(copy).join(relative)).expect("SDK WIT"),
                "WIT copy {copy}/{relative} differs"
            );
        }
    }
    let status = Command::new("wasm-tools")
        .args(["component", "wit", "wit", "-o", "/dev/null"])
        .current_dir(workspace)
        .status()
        .expect("install wasm-tools to validate the WIT contract");
    assert!(status.success(), "WIT contract is invalid");
}

fn build_fixtures() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let fixtures = workspace.join("fixtures/wasm");
    let target = fixtures.join("target");
    let status = Command::new("cargo")
        .args([
            "build",
            "--workspace",
            "--target",
            "wasm32-wasip2",
            "--release",
        ])
        .env("CARGO_TARGET_DIR", &target)
        .current_dir(&fixtures)
        .status()
        .expect("install the wasm32-wasip2 Rust target");
    assert!(status.success(), "fixture build failed");
    let output = fixtures.join("target/wasm32-wasip2/release");
    let mut manifest = String::new();
    for entry in fs::read_dir(&output).expect("fixture output") {
        let path = entry.expect("fixture entry").path();
        let is_positive = path.file_stem().is_some_and(|stem| {
            fixtures
                .join("src")
                .join(stem.to_string_lossy().replace('_', "-"))
                .is_dir()
        });
        if path.extension().is_some_and(|ext| ext == "wasm") && is_positive {
            let alias = fixtures.join(format!(
                "{}.wasm",
                path.file_stem()
                    .unwrap()
                    .to_string_lossy()
                    .replace('_', "-")
            ));
            fs::copy(&path, alias).expect("copy ignored fixture alias");
            let digest = sha2::Sha256::digest(fs::read(&path).expect("fixture bytes"));
            writeln!(
                &mut manifest,
                "{} {digest:x}",
                path.file_name().unwrap().to_string_lossy()
            )
            .unwrap();
        }
    }
    fs::write(fixtures.join("manifest.sha256"), manifest).expect("fixture manifest");
    let status = Command::new("cargo")
        .args([
            "build",
            "--workspace",
            "--target",
            "wasm32-wasip2",
            "--release",
        ])
        .env("CARGO_TARGET_DIR", target)
        .current_dir(workspace.join("fixtures/wasm-negative"))
        .status()
        .expect("build negative fixtures");
    assert!(status.success(), "negative fixture build failed");
}

fn run(workspace: &Path, args: &[&str]) {
    println!("$ cargo {}", args.join(" "));
    let status = Command::new("cargo")
        .args(args)
        .current_dir(workspace)
        .status()
        .expect("failed to launch cargo");
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn run_script(workspace: &Path, label: &str, relative: &str) {
    println!("$ {relative} # {label}");
    let script = workspace.join(relative);
    let status = Command::new(&script)
        .current_dir(workspace)
        .status()
        .unwrap_or_else(|error| panic!("failed to launch {label} ({relative}): {error}"));
    if !status.success() {
        eprintln!("{label} failed ({relative})");
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn check_glue(workspace: &Path) {
    let example_dir = workspace.join("examples/minimal-embed");
    let example_file = workspace.join("examples/minimal-embed.rs");
    if !example_dir.exists() && !example_file.exists() {
        return;
    }

    let mut files = Vec::new();
    if example_file.exists() {
        files.push(example_file);
    }
    if example_dir.exists() {
        collect_rust_files(&example_dir, &mut files);
    }

    let mut regions = 0;
    for file in files {
        let source = fs::read_to_string(&file).expect("failed to read minimal embed example");
        let mut start = None;
        for (index, line) in source.lines().enumerate() {
            if line.contains(GLUE_START) {
                assert!(
                    start.replace(index).is_none(),
                    "duplicate glue start in {}",
                    file.display()
                );
            }
            if line.contains(GLUE_END) {
                let first = start
                    .take()
                    .unwrap_or_else(|| panic!("glue end without start in {}", file.display()));
                let count = index - first - 1;
                assert!(
                    count <= GLUE_LIMIT,
                    "{} has {count} glue lines (limit {GLUE_LIMIT})",
                    file.display()
                );
                regions += 1;
            }
        }
        assert!(
            start.is_none(),
            "glue start without end in {}",
            file.display()
        );
    }
    assert_eq!(
        regions, 1,
        "minimal embed example must have exactly one glue marker pair"
    );
}

fn collect_rust_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in fs::read_dir(dir).expect("failed to read minimal embed example directory") {
        let path = entry.expect("failed to read example entry").path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
}

// Exercise both the unconfigured export path and the host stderr recipe.
fn check_datadog_offline(workspace: &Path) {
    let output = Command::new("cargo")
        .args(["run", "--quiet", "-p", "datadog-export"])
        .current_dir(workspace)
        .env_remove("DD_API_KEY")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .env("CRABBER_LOG_STDERR", "1")
        .env("RUST_LOG", "info")
        .output()
        .expect("run offline Datadog fixture");
    assert!(output.status.success(), "offline Datadog fixture failed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("status=Completed"));
    assert!(
        stderr.contains("model request failed, retry scheduled"),
        "host stderr layer did not observe the retry diagnostic"
    );
    for sentinel in [
        "SECRET response credential header",
        "PROMPT_SECRET",
        "TOOL_SECRET",
    ] {
        assert!(
            !stdout.contains(sentinel) && !stderr.contains(sentinel),
            "offline telemetry exposed fixture content"
        );
    }
    println!("offline Datadog fixture completed with redacted stderr");
}

fn check_observability(workspace: &Path) {
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "datadog",
            "--test",
            "trace_context",
        ],
    );
    run(
        workspace,
        &["test", "-p", "crabber-obs", "--features", "otlp"],
    );
    run(
        workspace,
        &[
            "clippy",
            "-p",
            "crabber-obs",
            "--features",
            "otlp",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "otlp",
            "--test",
            "trace_context",
        ],
    );
    run(
        workspace,
        &[
            "test",
            "-p",
            "crabber",
            "--features",
            "otlp,sqlite",
            "--test",
            "telemetry",
        ],
    );
    check_datadog_offline(workspace);
}
