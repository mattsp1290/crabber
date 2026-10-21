//! Local development commands, invoked with `cargo xtask`.

use sha2::Digest;
use std::{env, fmt::Write, fs, path::Path, process::Command};

mod agui;
mod checks;
mod datadog;
mod probes;
mod process;
mod telemetry_messages;
mod timings;

const GLUE_START: &str = "crabber:glue-start";
const GLUE_END: &str = "crabber:glue-end";
const GLUE_LIMIT: usize = 60;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("check") => {
            let options = checks::Options::parse(&args[1..]).unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(2);
            });
            std::process::exit(check().execute(&options));
        }
        Some("build-fixtures") if args.len() == 1 => build_fixtures(),
        Some("verify-datadog") if args.len() == 1 => datadog::verify(),
        Some("check-probes") if args.len() == 1 => probes::check(workspace()),
        _ => {
            eprintln!(
                "usage: cargo xtask check [--list] [--timings <path>] | build-fixtures | verify-datadog | check-probes"
            );
            std::process::exit(2);
        }
    }
}

fn workspace() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn check() -> checks::Checks {
    let mut gates = checks::Checks::default();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask belongs directly under the workspace root");

    gates.internal("telemetry-messages", workspace, telemetry_messages::check);
    gates.internal("agui-pins", workspace, agui::check);
    probes::schedule(workspace, &mut gates);
    gates.internal("wit-copies", workspace, check_wit);
    let mut wit = Command::new("wasm-tools");
    wit.args(["component", "wit", "wit", "-o", "/dev/null"])
        .current_dir(workspace);
    gates.command(workspace, wit, "WIT contract validation");
    schedule_fixtures(workspace, &mut gates);
    gates.cargo(workspace, &["fmt", "--all", "--", "--check"]);
    check_server_host(workspace, &mut gates);
    check_custom_http(workspace, &mut gates);
    gates.cargo(
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
    check_sqlite(workspace, &mut gates);
    gates.cargo(
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
    gates.cargo(workspace, &["test", "--workspace"]);
    gates.cargo(
        workspace,
        &["run", "--quiet", "-p", "agui-sse", "--", "--check"],
    );
    gates.cargo(
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
    check_observability(workspace, &mut gates);
    check_journeys(workspace, &mut gates);
    gates.cargo(
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
    gates.cargo(
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
    gates.script(
        workspace,
        "external consumer check",
        "testdata/external-consumer/check.sh",
    );
    gates.internal("minimal-embed-glue", workspace, check_glue);
    gates
}

// This local adoption probe deliberately needs no network provider or AG-UI SDK.
fn check_server_host(workspace: &Path, gates: &mut checks::Checks) {
    // check() builds the actual local component fixtures before this gate.
    gates.cargo(
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
    gates.cargo(
        workspace,
        &["test", "-p", "server-host-probe", "--features", "sqlite"],
    );
    gates.cargo(
        workspace,
        &["run", "--quiet", "-p", "server-host-probe", "--", "memory"],
    );
    gates.cargo(
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
    gates.script(
        workspace,
        "locked server-host consumer",
        "testdata/server-host-consumer/check.sh",
    );
}

fn check_sqlite(workspace: &Path, gates: &mut checks::Checks) {
    gates.cargo(
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
    gates.cargo(
        workspace,
        &["test", "-p", "crabber-session", "--features", "sqlite"],
    );

    for package in ["crabber", "crabber-runtime"] {
        gates.cargo(
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
    gates.cargo(
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
    gates.cargo(
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
    gates.cargo(
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
        gates.cargo(workspace, &["test", "-p", package, "--features", "sqlite"]);
    }
    gates.cargo(
        workspace,
        &[
            "test",
            "-p",
            "minimal-embed",
            "sqlite_interrupt_then_fresh_process_continues_session",
        ],
    );
}

fn check_custom_http(workspace: &Path, gates: &mut checks::Checks) {
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
    // This script includes the positive probe manifest check before exercising
    // the negative feature-boundary and raw-escape probes.
    gates.script(
        workspace,
        "custom HTTP positive and negative feature-boundary probe",
        "testdata/custom-http-feature-probe/check.sh",
    );
    gates.cargo(
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
    gates.cargo(
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
    gates.cargo(
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

fn check_journeys(workspace: &Path, gates: &mut checks::Checks) {
    gates.cargo(
        workspace,
        &["run", "--quiet", "-p", "host-trace", "--", "memory"],
    );
    gates.cargo(
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
    gates.cargo(
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
    gates.cargo(
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
        gates.cargo(workspace, &arguments);
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
}

fn build_fixtures() {
    let mut gates = checks::Checks::default();
    schedule_fixtures(workspace(), &mut gates);
    let code = gates.execute(&checks::Options::default());
    if code != 0 {
        std::process::exit(code);
    }
}

fn schedule_fixtures(workspace: &Path, gates: &mut checks::Checks) {
    let fixtures = workspace.join("fixtures/wasm");
    let target = fixtures.join("target");
    let arguments = [
        "build",
        "--workspace",
        "--target",
        "wasm32-wasip2",
        "--release",
    ];
    let mut positive = Command::new("cargo");
    positive
        .args(arguments)
        .env("CARGO_TARGET_DIR", &target)
        .current_dir(&fixtures);
    gates.command(&fixtures, positive, "positive WASM fixtures compile");
    gates.internal("wasm-fixture-aliases", workspace, copy_fixture_aliases);
    let negative_root = workspace.join("fixtures/wasm-negative");
    let mut negative = Command::new("cargo");
    negative
        .args(arguments)
        .env("CARGO_TARGET_DIR", target)
        .current_dir(&negative_root);
    gates.command(&negative_root, negative, "negative WASM fixtures compile");
}

fn copy_fixture_aliases(workspace: &Path) {
    let fixtures = workspace.join("fixtures/wasm");
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
fn check_datadog_offline(output: &std::process::Output) {
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

fn check_observability(workspace: &Path, gates: &mut checks::Checks) {
    gates.cargo(
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
    gates.cargo(
        workspace,
        &["test", "-p", "crabber-obs", "--features", "otlp"],
    );
    gates.cargo(
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
    gates.cargo(
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
    gates.cargo(
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
    let mut command = Command::new("cargo");
    command
        .args(["run", "--quiet", "-p", "datadog-export"])
        .current_dir(workspace)
        .env_remove("DD_API_KEY")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .env("CRABBER_LOG_STDERR", "1")
        .env("RUST_LOG", "info");
    gates.captured(
        workspace,
        command,
        "Completed; retry diagnostic on stderr; no fixture secrets",
        check_datadog_offline,
    );
}
