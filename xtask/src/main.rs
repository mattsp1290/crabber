//! Local development commands, invoked with `cargo xtask`.

use sha2::Digest;
use std::{env, fmt::Write, fs, path::Path, process::Command};

mod agui;
#[cfg(target_os = "linux")]
mod budget;
mod checks;
#[cfg(feature = "postgres-check")]
mod complete;
mod datadog;
mod inventory;
mod lease;
mod native_artifacts;
mod nextest;
mod parts;
#[cfg(feature = "postgres-check")]
mod postgres;
mod probes;
mod process;
mod standard_tools;
mod telemetry_messages;
mod timings;

const GLUE_START: &str = "crabber:glue-start";
const GLUE_END: &str = "crabber:glue-end";
const GLUE_LIMIT: usize = 60;

fn main() {
    let root = workspace().canonicalize();
    let invocation = env::current_dir().and_then(fs::canonicalize);
    if !matches!((root, invocation), (Ok(root), Ok(invocation)) if invocation.starts_with(&root)
        && !invocation
            .ancestors()
            .take_while(|path| *path != root.as_path())
            .any(|path| path.join(".git").exists()))
    {
        eprintln!(
            "xtask belongs to a different workspace; run `cargo clean -p xtask` in this checkout and retry"
        );
        std::process::exit(2);
    }
    let args: Vec<String> = env::args().skip(1).collect();
    // A partial fixture command replaces this process before any owner,
    // signal handler or budget thread exists. The enabled binary owns cleanup.
    #[cfg(all(
        feature = "postgres-check",
        not(feature = "session-fixtures"),
        target_os = "linux"
    ))]
    if let Some(code) = postgres::session::delegate(&args) {
        std::process::exit(code);
    }
    #[cfg(target_os = "linux")]
    if !(args.first().map(String::as_str) == Some("check")
        && args.iter().skip(1).any(|arg| arg == "--list"))
        && let Err(error) = process::enable_tree_supervision()
    {
        eprintln!("cannot enable Linux child supervision: {error}");
        std::process::exit(1);
    }
    #[cfg(target_os = "linux")]
    if let Err(error) = process::cancellation().and_then(budget::watch) {
        eprintln!("cannot supervise complete-check budget: {error}");
        std::process::exit(1);
    }
    match args.first().map(String::as_str) {
        Some("check") => {
            let options = checks::Options::parse(&args[1..]).unwrap_or_else(|error| {
                eprintln!("{error}");
                std::process::exit(2);
            });
            #[cfg(feature = "postgres-check")]
            if options.part.is_some() {
                std::process::exit(complete::contracts(options));
            }
            if options.offline {
                std::process::exit(check().execute(&options));
            }
            #[cfg(feature = "postgres-check")]
            std::process::exit(postgres::complete(options));
            #[cfg(not(feature = "postgres-check"))]
            {
                eprintln!("the complete check requires postgres-check; use cargo xtask check");
                std::process::exit(2);
            }
        }
        #[cfg(feature = "session-fixtures")]
        Some("postgres-template-migrate") if args.len() == 1 => {
            std::process::exit(postgres::session::migrate())
        }
        #[cfg(feature = "postgres-check")]
        Some("postgres-fixture-helper") => {
            std::process::exit(postgres::child::main(args.get(1).map(String::as_str)))
        }
        #[cfg(feature = "postgres-check")]
        Some("check-postgres") => std::process::exit(postgres::check(&args[1..])),
        #[cfg(not(feature = "postgres-check"))]
        Some("check-postgres") => {
            eprintln!("use cargo run -p xtask --features postgres-check -- check-postgres");
            std::process::exit(2);
        }
        Some("inventory-tests") if args.len() == 1 => println!(
            "{}",
            serde_json::to_string_pretty(&inventory::tests(workspace())).unwrap()
        ),
        Some("build-fixtures") if args.len() == 1 => build_fixtures(),
        Some("verify-datadog") if args.len() == 1 => datadog::verify(),
        Some("check-probes") if args.len() == 1 => probes::check(workspace()),
        Some("qualify-nextest") if args.len() == 1 => {
            std::process::exit(nextest::qualify_failure(workspace()));
        }
        _ => {
            eprintln!(
                "usage: cargo xtask check [--list] [--part quality-contracts] [--timings <path>] | build-fixtures | verify-datadog | check-probes | inventory-tests"
            );
            std::process::exit(2);
        }
    }
}

fn workspace() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn check_workspace(workspace: &Path, gates: &mut checks::Checks) {
    gates.cargo(workspace, &["fmt", "--all", "--", "--check"]);
    gates.internal("standard-tool-prerequisites", workspace, |_| {
        standard_tools::check_tool_prerequisites();
    });
    gates.internal(
        "live-tests-ignored",
        workspace,
        check_live_tests_are_ignored,
    );
    gates.internal("telemetry-messages", workspace, telemetry_messages::check);
    gates.internal("agui-pins", workspace, agui::check);
    gates.internal("wit-copies", workspace, check_wit);
    let mut wit = Command::new("wasm-tools");
    wit.args(["component", "wit", "wit", "-o", "/dev/null"])
        .current_dir(workspace);
    gates.command(workspace, wit, "WIT contract validation");
    gates.internal("minimal-embed-glue", workspace, check_glue);
    nextest::version_gate(workspace, gates);
}

fn check() -> checks::Checks {
    let mut gates = checks::Checks::offline();
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask belongs directly under the workspace root");

    check_workspace(workspace, &mut gates);
    probes::schedule(workspace, &mut gates);
    let fixture_lease = schedule_fixtures(workspace, &mut gates);
    check_server_host(workspace, &mut gates, true);
    check_custom_http(workspace, &mut gates);
    for (target, filter) in [
        ("facade_contracts", Some("typed_input::")),
        ("typed_output", None),
    ] {
        let mut args = vec![
            "test",
            "-p",
            "crabber",
            "--features",
            "wasm,sqlite,postgres,custom-http",
            "--test",
            target,
        ];
        args.extend(filter);
        gates.cargo(workspace, &args);
    }
    check_sqlite(workspace, &mut gates);
    for features in ["wasm", "wasm,sqlite"] {
        gates.cargo(
            workspace,
            &[
                "check",
                "-p",
                "crabber",
                "--features",
                features,
                "--test",
                "wasm_state",
                "--test",
                "wasm_roles",
            ],
        );
    }
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
    // Workspace owns common behavior, including both explicit probe backends.
    // Minimal feature builds below retain absence/boundary compilation checks.
    gates.cargo(
        workspace,
        &[
            "test",
            "--workspace",
            "--features",
            nextest::WORKSPACE_FEATURES,
        ],
    );
    nextest::group_gate(workspace, &mut gates);
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
    standard_tools::schedule(workspace, &mut gates);
    check_provider_features(workspace, &mut gates);
    check_observability(workspace, &mut gates);
    check_journeys(workspace, &mut gates);
    gates.script(
        workspace,
        "external consumer check",
        "testdata/external-consumer/check.sh",
    );
    gates.internal("release-wasm-fixture-output", workspace, move |_| {
        fixture_lease.release();
    });
    gates
}

// This local adoption probe deliberately needs no network provider or AG-UI SDK.
fn check_server_host(workspace: &Path, gates: &mut checks::Checks, run_cli: bool) {
    for features in ["wasm", "sqlite"] {
        gates.cargo(
            workspace,
            &[
                "check",
                "-p",
                "server-host-probe",
                "--features",
                features,
                "--bin",
                "server-host-probe",
            ],
        );
    }
    gates.cargo(
        workspace,
        &[
            "check",
            "-p",
            "workspace-context-probe",
            "--features",
            "sqlite",
            "--lib",
        ],
    );
    if run_cli {
        gates.cargo(
            workspace,
            &["run", "--quiet", "-p", "server-host-probe", "--", "memory"],
        );
    }
    gates.script(
        workspace,
        "locked server-host consumer",
        "testdata/server-host-consumer/check.sh",
    );
}

fn check_sqlite(workspace: &Path, gates: &mut checks::Checks) {
    // The facade already has a raw PostgreSQL SQLx dev dependency. Keep this
    // driver-free boundary separate from its otherwise compatible Clippy pass.
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
            "--all-targets",
        ],
    );
    gates.cargo(
        workspace,
        &[
            "clippy",
            "-p",
            "crabber-session",
            "-p",
            "crabber-runtime",
            "-p",
            "crabber",
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

fn check_provider_features(workspace: &Path, gates: &mut checks::Checks) {
    for feature in ["anthropic", "openai", "codex", "opencode-go"] {
        gates.cargo(
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
    gates.cargo(
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
}

fn check_live_tests_are_ignored(workspace: &Path) {
    let mut files = Vec::new();
    for krate in fs::read_dir(workspace.join("crates")).expect("failed to read crates") {
        let tests = krate
            .expect("failed to read crate entry")
            .path()
            .join("tests");
        let Ok(entries) = fs::read_dir(&tests) else {
            continue;
        };
        for entry in entries {
            let path = entry.expect("failed to read test entry").path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("_live.rs"))
            {
                files.push(path);
            }
        }
    }
    files.sort();
    assert!(!files.is_empty(), "no live test files found");
    for file in files {
        let source = fs::read_to_string(&file)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", file.display()));
        if let Err(problem) = live_tests_are_ignored(&source) {
            panic!("{}: {problem}", file.display());
        }
    }
}

/// Checks that each test's own attribute block carries `#[ignore`.
fn live_tests_are_ignored(source: &str) -> Result<(), String> {
    let mut attributes: Vec<&str> = Vec::new();
    let mut tests = 0;
    for line in source.lines().map(str::trim) {
        if line.starts_with("#[") {
            attributes.push(line);
            continue;
        }
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        let is_test = attributes
            .iter()
            .any(|attr| attr.starts_with("#[test]") || attr.starts_with("#[tokio::test"));
        if is_test {
            tests += 1;
            if !attributes.iter().any(|attr| attr.starts_with("#[ignore")) {
                return Err(format!("live test without #[ignore] before `{line}`"));
            }
        }
        attributes.clear();
    }
    if tests == 0 {
        return Err("no live tests found".into());
    }
    Ok(())
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
    let lease = schedule_fixtures(workspace(), &mut gates);
    gates.internal("release-wasm-fixture-output", workspace(), move |_| {
        lease.release();
    });
    let code = gates.execute(&checks::Options::default());
    if code != 0 {
        std::process::exit(code);
    }
}

fn schedule_fixtures(workspace: &Path, gates: &mut checks::Checks) -> lease::Lease {
    let fixtures = workspace.join("fixtures/wasm");
    let target = fixtures.join("target");
    let lease = lease::Lease::default();
    let acquire = lease.clone();
    let lock = target.join(".fixture-output.lock");
    gates.internal("acquire-wasm-fixture-output", workspace, move |_| {
        acquire.acquire(&lock);
    });
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
    lease
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

#[cfg(test)]
mod tests {
    use super::live_tests_are_ignored;

    #[test]
    fn live_test_guard_requires_ignore_on_each_test() {
        let ignored = "#[tokio::test]\n#[ignore = \"live\"]\nasync fn a() {}\n#[ignore]\n#[test]\nfn b() {}\n";
        assert!(live_tests_are_ignored(ignored).is_ok());
        for source in [
            "#[tokio::test]\n#[ignore]\nasync fn a() {}\n#[tokio::test]\nasync fn b() {}\n",
            "#[tokio::test]\n// #[ignore]\nasync fn a() {}\n",
            "#[test]\nfn a() {}\n#[ignore]\nfn helper() {}\n",
            "fn helper() {}\n",
        ] {
            assert!(live_tests_are_ignored(source).is_err(), "{source}");
        }
    }
}
