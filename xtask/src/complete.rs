//! One required behavior graph; feature absence remains a narrow build boundary.
use crate::{
    checks::Checks,
    parts::{Owner, Part},
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

const FEATURES: &str = concat!(
    "xtask/postgres-check,xtask/session-fixtures,crabber/wasm,crabber/sqlite,crabber/postgres,crabber/custom-http,",
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

fn capture_json(gates: &mut Checks, name: Option<&str>, arguments: &[&str], destination: PathBuf) {
    let workspace = crate::workspace();
    let mut command = Command::new("cargo");
    command.args(arguments).current_dir(workspace);
    gates.captured(
        name,
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

fn compile_native_artifacts(gates: &mut Checks, directory: &Path, destination: PathBuf) {
    let workspace = crate::workspace();
    let mut compile = Command::new("cargo");
    compile.current_dir(workspace).args([
        "nextest",
        "list",
        "--workspace",
        "--features",
        FEATURES,
        "--list-type",
        "binaries-only",
        "--message-format",
        "json",
        "--cargo-message-format",
        "json-render-diagnostics",
    ]);
    gates.captured_native(
        workspace,
        compile,
        directory.join("native-artifacts.jsonl"),
        move |output| {
            let value = crate::native_artifacts::summary(&output.stdout);
            fs::write(&destination, serde_json::to_vec(&value).unwrap())
                .expect("persist canonical native build metadata");
        },
    );
    crate::normal_outputs::schedule(gates, directory, FEATURES);
}

pub(crate) fn contracts(mut options: crate::checks::Options) -> i32 {
    if !options.list && !cfg!(target_os = "linux") {
        eprintln!("quality-contracts requires Linux process supervision");
        return 2;
    }
    let directory = output_directory();
    if options.timings.is_none() && !options.list {
        options.timings = Some(directory.join("phases.json"));
    }
    let environment = vec![("CRABBER_REQUIRE_POSTGRES".into(), "1".into())];
    schedule(environment, &directory, None, Some(Part::QualityContracts)).execute(&options)
}

pub(crate) fn schedule(
    environment: Vec<(String, String)>,
    directory: &Path,
    migration: Option<crate::postgres::session::TemplateMigration>,
    part: Option<Part>,
) -> Checks {
    let profile = if std::env::var("CI").as_deref() == Ok("true") {
        "complete-ci"
    } else {
        "complete"
    };
    schedule_profile(environment, directory, migration, part, profile)
}

pub(crate) fn schedule_profile(
    environment: Vec<(String, String)>,
    directory: &Path,
    migration: Option<crate::postgres::session::TemplateMigration>,
    part: Option<Part>,
    profile: &str,
) -> Checks {
    assert!(["complete", "complete-ci"].contains(&profile));
    let workspace = crate::workspace();
    let mut gates = Checks::partitioned(environment, part);
    let fixture_lease = gates.owned(Owner::QualityContracts, |gates| preflight(gates, directory));
    gates.owned(Owner::QualityContracts, |gates| {
        feature_boundaries(gates, directory);
        crate::check_provider_features(workspace, gates);
    });
    behavior_graph(&mut gates, directory, migration, profile);
    gates.owned(Owner::QualityContracts, |gates| {
        gates.script(
            workspace,
            "external embedding consumer",
            "testdata/external-consumer/check.sh",
        );
    });
    gates.owned(Owner::QualityArtifacts, |gates| {
        cli_journeys(gates);
        crate::standard_tools::schedule(workspace, gates);
        gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
            gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
                gates.internal("release-wasm-fixture-output", workspace, move |_| {
                    fixture_lease.release();
                });
            });
        });
    });
    gates.select_part()
}

fn preflight(gates: &mut Checks, directory: &Path) -> crate::lease::Lease {
    let workspace = crate::workspace();
    let create = directory.to_owned();
    gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
        gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
            gates.prepares_for(crate::parts::PartFamily::QualityArtifacts, |gates| {
                gates.internal("prepare-complete-manifests", workspace, move |_| {
                    fs::create_dir_all(&create).expect("prepare complete-check manifests");
                });
            });
        });
    });
    crate::check_workspace(workspace, gates);
    gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
        gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
            gates.prepares_for(crate::parts::PartFamily::QualityArtifacts, |gates| {
                tool_versions(gates, directory);
            });
        });
    });
    let mut supervisor_tests = Command::new("python3");
    supervisor_tests
        .args(["-I", "-S", ".github/scripts/test_check.py"])
        .current_dir(workspace);
    gates.command(
        workspace,
        supervisor_tests,
        "outer deadline, cancellation and process ownership",
    );
    let mut freshness_tests = Command::new("python3");
    freshness_tests
        .args(["-I", "-S", ".github/scripts/test_cache_source_times.py"])
        .current_dir(workspace);
    gates.command(
        workspace,
        freshness_tests,
        "unchanged cache inputs reuse timestamps; changed inputs rebuild",
    );
    let mut pruning_tests = Command::new("python3");
    pruning_tests
        .args(["-I", "-S", ".github/scripts/test_prune_test_cache.py"])
        .current_dir(workspace);
    gates.command(
        workspace,
        pruning_tests,
        "cache maintenance retains current executables and rejects incomplete provenance",
    );
    crate::probes::schedule(workspace, gates);
    gates.owned(Owner::QualityArtifacts, |gates| {
        gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
            gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
                crate::schedule_fixtures(workspace, gates)
            })
        })
    })
}

fn feature_boundaries(gates: &mut Checks, directory: &Path) {
    let workspace = crate::workspace();
    driver_free_libraries(gates, directory.join("boundary-features.json"));
    gates.script(
        workspace,
        "custom HTTP feature visibility and raw-escape boundaries",
        "testdata/custom-http-feature-probe/check.sh",
    );
    // The combined graph compiles the actual CLI's SQLite/PostgreSQL match
    // arms. There is no WASM-specific CLI arm, and the standalone external
    // consumer below owns genuine WASM-only embedding. Rechecking those CLI
    // sources in three narrow graphs adds no additional visibility assertion.
    gates.script(
        workspace,
        "locked server-host consumer",
        "testdata/server-host-consumer/check.sh",
    );
}

fn driver_free_libraries(gates: &mut Checks, destination: PathBuf) {
    let workspace = crate::workspace();
    // Driver-free session/runtime compilation cannot borrow the facade's raw
    // SQLx PostgreSQL dev dependency. Broad execution does not prove absence.
    let mut command = Command::new("cargo");
    command
        .args([
            "check",
            "-p",
            "crabber-session",
            "-p",
            "crabber-runtime",
            "-p",
            "crabber-obs",
            "--no-default-features",
            "--features",
            "crabber-session/sqlite,crabber-runtime/sqlite",
            "--lib",
            "--message-format=json",
        ])
        .current_dir(workspace);
    gates.captured(
        None,
        workspace,
        command,
        "SQLite libraries without a PostgreSQL driver; observations without OTLP",
        move |output| {
            let mut observed = std::collections::BTreeMap::new();
            for message in serde_json::Deserializer::from_slice(&output.stdout).into_iter::<Value>()
            {
                let message = message.expect("Cargo JSON artifact message");
                if message["reason"] == "compiler-artifact"
                    && let Some(name) = message["target"]["name"].as_str()
                    && ["sqlx", "crabber_session", "crabber_runtime", "crabber_obs"].contains(&name)
                {
                    observed.insert(name.to_owned(), message["features"].clone());
                }
            }
            for name in ["sqlx", "crabber_session", "crabber_runtime", "crabber_obs"] {
                let features = observed
                    .get(name)
                    .expect("required production library compiled");
                let features = features.as_array().unwrap();
                assert!(!features.iter().any(|f| f == "postgres" || f == "otlp"));
                if name != "crabber_obs" {
                    assert!(features.iter().any(|f| f == "sqlite"));
                }
            }
            fs::write(&destination, serde_json::to_vec_pretty(&observed).unwrap())
                .expect("persist negative feature boundary evidence");
        },
    );
}

fn behavior_graph(
    gates: &mut Checks,
    directory: &Path,
    migration: Option<crate::postgres::session::TemplateMigration>,
    profile: &str,
) {
    let workspace = crate::workspace();
    gates.owned(Owner::QualityArtifacts, |gates| {
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
    });
    // The same features own binaries, tests and docs; there are no per-package
    // behavior reruns merely to enable PostgreSQL or another provider/exporter.
    gates.owned(Owner::Native, |gates| {
        let cargo_metadata = crate::native_files::File::Metadata.path(directory);
        let binaries = directory.join("binaries.json");
        let manifest = directory.join("tests.json");
        gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
            gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
                gates.prepares_for(crate::parts::PartFamily::QualityArtifacts, |gates| {
                    capture_json(
                        gates,
                        None,
                        &["metadata", "--format-version", "1", "--features", FEATURES],
                        cargo_metadata.clone(),
                    );
                });
            });
        });
        compile_native_artifacts(gates, directory, binaries.clone());
        crate::native_libraries::compile(gates, directory, FEATURES);
        crate::native_libraries::migration_owner(gates, directory);
        crate::native_remaining::prepare(gates, directory, FEATURES);
        gates.prepares_for(crate::parts::PartFamily::NativeRemaining, |gates| {
            gates.prepares_for(crate::parts::PartFamily::NativeLibraries, |gates| {
                gates.prepares_for(crate::parts::PartFamily::QualityArtifacts, |gates| {
                    crate::postgres::session::TemplateMigration::schedule(gates, migration);
                });
            });
        });
        capture_json(
            gates,
            Some("native-test-manifest"),
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
        crate::nextest::complete_group_gate(workspace, gates, &cargo_metadata, &binaries, profile);
        run_native_behaviors(gates, workspace, &cargo_metadata, &binaries, profile);
        crate::native_libraries::execute(gates, directory, profile);
        crate::native_remaining::execute(gates, directory, profile);
    });
    gates.owned(Owner::QualityArtifacts, |gates| {
        gates.cargo(
            workspace,
            &["test", "--workspace", "--features", FEATURES, "--doc"],
        );
    });
}

fn run_native_behaviors(
    gates: &mut Checks,
    workspace: &Path,
    cargo_metadata: &Path,
    binaries: &Path,
    profile: &str,
) {
    let role_test = crate::nextest::CLUSTER_ROLE_TEST;
    for (id, filter, expected) in [
        (
            "native-cluster-role",
            format!("test(={role_test})"),
            "isolated cluster-role migration; no other native histories run",
        ),
        (
            "native-complement",
            format!("not test(={role_test})"),
            "all remaining native behavior, including required real PostgreSQL",
        ),
    ] {
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
                "--no-tests",
                "fail",
            ])
            .args(["-E", &filter])
            .current_dir(workspace);
        crate::standard_tools::require_subprocess_tools(&mut behavior);
        gates.named_command(id, workspace, behavior, expected);
    }
}

fn cli_journeys(gates: &mut Checks) {
    let workspace = crate::workspace();
    // These actual executables were built with the combined graph above.
    // Running Cargo again here would resolve a smaller graph and rebuild it.
    for (name, arguments) in [
        ("host-trace", vec!["memory"]),
        ("admission-receipt", vec!["--memory"]),
        ("bounded-snapshot", vec!["--memory", "--check", "--smoke"]),
        ("operational-telemetry", vec!["--check"]),
        ("host-trace", vec!["sqlite"]),
        ("admission-receipt", vec!["--sqlite"]),
        ("bounded-snapshot", vec!["--sqlite", "--check", "--smoke"]),
        ("fenced-abandon", vec!["--sqlite"]),
        ("server-host-probe", vec!["memory", "--smoke"]),
        ("agui-sse", vec!["--check"]),
        ("mount-standard", vec!["--check"]),
        ("examples/protected_workspace", vec![]),
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
        None,
        workspace,
        exporter,
        "offline exporter diagnostic and redaction",
        crate::check_datadog_offline,
    );
}

fn validate_manifest(workspace: &Path, metadata: &Path, manifest: &Path) {
    let metadata: Value = serde_json::from_slice(&fs::read(metadata).unwrap()).unwrap();
    let list: Value = serde_json::from_slice(&fs::read(manifest).unwrap()).unwrap();
    let policy: Value = serde_json::from_slice(
        &fs::read(workspace.join("docs/test-coverage-policy.json")).unwrap(),
    )
    .unwrap();
    crate::native_coverage::validate(&metadata, &list, &policy);
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
        let path = match program {
            "rustc" => crate::native_files::File::RustcVersion,
            "wasm-tools" => crate::native_files::File::WasmToolsVersion,
            "docker" => crate::native_files::File::DockerVersion,
            _ => unreachable!("known version program"),
        }
        .path(directory);
        gates.captured(
            None,
            crate::workspace(),
            command,
            "pinned compiler/tool and server version",
            move |output| {
                let actual = String::from_utf8_lossy(&output.stdout);
                admit_tool_version(&actual, expected.as_deref());
                fs::write(&path, actual.as_bytes()).expect("persist tool version");
            },
        );
    }
}

pub(crate) fn admit_tool_version(actual: &str, expected: Option<&str>) {
    if let Some(expected) = expected {
        let printed = actual
            .split_whitespace()
            .take(2)
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(printed, expected, "tool version differs from the pin");
    }
    assert!(!actual.trim().is_empty(), "missing tool version");
}
