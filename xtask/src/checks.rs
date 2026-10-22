//! One ordered inventory drives execution and `check --list`.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::Digest;

use crate::timings::Report;

/// Keep provider/exporter configuration out of both credential-free test lanes.
/// Scoped PostgreSQL fixture handoffs and explicit stderr recipes remain intact.
pub(crate) fn remove_live_configuration(command: &mut Command) {
    for name in [
        "CRABBER_TRACE_LIVE",
        "CRABBER_OBS_VERIFY_MARKER",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
        "OPENCODE_GO_API_KEY",
        "DD_API_KEY",
        "DD_APP_KEY",
        "DD_SERVICE",
        "DD_ENV",
        "DD_VERSION",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
    ] {
        command.env_remove(name);
    }
}

#[derive(Clone, Default)]
pub(crate) struct Options {
    pub(crate) list: bool,
    pub(crate) offline: bool,
    pub(crate) timings: Option<PathBuf>,
}

impl Options {
    pub(crate) fn parse(args: &[String]) -> Result<Self, String> {
        let mut options = Self::default();
        let mut args = args.iter();
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--list" if !options.list => options.list = true,
                "--lane" if !options.offline => {
                    if args.next().map(String::as_str) != Some("offline") {
                        return Err(
                            "--lane accepts offline; omit it for the complete required check"
                                .into(),
                        );
                    }
                    options.offline = true;
                }
                "--timings" if options.timings.is_none() => {
                    options.timings =
                        Some(PathBuf::from(args.next().ok_or("--timings needs a path")?));
                }
                _ => return Err(format!("unexpected check argument: {argument}")),
            }
        }
        Ok(options)
    }
}

enum Action {
    Command(Command),
    Internal(Box<dyn Fn(&Path)>),
    Captured(Command, Box<dyn Fn(&Output)>, Option<PathBuf>),
}

pub(crate) struct GateCommand {
    pub(crate) inventory: Value,
    directory: PathBuf,
    action: Action,
}

impl GateCommand {
    pub(crate) fn execute(&mut self, signal: &std::sync::atomic::AtomicUsize) -> Result<(), i32> {
        match &mut self.action {
            Action::Command(command) => {
                println!("$ {}", self.inventory["argv"]);
                crate::process::status(command, signal).map_or_else(
                    |error| {
                        eprintln!(
                            "command {} could not start or be reaped: {error}",
                            self.inventory["id"]
                        );
                        Err(1)
                    },
                    |status| {
                        if status.success() {
                            Ok(())
                        } else {
                            Err(status.code().unwrap_or(1))
                        }
                    },
                )
            }
            Action::Captured(command, validate, journal) => {
                let output = if let Some(path) = journal {
                    crate::process::native_output(command, signal, path)
                } else {
                    crate::process::output(command, signal)
                }
                .map_err(|error| {
                    eprintln!(
                        "command {} could not start or be reaped: {error}",
                        self.inventory["id"]
                    );
                    1
                })?;
                if !output.status.success() {
                    eprintln!(
                        "captured command {} failed: {}",
                        self.inventory["id"], output.status
                    );
                    eprint!("{}", String::from_utf8_lossy(&output.stderr));
                    return Err(output.status.code().unwrap_or(1));
                }
                validate(&output);
                Ok(())
            }
            Action::Internal(check) => {
                check(&self.directory);
                Ok(())
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct Checks {
    gates: Vec<GateCommand>,
    offline: bool,
    required_environment: Vec<(String, String)>,
}

impl Checks {
    pub(crate) fn offline() -> Self {
        Self {
            offline: true,
            ..Self::default()
        }
    }

    pub(crate) fn required(environment: Vec<(String, String)>) -> Self {
        Self {
            required_environment: environment,
            ..Self::default()
        }
    }

    pub(crate) fn cargo(&mut self, directory: &Path, args: &[&str]) {
        if self.offline && args.first() == Some(&"test") && !args.contains(&"--doc") {
            crate::nextest::schedule(directory, self, args);
            return;
        }
        let mut command = Command::new("cargo");
        command.args(args).current_dir(directory);
        self.command(directory, command, "Cargo exit status");
    }

    pub(crate) fn script(&mut self, directory: &Path, label: &str, relative: &str) {
        let mut command = Command::new(directory.join(relative));
        command.current_dir(directory);
        self.command(directory, command, label);
    }

    pub(crate) fn command(&mut self, directory: &Path, command: Command, expected: &str) {
        self.register_command(None, directory, command, expected);
    }

    pub(crate) fn named_command(
        &mut self,
        id: &str,
        directory: &Path,
        command: Command,
        expected: &str,
    ) {
        self.register_command(Some(id), directory, command, expected);
    }

    fn configure_environment(&self, command: &mut Command) {
        remove_live_configuration(command);
        if !self.required_environment.is_empty() {
            // One complete invocation has one native target root, including
            // standalone consumers. Explicit fixture/probe targets stay scoped.
            if !command
                .get_envs()
                .any(|(name, _)| name == "CARGO_TARGET_DIR")
            {
                let workspace = crate::workspace();
                let target = std::env::var_os("CARGO_TARGET_DIR")
                    .map_or_else(|| workspace.join("target"), PathBuf::from);
                let target = if target.is_absolute() {
                    target
                } else {
                    workspace.join(target)
                };
                command.env("CARGO_TARGET_DIR", target);
            }
            command
                .envs(self.required_environment.clone())
                .env("CRABBER_REQUIRE_POSTGRES", "1")
                .env_remove("CRABBER_TEST_POSTGRES_URL")
                .env_remove("CRABBER_POSTGRES_URL")
                .env_remove("CRABBER_TEST_POSTGRES_CHILD_DATABASE")
                .env_remove("CRABBER_TEST_HISTORY_CHILD");
        }
        if self.offline {
            for name in [
                "CRABBER_TEST_POSTGRES_URL",
                "CRABBER_POSTGRES_URL",
                "CRABBER_REQUIRE_POSTGRES",
                "CRABBER_TEST_POSTGRES_ADMIN_URL",
                "CRABBER_TEST_RUN_POSTGRES_ADMIN_URL",
                "CRABBER_TEST_POSTGRES_RUN",
                "CRABBER_TEST_POSTGRES_CHILD_DATABASE",
                "CRABBER_TEST_HISTORY_CHILD",
                "CRABBER_TEST_POSTGRES_METRICS_FILE",
            ] {
                command.env_remove(name);
            }
            remove_live_configuration(command);
        }
    }

    fn register_command(
        &mut self,
        name: Option<&str>,
        directory: &Path,
        mut command: Command,
        expected: &str,
    ) {
        self.configure_environment(&mut command);
        let argv: Vec<_> = std::iter::once(command.get_program())
            .chain(command.get_args())
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        // IDs survive reordering and do not contain absolute checkout paths.
        let relative_argv: Vec<_> = argv
            .iter()
            .map(|arg| {
                Path::new(arg)
                    .strip_prefix(directory)
                    .map_or_else(|_| arg.clone(), |path| path.to_string_lossy().into_owned())
            })
            .collect();
        let directory_key = directory
            .strip_prefix(crate::workspace())
            .unwrap_or_else(|_| Path::new(directory.file_name().unwrap_or_default()));
        let digest = sha2::Sha256::digest(format!(
            "{}\0{}",
            directory_key.display(),
            relative_argv.join("\0")
        ));
        let id = name.map_or_else(
            || format!("command-{}", &format!("{digest:x}")[..16]),
            str::to_owned,
        );
        let features = argv
            .windows(2)
            .find(|pair| pair[0] == "--features")
            .map_or("default/workspace-unified", |pair| pair[1].as_str());
        let inventory = json!({
            "id": id,
            "argv": relative_argv,
            "features": features,
            "environment_names": command.get_envs().map(|(key, _)| key.to_string_lossy().into_owned()).collect::<Vec<_>>(),
            "backend": if features.contains("postgres") {
                "Memory; PostgreSQL only when explicitly configured; SQLite if enabled"
            } else if features.contains("sqlite") {
                "Memory and SQLite"
            } else if args_include(&argv, "--workspace") {
                "workspace-resolved features (see ledger; optional PostgreSQL is not required coverage)"
            } else { "feature-specific; see ledger" },
            "expected": expected,
            "service_mode": if self.offline { "offline; live PostgreSQL and exporter/provider credentials removed" } else { "command-specific" },
            "kind": "command",
            "phase": match argv.get(1).map(String::as_str) {
                Some("test") => "build-and-test",
                Some("nextest") if argv.get(2).is_some_and(|arg| arg == "run") => "build-and-test",
                Some("build" | "check" | "clippy") => "build-and-check",
                Some("run") => "build-and-example",
                _ => "validation",
            },
        });
        self.push(directory, inventory, Action::Command(command));
    }

    pub(crate) fn captured(
        &mut self,
        directory: &Path,
        command: Command,
        expected: &str,
        validate: impl Fn(&Output) + 'static,
    ) {
        self.command(directory, command, expected);
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["kind"] = json!("captured-command");
        if let Action::Command(command) =
            std::mem::replace(&mut gate.action, Action::Internal(Box::new(|_| {})))
        {
            gate.action = Action::Captured(command, Box::new(validate), None);
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn captured_native(
        &mut self,
        directory: &Path,
        command: Command,
        journal: PathBuf,
        validate: impl Fn(&Output) + 'static,
    ) {
        self.captured(
            directory,
            command,
            "successful canonical native build metadata",
            validate,
        );
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["native_artifacts"] = json!(journal);
        if let Action::Captured(_, _, path) = &mut gate.action {
            *path = Some(journal);
        }
    }

    pub(crate) fn internal(&mut self, id: &str, directory: &Path, check: impl Fn(&Path) + 'static) {
        self.push(
            directory,
            json!({
                "id": id,
                "argv": [],
                "features": "see docs/test-performance.md",
                "environment_names": [],
                "backend": "filesystem/static validation",
                "expected": match id {
                    "telemetry-messages" => "all runtime telemetry message sites use the qualified redacted vocabulary",
                    "agui-pins" => "all consumer manifests and locks use the qualified AG-UI source and revision",
                    "wit-copies" => "all guest and macro WIT copies equal the root contract",
                    "wasm-fixture-aliases" => "positive aliases and SHA-256 fixture manifest generated",
                    "minimal-embed-glue" => "exactly one glue marker pair and at most 60 glue lines",
                    _ => "all preparation/assertion steps succeed",
                },
                "kind": "internal",
            }),
            Action::Internal(Box::new(check)),
        );
    }

    fn push(&mut self, directory: &Path, mut inventory: Value, action: Action) {
        let prerequisite = self.gates.last().map(|gate| gate.inventory["id"].clone());
        inventory["working_directory"] = json!(
            directory
                .strip_prefix(crate::workspace())
                .unwrap_or(directory)
                .to_string_lossy()
        );
        inventory["prerequisites"] = json!(prerequisite.into_iter().collect::<Vec<_>>());
        assert!(
            !self
                .gates
                .iter()
                .any(|gate| gate.inventory["id"] == inventory["id"]),
            "duplicate gate ID"
        );
        self.gates.push(GateCommand {
            inventory,
            directory: directory.to_owned(),
            action,
        });
    }

    pub(crate) fn execute(self, options: &Options) -> i32 {
        if options.list {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &self
                        .gates
                        .iter()
                        .map(|gate| &gate.inventory)
                        .collect::<Vec<_>>()
                )
                .unwrap()
            );
            return 0;
        }
        Report::run(self.gates, options.timings.as_deref())
    }
}

fn args_include(args: &[String], value: &str) -> bool {
    args.iter().any(|arg| arg == value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_path_and_unknown_options() {
        for args in [
            vec!["--timings"],
            vec!["--typo"],
            vec!["--list", "--list"],
            vec!["--lane"],
            vec!["--lane", "postgres"],
        ] {
            assert!(
                Options::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err()
            );
        }
    }

    #[test]
    fn failure_is_reported_and_later_obligation_is_cancelled() {
        let directory =
            std::env::temp_dir().join(format!("crabber-gate-test-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let output = directory.join("timings.json");
        let mut checks = Checks::default();
        let mut failure = Command::new("sh");
        failure.args(["-c", "exit 7"]);
        failure.env("CRABBER_REPORT_SECRET", "must-not-be-serialized");
        checks.command(&directory, failure, "intentional failure");
        checks.internal("must-not-run", &directory, |_| {
            panic!("cancelled gate executed")
        });
        assert_eq!(
            checks.execute(&Options {
                list: false,
                timings: Some(output.clone()),
                ..Options::default()
            }),
            7
        );
        let text = std::fs::read_to_string(output).unwrap();
        assert!(!text.contains("must-not-be-serialized"));
        let report: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(report["status"], "failed");
        assert_eq!(report["gates"][0]["status"], "failed");
        assert_eq!(report["gates"][0]["exit_code"], 7);
        assert_eq!(report["gates"][1]["status"], "cancelled");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn offline_lane_strips_live_service_overrides_but_keeps_stderr_recipe() {
        let mut checks = Checks::offline();
        let mut command = Command::new("sh");
        command.args(["-c", "test -z \"${CRABBER_REQUIRE_POSTGRES+x}\" && test -z \"${DD_API_KEY+x}\" && test \"$RUST_LOG\" = info"])
            .env("CRABBER_REQUIRE_POSTGRES","1").env("DD_API_KEY","fixture-only-placeholder")
            .env("RUST_LOG","info");
        checks.command(Path::new("."), command, "offline service isolation");
        assert_eq!(checks.execute(&Options::default()), 0);
    }

    #[test]
    fn live_configuration_removal_preserves_scoped_fixture_handoff() {
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                concat!(
                    "test -z \"${DD_ENV+x}\" && ",
                    "test -z \"${OTEL_EXPORTER_OTLP_LOGS_HEADERS+x}\" && ",
                    "test -z \"${OTEL_EXPORTER_OTLP_METRICS_ENDPOINT+x}\" && ",
                    "test \"$CRABBER_TEST_POSTGRES_URL\" = fixture-endpoint && ",
                    "test \"$CRABBER_TEST_POSTGRES_CHILD_DATABASE\" = ct_fixture && ",
                    "test \"$RUST_LOG\" = info"
                ),
            ])
            .env("DD_ENV", "fixture-only")
            .env("OTEL_EXPORTER_OTLP_LOGS_HEADERS", "fixture-only")
            .env("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "fixture-only")
            .env("CRABBER_TEST_POSTGRES_URL", "fixture-endpoint")
            .env("CRABBER_TEST_POSTGRES_CHILD_DATABASE", "ct_fixture")
            .env("RUST_LOG", "info");
        remove_live_configuration(&mut command);
        assert!(command.status().unwrap().success());
    }

    #[test]
    fn panic_produces_terminal_report() {
        let mut checks = Checks::default();
        checks.internal("panic", Path::new("."), |_| panic!("fixture failure"));
        assert_eq!(checks.execute(&Options::default()), 1);
    }
}
