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
    pub(crate) part: Option<crate::parts::Part>,
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
                "--part" if options.part.is_none() => {
                    options.part = Some(match args.next().map(String::as_str) {
                        Some("quality-contracts") => crate::parts::Part::QualityContracts,
                        Some("quality-artifacts") => crate::parts::Part::QualityArtifacts,
                        name => crate::parts::native_slots()
                            .find(|part| Some(part.name()) == name)
                            .ok_or_else(|| {
                                "--part accepts quality-contracts, quality-artifacts or native-library-1 through native-library-4 or native-remaining-1 through native-remaining-4".to_owned()
                            })?,
                    });
                }
                "--timings" if options.timings.is_none() => {
                    options.timings =
                        Some(PathBuf::from(args.next().ok_or("--timings needs a path")?));
                }
                _ => return Err(format!("unexpected check argument: {argument}")),
            }
        }
        if options.part.is_some() && options.offline {
            return Err("--part cannot be combined with --lane".into());
        }
        if options.part.is_some() && !cfg!(feature = "postgres-check") {
            return Err("--part requires cargo xtask check".into());
        }
        Ok(options)
    }
}

enum Action {
    #[cfg(feature = "postgres-check")]
    RemainingBuild(Command, crate::native_remaining::RemainingBuild),
    Command(Command),
    Internal(Box<dyn Fn(&Path)>),
    Captured(
        Command,
        Box<dyn Fn(&Output)>,
        Option<PathBuf>,
        Option<PathBuf>,
    ),
}

pub(crate) struct GateCommand {
    pub(crate) inventory: Value,
    directory: PathBuf,
    action: Action,
    #[cfg(feature = "postgres-check")]
    role: crate::parts::GateRole,
}

impl GateCommand {
    pub(crate) fn execute(&mut self, signal: &std::sync::atomic::AtomicUsize) -> Result<(), i32> {
        match &mut self.action {
            #[cfg(feature = "postgres-check")]
            Action::RemainingBuild(command, request) => request.execute(command, signal),
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
            Action::Captured(command, validate, journal, evidence) => {
                let output =
                    captured_output(command, signal, journal.as_deref()).map_err(|error| {
                        eprintln!(
                            "command {} could not start or be reaped: {error}",
                            self.inventory["id"]
                        );
                        1
                    })?;
                if let Some(path) = evidence {
                    std::fs::write(path.with_extension("stdout.log"), &output.stdout)
                        .expect("persist actual stdout");
                    std::fs::write(path.with_extension("stderr.log"), &output.stderr)
                        .expect("persist actual stderr");
                }
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

pub(crate) fn captured_output(
    command: &mut Command,
    signal: &std::sync::atomic::AtomicUsize,
    journal: Option<&Path>,
) -> std::io::Result<Output> {
    match journal {
        Some(path) => crate::process::native_output(command, signal, path),
        None => crate::process::output(command, signal),
    }
}

#[derive(Default)]
pub(crate) struct Checks {
    gates: Vec<GateCommand>,
    offline: bool,
    required_environment: Vec<(String, String)>,
    role: crate::parts::GateRole,
    partitioned: bool,
    part: Option<crate::parts::Part>,
    scope: Option<crate::parts::Scope>,
}

impl Checks {
    #[cfg(feature = "postgres-check")]
    pub(crate) fn inventory(&self) -> Value {
        serde_json::to_value(
            self.gates
                .iter()
                .map(|gate| &gate.inventory)
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    pub(crate) fn offline() -> Self {
        Self {
            offline: true,
            ..Self::default()
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn required(environment: Vec<(String, String)>) -> Self {
        Self {
            required_environment: environment,
            ..Self::default()
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn partitioned(
        environment: Vec<(String, String)>,
        part: Option<crate::parts::Part>,
    ) -> Self {
        Self {
            partitioned: true,
            part,
            ..Self::required(environment)
        }
    }

    pub(crate) fn owned<T>(
        &mut self,
        owner: crate::parts::Owner,
        schedule: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = self.role.clone();
        self.role = crate::parts::GateRole::Required(owner, std::collections::BTreeSet::default());
        let result = schedule(self);
        self.role = previous;
        result
    }

    pub(crate) fn prepares_for<T>(
        &mut self,
        family: crate::parts::PartFamily,
        schedule: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let previous = self.role.clone();
        if let crate::parts::GateRole::Required(owner, mut existing) = previous.clone() {
            existing.insert(family);
            self.role = crate::parts::GateRole::Required(owner, existing);
        }
        let result = schedule(self);
        self.role = previous;
        result
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn library_quarter(&self) -> Option<crate::parts::Quarter> {
        match self.part {
            Some(crate::parts::Part::NativeLibraries(quarter)) => Some(quarter),
            _ => None,
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn remaining_quarter(&self) -> Option<crate::parts::Quarter> {
        match self.part {
            Some(crate::parts::Part::NativeRemaining(q)) => Some(q),
            _ => None,
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn native_contribution(
        &mut self,
        family: crate::parts::PartFamily,
        schedule: impl FnOnce(&mut Self),
    ) {
        if self.part.is_some_and(|part| part.family() == family) {
            let previous = std::mem::replace(
                &mut self.role,
                crate::parts::GateRole::NativeContribution(family),
            );
            schedule(self);
            self.role = previous;
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn preparation(
        &mut self,
        part: crate::parts::Part,
        schedule: impl FnOnce(&mut Self),
    ) {
        if self.part == Some(part) {
            let previous = self.role.clone();
            self.role = crate::parts::GateRole::Preparation(part);
            schedule(self);
            self.role = previous;
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn is_part(&self, part: crate::parts::Part) -> bool {
        self.part == Some(part)
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn select_part(mut self) -> Self {
        use crate::parts::{GateRole, Part, Scope};
        let ownership = crate::parts::ownership(
            self.gates
                .iter()
                .filter(|gate| matches!(gate.role, GateRole::Required(..)))
                .map(|gate| &gate.inventory),
        );
        if let Some(part) = self.part {
            self.gates.retain_mut(|gate| {
                let role = match &gate.role {
                    GateRole::Required(owner, _) if part.owns(*owner) => "obligation",
                    GateRole::Required(_, prepared) if prepared.contains(&part.family()) => {
                        "preparation"
                    }
                    GateRole::Preparation(prepared) if *prepared == part => "preparation",
                    GateRole::NativeContribution(family) if *family == part.family() => {
                        "contribution"
                    }
                    _ => return false,
                };
                gate.inventory["execution_role"] = json!(role);
                gate.inventory["semantic_credit"] = json!(role == "obligation");
                true
            });
            let mut previous = None;
            for gate in &mut self.gates {
                gate.inventory["prerequisites"] = json!(previous.into_iter().collect::<Vec<_>>());
                previous = Some(gate.inventory["id"].clone());
            }
            self.scope = Some(Scope {
                part,
                ownership,
                demos: if part == Part::QualityArtifacts {
                    crate::postgres::demo_inventory()
                } else {
                    json!([])
                },
            });
        }
        self
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

    #[cfg(feature = "postgres-check")]
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
                .env_remove("CRABBER_TEST_POSTGRES_CHILD_ROLE")
                .env_remove("CRABBER_TEST_HISTORY_CHILD");
        }
        if self.part == Some(crate::parts::Part::QualityContracts) {
            for name in [
                "CRABBER_TEST_POSTGRES_ADMIN_URL",
                "CRABBER_TEST_RUN_POSTGRES_ADMIN_URL",
                "CRABBER_TEST_POSTGRES_RUN",
                "CRABBER_TEST_POSTGRES_TEMPLATE_URL",
                "CRABBER_TEST_POSTGRES_METRICS_FILE",
                "CRABBER_TEST_POSTGRES_URL",
                "CRABBER_POSTGRES_URL",
                "CRABBER_TEST_POSTGRES_CHILD_DATABASE",
                "CRABBER_TEST_POSTGRES_CHILD_ROLE",
                "CRABBER_TEST_HISTORY_CHILD",
                "CRABBER_TEST_POSTGRES_TRANSPORT",
                "PGAPPNAME",
                "PGDATABASE",
                "PGHOST",
                "PGPORT",
                "PGUSER",
                "PGPASSWORD",
                "PGSERVICE",
                "PGSERVICEFILE",
                "PGPASSFILE",
                "DATABASE_URL",
            ] {
                command.env_remove(name);
            }
        }
        if self.part.is_some() {
            crate::standard_tools::require_subprocess_tools(command);
            command.env("CRABBER_REQUIRE_POSTGRES", "1");
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
                "CRABBER_TEST_POSTGRES_CHILD_ROLE",
                "CRABBER_TEST_HISTORY_CHILD",
                "CRABBER_TEST_POSTGRES_METRICS_FILE",
                "CRABBER_TEST_POSTGRES_TEMPLATE_URL",
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
        name: Option<&str>,
        directory: &Path,
        command: Command,
        expected: &str,
        validate: impl Fn(&Output) + 'static,
    ) {
        self.register_command(name, directory, command, expected);
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["kind"] = json!("captured-command");
        if let Action::Command(command) =
            std::mem::replace(&mut gate.action, Action::Internal(Box::new(|_| {})))
        {
            gate.action = Action::Captured(command, Box::new(validate), None, None);
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn recorded(
        &mut self,
        id: &str,
        command: Command,
        evidence: PathBuf,
        expected: &str,
        validate: impl Fn(&Output) + 'static,
    ) {
        self.captured(Some(id), crate::workspace(), command, expected, validate);
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["execution_evidence"] = json!(evidence);
        if let Action::Captured(_, _, _, path) = &mut gate.action {
            *path = Some(evidence);
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
            None,
            directory,
            command,
            "successful canonical native build metadata",
            validate,
        );
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["native_artifacts"] = json!(journal);
        if let Action::Captured(_, _, path, _) = &mut gate.action {
            *path = Some(journal);
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn named_captured(
        &mut self,
        id: &str,
        directory: &Path,
        command: Command,
        expected: &str,
        journal: PathBuf,
        validate: impl Fn(&Output) + 'static,
    ) {
        self.named_command(id, directory, command, expected);
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["cargo_artifacts"] = json!(journal);
        if let Action::Command(command) =
            std::mem::replace(&mut gate.action, Action::Internal(Box::new(|_| {})))
        {
            gate.action = Action::Captured(command, Box::new(validate), Some(journal), None);
        }
    }

    #[cfg(feature = "postgres-check")]
    pub(crate) fn remaining_target_build(
        &mut self,
        request: crate::native_remaining::RemainingBuild,
    ) {
        let directory = request.directory().to_owned();
        self.register_command(
            Some("native-remaining-build"),
            crate::workspace(),
            request.template(),
            "exact metadata-derived remaining target closure with observed normal prerequisites",
        );
        let gate = self.gates.last_mut().unwrap();
        gate.inventory["kind"] = json!("native-build-operation");
        gate.inventory["command_template"] = gate.inventory["argv"].clone();
        gate.inventory["argv"] = json!([]);
        gate.inventory["resolved_plan"] = json!(directory.join("remaining-target-plan.json"));
        gate.inventory["cargo_artifacts"] = json!(directory.join("remaining-artifacts.jsonl"));
        gate.inventory["compiler_input"] = json!(directory.join("remaining-compiler-input.json"));
        if let Action::Command(command) =
            std::mem::replace(&mut gate.action, Action::Internal(Box::new(|_| {})))
        {
            gate.action = Action::RemainingBuild(command, request);
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
        if self.partitioned {
            match &self.role {
                crate::parts::GateRole::Required(owner, _) => {
                    inventory["owner"] = json!(owner.name());
                }
                #[cfg(feature = "postgres-check")]
                crate::parts::GateRole::NativeContribution(_) => {
                    inventory["owner"] = json!("native");
                }
                #[cfg(feature = "postgres-check")]
                crate::parts::GateRole::Preparation(part) => {
                    inventory["preparation_for"] = json!(part.name());
                }
                crate::parts::GateRole::Unpartitioned => {
                    panic!("required gate needs a source owner")
                }
            }
        }
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
            #[cfg(feature = "postgres-check")]
            role: self.role.clone(),
        });
    }

    pub(crate) fn execute(self, options: &Options) -> i32 {
        if options.list {
            let gates: Vec<_> = self.gates.iter().map(|gate| &gate.inventory).collect();
            let inventory = if let Some(scope) = self.scope {
                let mut metadata = scope.metadata();
                metadata["gates"] = json!(gates);
                metadata
            } else {
                json!(gates)
            };
            println!("{}", serde_json::to_string_pretty(&inventory).unwrap());
            return 0;
        }
        Report::run(self.gates, options.timings.as_deref(), self.scope)
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
            vec!["--part"],
            vec!["--part", "typo"],
            vec!["--part", "quality-contracts", "--part", "quality-contracts"],
            vec!["--part", "quality-contracts", "--lane", "offline"],
            vec!["--lane", "offline", "--part", "quality-contracts"],
            vec!["--part", "quality-artifacts", "--lane", "offline"],
            vec!["--part", "quality-artifacts", "--part", "quality-artifacts"],
        ] {
            assert!(
                Options::parse(&args.into_iter().map(str::to_owned).collect::<Vec<_>>()).is_err()
            );
        }
        #[cfg(feature = "postgres-check")]
        for (name, part) in [
            ("quality-contracts", crate::parts::Part::QualityContracts),
            ("quality-artifacts", crate::parts::Part::QualityArtifacts),
        ] {
            assert_eq!(
                Options::parse(&["--part".into(), name.into()])
                    .unwrap()
                    .part,
                Some(part)
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
        let artifact_path = directory.join("artifact-stage.json");
        let mut artifact = Checks {
            scope: Some(crate::parts::Scope {
                part: crate::parts::Part::QualityArtifacts,
                ownership: json!({}),
                demos: json!([]),
            }),
            ..Checks::default()
        };
        let mut failure = Command::new("sh");
        failure.args(["-c", "exit 7"]);
        artifact.command(&directory, failure, "actual failed artifact stage");
        assert_eq!(
            artifact.execute(&Options {
                timings: Some(artifact_path.clone()),
                ..Options::default()
            }),
            7
        );
        let stage: Value = serde_json::from_slice(&std::fs::read(artifact_path).unwrap()).unwrap();
        assert_eq!(stage["status"], "awaiting-owned-cleanup");
        assert_eq!(stage["selected_gates_status"], "failed");
        assert!(stage["processes_drained"].is_null());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn offline_lane_strips_live_service_overrides_but_keeps_stderr_recipe() {
        let mut checks = Checks::offline();
        let mut command = Command::new("sh");
        command.args(["-c", "test -z \"${CRABBER_REQUIRE_POSTGRES+x}\" && test -z \"${CRABBER_TEST_POSTGRES_CHILD_ROLE+x}\" && test -z \"${DD_API_KEY+x}\" && test \"$RUST_LOG\" = info"])
            .env("CRABBER_REQUIRE_POSTGRES","1").env("CRABBER_TEST_POSTGRES_CHILD_ROLE","fixture-worker").env("DD_API_KEY","fixture-only-placeholder")
            .env("RUST_LOG","info");
        checks.command(Path::new("."), command, "offline service isolation");
        assert_eq!(checks.execute(&Options::default()), 0);
        let mut contracts = Checks {
            part: Some(crate::parts::Part::QualityContracts),
            ..Checks::default()
        };
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                concat!(
                    "test -z \"${CRABBER_TEST_POSTGRES_RUN+x}\" && ",
                    "test -z \"${CRABBER_TEST_RUN_POSTGRES_ADMIN_URL+x}\" && ",
                    "test -z \"${CRABBER_TEST_POSTGRES_TEMPLATE_URL+x}\" && ",
                    "test -z \"${CRABBER_TEST_POSTGRES_METRICS_FILE+x}\" && ",
                    "test -z \"${PGAPPNAME+x}\" && ",
                    "test \"$BACKGROUND_JOBS_REQUIRE_SHELL\" = 1 && ",
                    "test \"$PYTHON_REPL_REQUIRE_PYTHON\" = 1 && ",
                    "test \"$CRABBER_REQUIRE_POSTGRES\" = 1"
                ),
            ])
            .env("CRABBER_TEST_POSTGRES_RUN", "foreign-run")
            .env("CRABBER_TEST_RUN_POSTGRES_ADMIN_URL", "foreign-admin")
            .env("CRABBER_TEST_POSTGRES_TEMPLATE_URL", "foreign-template")
            .env("CRABBER_TEST_POSTGRES_METRICS_FILE", "foreign-ledger")
            .env("PGAPPNAME", "foreign-application");
        contracts.command(
            Path::new("."),
            command,
            "contracts rejects inherited fixture context",
        );
        assert_eq!(contracts.execute(&Options::default()), 0);
        let mut artifacts = Checks {
            part: Some(crate::parts::Part::QualityArtifacts),
            required_environment: vec![
                ("CRABBER_TEST_POSTGRES_RUN".into(), "owned-run".into()),
                (
                    "CRABBER_TEST_RUN_POSTGRES_ADMIN_URL".into(),
                    "owned-admin".into(),
                ),
            ],
            ..Checks::default()
        };
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                concat!(
                    "test \"$CRABBER_TEST_POSTGRES_RUN\" = owned-run && ",
                    "test \"$CRABBER_TEST_RUN_POSTGRES_ADMIN_URL\" = owned-admin && ",
                    "test \"$CRABBER_TEST_POSTGRES_TEMPLATE_URL\" = owned-template && ",
                    "test \"$BACKGROUND_JOBS_REQUIRE_SHELL\" = 1 && ",
                    "test \"$PYTHON_REPL_REQUIRE_PYTHON\" = 1 && ",
                    "test \"$CRABBER_REQUIRE_POSTGRES\" = 1"
                ),
            ])
            .env("CRABBER_TEST_POSTGRES_TEMPLATE_URL", "owned-template");
        artifacts.command(
            Path::new("."),
            command,
            "artifact owner retains its scoped handoff",
        );
        assert_eq!(artifacts.execute(&Options::default()), 0);
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
    #[cfg(feature = "postgres-check")]
    #[test]
    fn library_parts_project_closed_preparation_without_whole_native_credit() {
        use crate::parts::{Part, Quarter};
        let directory =
            crate::workspace().join("target/test-performance/library-list-purity-unused");
        let full = crate::complete::schedule(Vec::new(), &directory, None, None);
        for (label, quarter) in [
            ("native-library-1", Quarter::One),
            ("native-library-2", Quarter::Two),
            ("native-library-3", Quarter::Three),
            ("native-library-4", Quarter::Four),
        ] {
            assert_eq!(
                Options::parse(&["--part".into(), label.into()])
                    .unwrap()
                    .part,
                Some(Part::NativeLibraries(quarter))
            );
            let partial = crate::complete::schedule(
                Vec::new(),
                &directory,
                None,
                Some(Part::NativeLibraries(quarter)),
            );
            let metadata = partial.scope.as_ref().unwrap().metadata();
            assert_eq!(
                metadata["ownership"],
                crate::parts::ownership(full.gates.iter().map(|g| &g.inventory))
            );
            assert_eq!(metadata["qualified"], false);
            assert_eq!(metadata["full_coverage_policy_executed"], false);
            assert_eq!(metadata["requires_owned_postgres"], true);
            assert_eq!(metadata["postgres_demos"], json!([]));
            assert_library_gate_selection(&partial, quarter);
            assert!(
                partial
                    .gates
                    .iter()
                    .all(|gate| gate.inventory["semantic_credit"] == false)
            );
            let owner = partial
                .gates
                .iter()
                .position(|gate| gate.inventory["id"] == "native-library-migration-owner")
                .unwrap();
            assert_eq!(
                partial.gates[owner + 1].inventory["argv"][1],
                "postgres-template-migrate"
            );
            for gate in &partial.gates {
                if gate.inventory["execution_role"] == "preparation" {
                    let original = full
                        .gates
                        .iter()
                        .find(|original| original.inventory["id"] == gate.inventory["id"])
                        .unwrap();
                    assert_eq!(gate.inventory["argv"], original.inventory["argv"]);
                }
            }
            assert_eq!(
                partial.execute(&Options {
                    list: true,
                    part: Some(Part::NativeLibraries(quarter)),
                    ..Options::default()
                }),
                0
            );
        }
        assert!(!directory.exists());
        for invalid in [
            "native-library-0",
            "native-library-5",
            "native-library-01",
            "native-libraries",
        ] {
            assert!(Options::parse(&["--part".into(), invalid.into()]).is_err());
        }
    }

    #[cfg(feature = "postgres-check")]
    #[test]
    fn remaining_parts_project_closed_preparation_without_library_or_whole_native_credit() {
        use crate::parts::{Part, Quarter};
        let directory =
            crate::workspace().join("target/test-performance/remaining-list-purity-unused");
        let full = crate::complete::schedule(Vec::new(), &directory, None, None);
        assert_eq!(full.gates.len(), 72);
        for (label, quarter) in [
            ("native-remaining-1", Quarter::One),
            ("native-remaining-2", Quarter::Two),
            ("native-remaining-3", Quarter::Three),
            ("native-remaining-4", Quarter::Four),
        ] {
            assert_eq!(
                Options::parse(&["--part".into(), label.into()])
                    .unwrap()
                    .part,
                Some(Part::NativeRemaining(quarter))
            );
            let partial = crate::complete::schedule(
                Vec::new(),
                &directory,
                None,
                Some(Part::NativeRemaining(quarter)),
            );
            let metadata = partial.scope.as_ref().unwrap().metadata();
            assert_eq!(metadata["target_quarter"], quarter.index());
            assert_eq!(metadata["qualified"], false);
            assert_eq!(metadata["full_coverage_policy_executed"], false);
            assert_eq!(metadata["postgres_demos"], json!([]));
            assert!(
                partial
                    .gates
                    .iter()
                    .all(|g| g.inventory["semantic_credit"] == false)
            );
            assert!(partial.gates.iter().all(|g| {
                !g.inventory["id"]
                    .as_str()
                    .unwrap()
                    .starts_with("native-library")
            }));
            let build = partial
                .gates
                .iter()
                .find(|g| g.inventory["id"] == "native-remaining-build")
                .unwrap();
            assert_eq!(build.inventory["kind"], "native-build-operation");
            assert_eq!(build.inventory["argv"], json!([]));
            assert!(build.inventory["command_template"].is_array());
            let owner = partial
                .gates
                .iter()
                .position(|g| g.inventory["id"] == "native-remaining-migration-owner")
                .unwrap();
            assert_eq!(
                partial.gates[owner + 1].inventory["argv"][1],
                "postgres-template-migrate"
            );
            assert_eq!(
                partial.execute(&Options {
                    list: true,
                    part: Some(Part::NativeRemaining(quarter)),
                    ..Options::default()
                }),
                0
            );
        }
        assert!(!directory.exists());
        for invalid in [
            "native-remaining-0",
            "native-remaining-5",
            "native-remaining-01",
        ] {
            assert!(Options::parse(&["--part".into(), invalid.into()]).is_err());
        }
    }

    #[cfg(feature = "postgres-check")]
    fn assert_library_gate_selection(partial: &Checks, quarter: crate::parts::Quarter) {
        use crate::parts::Quarter;
        let ids: std::collections::BTreeSet<_> = partial
            .gates
            .iter()
            .map(|gate| gate.inventory["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), partial.gates.len());
        for id in [
            "acquire-wasm-fixture-output",
            "wasm-fixture-aliases",
            "release-wasm-fixture-output",
            "prepare-complete-manifests",
            "standard-tool-prerequisites",
            "native-library-build",
            "native-library-migration-owner",
            "native-library-manifest",
            "native-library-resource-groups",
            "native-library-quarter-manifest",
            "native-library-quarter",
        ] {
            assert!(ids.contains(id), "missing {id}");
        }
        for id in [
            "required-coverage-and-features",
            "native-test-manifest",
            "native-resource-groups",
            "native-cluster-role",
            "native-complement",
            "normal-output-build",
        ] {
            assert!(!ids.contains(id), "selected full obligation {id}");
        }
        assert_eq!(ids.contains("native-library-role"), quarter == Quarter::One);
        assert_eq!(
            ids.contains("native-library-role-manifest"),
            quarter == Quarter::One
        );
    }

    #[cfg(feature = "postgres-check")]
    fn assert_part_projection(full: &Checks, directory: &Path, part: crate::parts::Part) {
        use crate::parts::Part;
        let partial = crate::complete::schedule(Vec::new(), directory, None, Some(part));
        let expected: Vec<_> = full
            .gates
            .iter()
            .filter(|g| g.inventory["owner"] == part.name())
            .map(|g| g.inventory["id"].clone())
            .collect();
        let actual: Vec<_> = partial
            .gates
            .iter()
            .filter(|g| g.inventory["semantic_credit"] == true)
            .map(|g| g.inventory["id"].clone())
            .collect();
        assert_eq!(actual, expected);
        let metadata = partial.scope.as_ref().unwrap().metadata();
        assert_eq!(metadata["qualified"], false);
        assert_eq!(metadata["full_native_suite_executed"], false);
        assert_eq!(
            metadata["requires_owned_postgres"],
            part == Part::QualityArtifacts
        );
        let mut previous = None;
        for gate in &partial.gates {
            assert_eq!(
                gate.inventory["prerequisites"],
                json!(previous.into_iter().collect::<Vec<_>>())
            );
            previous = Some(gate.inventory["id"].clone());
        }
        if part == Part::QualityContracts {
            assert_eq!(partial.gates.len(), 41);
            assert_eq!(metadata["postgres_demos"], json!([]));
        } else {
            assert_artifact_preparation(&partial, full);
            assert_eq!(metadata["postgres_demos"].as_array().unwrap().len(), 4);
        }
        assert_eq!(
            partial.execute(&Options {
                list: true,
                part: Some(part),
                ..Options::default()
            }),
            0
        );
        assert!(
            !directory.exists(),
            "listing must not execute any preparation"
        );
    }

    #[cfg(feature = "postgres-check")]
    fn assert_artifact_preparation(partial: &Checks, full: &Checks) {
        assert_eq!(partial.gates.len(), 33);
        assert_eq!(
            partial
                .gates
                .iter()
                .filter(|g| g.inventory["execution_role"] == "preparation")
                .count(),
            10
        );
        let nextest: Vec<_> = partial
            .gates
            .iter()
            .enumerate()
            .filter(|(_, g)| {
                g.inventory["argv"].as_array().is_some_and(|args| {
                    args.starts_with(&[json!("cargo"), json!("nextest"), json!("list")])
                        || args.starts_with(&[json!("cargo"), json!("nextest"), json!("run")])
                })
            })
            .collect();
        assert_eq!(nextest.len(), 1);
        let (compiler, gate) = nextest[0];
        let full_compiler = full
            .gates
            .iter()
            .find(|g| g.inventory["id"] == gate.inventory["id"])
            .unwrap();
        assert_eq!(gate.inventory["argv"], full_compiler.inventory["argv"]);
        let argv = gate.inventory["argv"].as_array().unwrap();
        assert!(argv.starts_with(&[json!("cargo"), json!("nextest"), json!("list")]));
        assert!(
            argv.windows(2)
                .any(|pair| pair == [json!("--list-type"), json!("binaries-only")])
        );
        assert!(!argv.iter().any(|arg| arg == "--binaries-metadata"));
        assert_eq!(gate.inventory["owner"], "native");
        assert_eq!(gate.inventory["execution_role"], "preparation");
        assert_eq!(gate.inventory["semantic_credit"], false);
        let super::Action::Captured(_, _, Some(journal), None) = &gate.action else {
            panic!("canonical compiler must retain its native journal");
        };
        assert!(journal.ends_with("native-artifacts.jsonl"));
        assert_eq!(gate.inventory["native_artifacts"], json!(journal));
        assert!(gate.inventory["cargo_artifacts"].is_null());
        let position = |id: &str| {
            partial
                .gates
                .iter()
                .position(|g| g.inventory["id"] == id)
                .unwrap()
        };
        let examples = position("prepare-normal-examples");
        let bins = position("normal-output-build");
        assert!(matches!(
            &partial.gates[examples].action,
            super::Action::Internal(_)
        ));
        assert_eq!(partial.gates[examples].inventory["argv"], json!([]));
        assert_eq!(
            partial.gates[examples].inventory["execution_role"],
            "preparation"
        );
        assert_eq!(partial.gates[examples].inventory["semantic_credit"], false);
        let gate = &partial.gates[bins];
        let super::Action::Captured(_, _, Some(journal), None) = &gate.action else {
            panic!("normal-output capture must retain its Cargo journal");
        };
        assert_eq!(gate.inventory["cargo_artifacts"], json!(journal));
        assert!(gate.inventory["native_artifacts"].is_null());
        let migration = partial
            .gates
            .iter()
            .position(|g| g.inventory["argv"][1] == "postgres-template-migrate")
            .unwrap();
        assert!(compiler < examples && examples < bins);
        assert_eq!(bins + 1, migration);
        assert_eq!(partial.gates[migration].inventory["owner"], "native");
        assert_eq!(partial.gates[migration].inventory["semantic_credit"], false);
        let full_bins = full
            .gates
            .iter()
            .find(|g| g.inventory["id"] == "normal-output-build")
            .unwrap();
        let mut expected = full_bins.inventory["argv"].as_array().unwrap().clone();
        assert!(expected.iter().any(|arg| arg == "--tests"));
        expected.extend([json!("--message-format"), json!("json-render-diagnostics")]);
        assert_eq!(partial.gates[bins].inventory["argv"], json!(expected));
        assert_eq!(
            partial.gates[migration].inventory["execution_role"],
            "preparation"
        );
    }

    #[cfg(feature = "postgres-check")]
    fn assert_stable_gate_ids(checks: &Checks, directory: &Path) {
        let other_directory = directory.with_extension("other");
        let other = crate::complete::schedule(Vec::new(), &other_directory, None, None);
        let commands = |checks: &Checks| {
            checks
                .gates
                .iter()
                .map(|gate| gate.inventory["argv"].clone())
                .collect::<Vec<_>>()
        };
        assert_ne!(
            commands(checks),
            commands(&other),
            "output paths must really differ"
        );
        let identities = |checks: &Checks| {
            checks
                .gates
                .iter()
                .map(|gate| {
                    json!({
                        "id": gate.inventory["id"],
                        "prerequisites": gate.inventory["prerequisites"],
                        "owner": gate.inventory["owner"],
                    })
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(
            identities(checks),
            identities(&other),
            "obligation identity must not depend on output paths"
        );
        let ids: std::collections::BTreeSet<_> = checks
            .gates
            .iter()
            .map(|gate| gate.inventory["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), checks.gates.len());
        for id in [
            "native-test-manifest",
            "native-resource-groups",
            "native-cluster-role",
            "native-complement",
        ] {
            let gate = checks
                .gates
                .iter()
                .find(|gate| gate.inventory["id"] == id)
                .unwrap();
            assert_eq!(gate.inventory["owner"], "native");
        }
        for id in ["native-test-manifest", "native-resource-groups"] {
            let gate = checks
                .gates
                .iter()
                .find(|gate| gate.inventory["id"] == id)
                .unwrap();
            assert!(matches!(
                &gate.action,
                super::Action::Captured(_, _, None, None)
            ));
            assert!(gate.inventory["cargo_artifacts"].is_null());
            assert!(gate.inventory["native_artifacts"].is_null());
        }
        assert!(!other_directory.exists());
    }

    #[cfg(feature = "postgres-check")]
    #[test]
    fn complete_migrates_the_owned_template_after_builds_before_native_execution() {
        let directory = std::env::temp_dir().join(format!(
            "crabber-migration-list-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        assert!(!directory.exists());
        let checks = crate::complete::schedule(Vec::new(), &directory, None, None);
        assert_stable_gate_ids(&checks, &directory);
        let ownership = crate::parts::ownership(checks.gates.iter().map(|gate| &gate.inventory));
        assert_eq!(checks.gates.len(), 72);
        assert_eq!(ownership["quality-contracts"].as_array().unwrap().len(), 41);
        assert_eq!(ownership["quality-artifacts"].as_array().unwrap().len(), 23);
        assert_eq!(ownership["native"].as_array().unwrap().len(), 8);
        assert_part_projection(&checks, &directory, crate::parts::Part::QualityContracts);
        assert_part_projection(&checks, &directory, crate::parts::Part::QualityArtifacts);
        for invalid in [
            json!({"id": "lost"}),
            json!({"id": "lost", "owner": "typo"}),
            json!({"id": "contracts", "owner": "native"}),
        ] {
            let records = [
                json!({"id": "contracts", "owner": "quality-contracts"}),
                json!({"id": "artifacts", "owner": "quality-artifacts"}),
                json!({"id": "native", "owner": "native"}),
                invalid,
            ];
            assert!(std::panic::catch_unwind(|| crate::parts::ownership(&records)).is_err());
        }
        let position = |prefix: &[&str]| {
            let prefix: Vec<Value> = prefix.iter().map(|arg| json!(arg)).collect();
            checks
                .gates
                .iter()
                .position(|gate| {
                    gate.inventory["argv"]
                        .as_array()
                        .is_some_and(|args| args.starts_with(&prefix))
                })
                .unwrap()
        };
        let compile = position(&["cargo", "nextest", "list", "--workspace"]);
        let bins = position(&["cargo", "build", "--workspace", "--bins"]);
        let migration = checks
            .gates
            .iter()
            .position(|gate| gate.inventory["argv"][1] == "postgres-template-migrate")
            .unwrap();
        let behavior = position(&["cargo", "nextest", "run", "--cargo-metadata"]);
        assert!(compile < bins && bins + 1 == migration && migration < behavior);
        assert_eq!(
            checks.gates[migration].inventory["prerequisites"],
            json!([checks.gates[bins].inventory["id"]])
        );
        assert_eq!(
            checks.execute(&Options {
                list: true,
                ..Options::default()
            }),
            0
        );
        assert!(
            !directory.exists(),
            "--list must not initialize a fixture run"
        );
    }
}
