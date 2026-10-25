//! Library contributions retain their own inventories and never satisfy Native gates.
use crate::{checks::Checks, parts::LibraryQuarter};
use serde_json::Value;
use std::{fs, path::Path, process::Command};

pub(crate) fn compile(gates: &mut Checks, directory: &Path, features: &str) {
    gates.native_contribution(|gates| {
        let mut command = Command::new("cargo");
        command.current_dir(crate::workspace()).args([
            "nextest", "list", "--workspace", "--lib", "--features", features,
            "--list-type", "binaries-only", "--message-format", "json",
            "--cargo-message-format", "json-render-diagnostics",
        ]);
        let output = directory.to_owned();
        gates.named_captured("native-library-build", crate::workspace(), command,
            "all metadata-declared library and proc-macro test targets with actual compiler evidence",
            directory.join("library-artifacts.jsonl"), move |result| {
                let binaries = crate::native_artifacts::summary(&result.stdout);
                crate::native_library_evidence::admit_build(&output, &binaries, &result.stdout);
                fs::write(output.join("library-binaries.json"), serde_json::to_vec(&binaries).unwrap()).unwrap();
            });
    });
}

pub(crate) fn migration_owner(gates: &mut Checks, directory: &Path) {
    gates.native_contribution(|gates| crate::normal_outputs::migration_owner(gates, directory));
}

pub(crate) fn execute(gates: &mut Checks, directory: &Path, profile: &str) {
    let Some(quarter) = gates.library_quarter() else {
        return;
    };
    gates.native_contribution(|gates| {
        let inventory = directory.join("library-tests.json");
        let output = directory.to_owned();
        let mut command = base("list", directory, profile);
        command.args(["--message-format", "json"]);
        gates.captured(
            Some("native-library-manifest"),
            crate::workspace(),
            command,
            "unpartitioned library cases, including ignored cases",
            move |result| {
                let value: Value = serde_json::from_slice(&result.stdout).unwrap();
                crate::native_library_evidence::admit_manifest(&output, &value);
                fs::write(&inventory, serde_json::to_vec(&value).unwrap()).unwrap();
            },
        );
        crate::nextest::scoped_group_gate(
            crate::workspace(),
            gates,
            &directory.join("cargo-metadata.json"),
            &directory.join("library-binaries.json"),
            profile,
            "native-library-resource-groups",
            directory.join("library-resource-groups.json"),
        );
        if quarter == LibraryQuarter::One {
            Selection::Role.schedule(gates, directory, profile);
        }
        Selection::Quarter(quarter).schedule(gates, directory, profile);
    });
}

fn base(action: &str, directory: &Path, profile: &str) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(crate::workspace())
        .args(["nextest", action, "--cargo-metadata"])
        .arg(directory.join("cargo-metadata.json"))
        .arg("--binaries-metadata")
        .arg(directory.join("library-binaries.json"))
        .args(["--profile", profile]);
    command
}

#[derive(Clone, Copy)]
enum Selection {
    Role,
    Quarter(LibraryQuarter),
}

impl Selection {
    fn name(self) -> &'static str {
        match self {
            Self::Role => "native-library-role",
            Self::Quarter(_) => "native-library-quarter",
        }
    }
    fn arguments(self, command: &mut Command) {
        let role = crate::nextest::CLUSTER_ROLE_TEST;
        match self {
            Self::Role => {
                command.args(["-E", &format!("test(={role})")]);
            }
            Self::Quarter(quarter) => {
                command.args([
                    "-E",
                    &format!("not test(={role})"),
                    "--partition",
                    &format!("hash:{}/4", quarter.index()),
                ]);
            }
        }
    }
    fn schedule(self, gates: &mut Checks, directory: &Path, profile: &str) {
        let mut list = base("list", directory, profile);
        list.args(["--message-format", "json"]);
        self.arguments(&mut list);
        let selected = directory.join(format!("{}-tests.json", self.name()));
        let destination = selected.clone();
        let full = directory.join("library-tests.json");
        gates.captured(
            Some(&format!("{}-manifest", self.name())),
            crate::workspace(),
            list,
            "real selected library partition, bound to unpartitioned inventory",
            move |result| {
                let value: Value = serde_json::from_slice(&result.stdout).unwrap();
                crate::native_library_evidence::admit_selection(
                    &read(&full),
                    &value,
                    matches!(self, Self::Role),
                );
                fs::write(&destination, serde_json::to_vec(&value).unwrap()).unwrap();
            },
        );
        let mut run = base("run", directory, profile);
        run.args([
            "--retries",
            "0",
            "--no-fail-fast",
            "--no-tests",
            "fail",
            "--color",
            "never",
            "--success-output",
            "never",
            "--status-level",
            "pass",
        ]);
        self.arguments(&mut run);
        crate::standard_tools::require_subprocess_tools(&mut run);
        let evidence = directory.join(self.name());
        let verdict = directory.join(format!("{}-execution.json", self.name()));
        gates.recorded(self.name(), run, evidence, move |result| {
            let expected = crate::native_library_evidence::selected(&read(&selected));
            let actual = crate::native_library_evidence::passing(&result.stdout, &result.stderr);
            assert_eq!(actual, expected, "executed passing cases differ from admitted selection");
            fs::write(&verdict, serde_json::to_vec_pretty(&serde_json::json!({
                "exact_equal": true, "actual_cases": actual.into_iter().map(|((binary, case), count)| serde_json::json!([binary, case, count])).collect::<Vec<_>>(),
                "stdin": "null", "success_output": "never", "status_level": "pass", "color": "never",
            })).unwrap()).unwrap();
        });
    }
}

fn read(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}
