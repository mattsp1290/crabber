use crate::native_files::{Family, File, Run};
use crate::{
    checks::Checks,
    native_compiler_input::Input,
    native_remaining_evidence::{Plan, read},
    parts::{PartFamily, Quarter},
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::AtomicUsize,
};

pub(crate) struct RemainingBuild {
    directory: PathBuf,
    features: String,
    quarter: Quarter,
}
impl RemainingBuild {
    fn new(directory: &Path, features: &str, quarter: Quarter) -> Self {
        Self {
            directory: directory.to_owned(),
            features: features.to_owned(),
            quarter,
        }
    }
    pub(crate) fn template(&self) -> Command {
        let mut command = Command::new("cargo");
        command.current_dir(crate::workspace()).args([
            "nextest",
            "list",
            "--workspace",
            "--features",
            &self.features,
            "--list-type",
            "binaries-only",
            "--message-format",
            "json",
            "--cargo-message-format",
            "json-render-diagnostics",
        ]);
        command
    }
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }
    pub(crate) fn execute(&self, command: &mut Command, signal: &AtomicUsize) -> Result<(), i32> {
        let bytes =
            fs::read(File::Metadata.path(&self.directory)).expect("scheduled Cargo metadata");
        let metadata: Value = serde_json::from_slice(&bytes).expect("actual Cargo metadata");
        let plan = Plan::from_metadata(&metadata, crate::workspace(), self.quarter, &bytes);
        plan.append_arguments(command);
        plan.persist(&self.directory, command);
        let journal = File::Compiler(Family::Remaining).path(&self.directory);
        let output = match crate::checks::captured_output(command, signal, Some(&journal)) {
            Ok(output) => output,
            Err(error) => {
                Input::capture_failed(&self.directory);
                eprintln!("remaining compiler could not start or be reaped: {error}");
                return Err(1);
            }
        };
        let input = compiler_input(&self.directory, &output)?;
        plan.admit_build(&self.directory, &metadata, &input);
        Ok(())
    }
}

fn compiler_input(directory: &Path, output: &Output) -> Result<Input, i32> {
    let mut input = Input::parse(&output.stdout);
    if !output.status.success() {
        input.child_failed();
    }
    let persisted = input.persist(directory);
    if !output.status.success() {
        if let Err(error) = persisted {
            eprintln!("cannot persist failed compiler evidence: {error}");
        }
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        return Err(output.status.code().unwrap_or(1));
    }
    persisted.map_err(|error| {
        eprintln!("cannot persist compiler evidence: {error}");
        1
    })?;
    Ok(input)
}

pub(crate) fn prepare(gates: &mut Checks, directory: &Path, features: &str) {
    let Some(quarter) = gates.remaining_quarter() else {
        return;
    };
    gates.native_contribution(PartFamily::NativeRemaining, |gates| {
        gates.remaining_target_build(RemainingBuild::new(directory, features, quarter));
        crate::normal_outputs::migration_owner(
            gates,
            directory,
            "native-remaining",
            Family::Remaining,
        );
    });
}

fn base(action: &str, directory: &Path, profile: &str) -> Command {
    let mut command = Command::new("cargo");
    command
        .current_dir(crate::workspace())
        .args(["nextest", action, "--cargo-metadata"])
        .arg(File::Metadata.path(directory))
        .arg("--binaries-metadata")
        .arg(File::Binaries(Family::Remaining).path(directory))
        .args(["--profile", profile]);
    command
}

fn manifest(gates: &mut Checks, directory: &Path, profile: &str, selected: bool) {
    let name = if selected {
        "native-remaining-selected-manifest"
    } else {
        "native-remaining-manifest"
    };
    let mut command = base("list", directory, profile);
    command.args(["--message-format", "json"]);
    let output = directory.to_owned();
    gates.captured(
        Some(name),
        crate::workspace(),
        command,
        "real scoped remaining-target cases including ignored evidence",
        move |result| {
            let value: Value = serde_json::from_slice(&result.stdout).unwrap();
            let filename = if selected {
                File::Selected(Family::Remaining)
            } else {
                File::Cases(Family::Remaining)
            };
            fs::write(filename.path(&output), serde_json::to_vec(&value).unwrap()).unwrap();
            crate::native_test_evidence::admit_manifest(
                &read(File::Binaries(Family::Remaining).path(&output)),
                &value,
            );
            crate::native_test_evidence::admit_packages(
                &read(File::Metadata.path(&output)),
                &value,
            );
            if selected {
                assert_eq!(
                    value,
                    read(File::Cases(Family::Remaining).path(&output)),
                    "selected remaining inventory changed"
                );
            }
        },
    );
}

pub(crate) fn execute(gates: &mut Checks, directory: &Path, profile: &str) {
    gates.native_contribution(PartFamily::NativeRemaining, |gates| {
        manifest(gates, directory, profile, false);
        crate::nextest::scoped_group_gate(
            crate::workspace(),
            gates,
            &File::Metadata.path(directory),
            &File::Binaries(Family::Remaining).path(directory),
            profile,
            "native-remaining-resource-groups",
            File::Groups(Family::Remaining).path(directory),
        );
        manifest(gates, directory, profile, true);
        let mut command = base("run", directory, profile);
        command.args([
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
        crate::standard_tools::require_subprocess_tools(&mut command);
        let selected = File::Selected(Family::Remaining).path(directory);
        let verdict = File::Execution(Run::Remaining).path(directory);
        gates.recorded(
            "native-remaining",
            command,
            Run::Remaining.path(directory),
            "exact observed remaining case execution",
            move |result: &Output| {
                let evidence = crate::native_test_evidence::execution(
                    &read(&selected),
                    &result.stdout,
                    &result.stderr,
                );
                fs::write(&verdict, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
            },
        );
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scratch() -> PathBuf {
        std::env::temp_dir().join(format!(
            "crabber-remaining-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    #[test]
    fn missing_metadata_cannot_launch_selector_free_command() {
        let directory = scratch();
        fs::create_dir(&directory).unwrap();
        let marker = directory.join("launched");
        let request = RemainingBuild::new(&directory.join("missing"), "", Quarter::One);
        let mut command = Command::new("sh");
        command.args(["-c", "touch \"$1\"", "probe"]).arg(&marker);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            request.execute(&mut command, &AtomicUsize::new(0))
        }));
        let launched = marker.exists();
        fs::remove_dir_all(directory).unwrap();
        assert!(result.is_err());
        assert!(!launched, "metadata failure launched a fallback compiler");
    }
    #[cfg(unix)]
    #[test]
    fn compiler_failure_preserves_exit_code_when_projection_and_persistence_fail() {
        use std::os::unix::process::ExitStatusExt;
        let directory = scratch();
        let output = Output {
            status: std::process::ExitStatus::from_raw(73 << 8),
            stdout: b"invalid PRIVATE_SENTINEL".to_vec(),
            stderr: Vec::new(),
        };
        assert!(matches!(compiler_input(&directory, &output), Err(73)));
        assert!(!directory.exists());
    }
}
