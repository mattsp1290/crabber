//! Gates for the standard tools and native extensions.
use crate::checks::Checks;
use std::{path::Path, process::Command};

const PREREQUISITES: &str =
    "standard tools tests need ripgrep >= 14 on PATH; extensions tests need python3 3.11-3.14";

// Fail before the long workspace gate when subprocess-backed tools cannot run.
pub(crate) fn check_tool_prerequisites() {
    let output = Command::new("rg")
        .arg("--version")
        .output()
        .expect(PREREQUISITES);
    let version = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success()
            && version
                .split_whitespace()
                .nth(1)
                .and_then(|number| number.split('.').next())
                .and_then(|number| number.parse::<u32>().ok())
                .is_some_and(|major| major >= 14),
        "{PREREQUISITES}"
    );
    println!("{}", version.lines().next().unwrap_or_default());
    let python = Command::new("python3")
        .arg("--version")
        .output()
        .expect(PREREQUISITES);
    let version = String::from_utf8_lossy(&python.stdout);
    let mut components = version
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .split('.');
    let major = components
        .next()
        .and_then(|number| number.parse::<u32>().ok());
    let minor = components
        .next()
        .and_then(|number| number.parse::<u32>().ok());
    assert!(
        python.status.success()
            && major == Some(3)
            && minor.is_some_and(|number| (11..=14).contains(&number)),
        "{PREREQUISITES}"
    );
    println!("{}", version.lines().next().unwrap_or_default());
}

pub(crate) fn require_subprocess_tools(command: &mut Command) {
    if cfg!(target_os = "linux") {
        command.env("CRABBER_TOOLS_REQUIRE_LOGIN_PROFILE", "1");
    }
    command
        .env("BACKGROUND_JOBS_REQUIRE_SHELL", "1")
        .env("PYTHON_REPL_REQUIRE_PYTHON", "1");
}

pub(crate) fn schedule_offline(workspace: &Path, gates: &mut Checks) {
    gates.cargo(
        workspace,
        &["run", "--quiet", "-p", "mount-standard", "--", "--check"],
    );
    gates.cargo(
        workspace,
        &[
            "run",
            "--quiet",
            "-p",
            "crabber-extensions",
            "--example",
            "protected_workspace",
        ],
    );
    schedule(workspace, gates);
}

pub(crate) fn schedule(workspace: &Path, gates: &mut Checks) {
    gates.cargo(
        workspace,
        &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--features",
            "crabber-extensions/live-opencode-go",
            "--",
            "-D",
            "warnings",
        ],
    );
    let mut docs = Command::new("cargo");
    docs.args(["doc", "--workspace", "--no-deps"])
        .current_dir(workspace)
        .env("RUSTDOCFLAGS", "-D warnings");
    gates.command(workspace, docs, "workspace rustdoc is warning-free");
    let mut live = Command::new("cargo");
    live.args([
        "test",
        "-p",
        "crabber-extensions",
        "--features",
        "live-opencode-go",
        "--test",
        "delegate_task_live",
        "--",
        "--ignored",
        "--nocapture",
    ])
    .current_dir(workspace)
    .env_remove("OPENCODE_GO_API_KEY")
    .env_remove("DELEGATE_TASK_REQUIRE_OPENCODE_GO");
    gates.owned(crate::parts::Owner::QualityContracts, |gates| {
        gates.captured(
            None,
            workspace,
            live,
            "both live suites prove credential-free skip",
            |output| {
                let message = "skipping live OpenCode Go suite";
                let skips = String::from_utf8_lossy(&output.stdout)
                    .matches(message)
                    .count()
                    + String::from_utf8_lossy(&output.stderr)
                        .matches(message)
                        .count();
                assert_eq!(
                    skips, 2,
                    "both live suites must prove their credential-free skip path"
                );
            },
        );
    });
}
