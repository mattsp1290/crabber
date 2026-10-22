//! Gates for the standard tools and native extensions.
use super::{run, run_env};
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

pub(crate) fn check_workspace_tests(workspace: &Path) {
    println!("$ cargo test --workspace");
    let mut command = Command::new("cargo");
    command.args(["test", "--workspace"]).current_dir(workspace);
    if cfg!(target_os = "linux") {
        command.env("CRABBER_TOOLS_REQUIRE_LOGIN_PROFILE", "1");
    }
    command
        .env("BACKGROUND_JOBS_REQUIRE_SHELL", "1")
        .env("PYTHON_REPL_REQUIRE_PYTHON", "1");
    let status = command.status().expect("launch workspace tests");
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

pub(crate) fn check(workspace: &Path) {
    run(
        workspace,
        &["run", "--quiet", "-p", "mount-standard", "--", "--check"],
    );
    run(
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
    run(
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
    run_env(
        workspace,
        &["doc", "--workspace", "--no-deps"],
        &[("RUSTDOCFLAGS", "-D warnings")],
        &[],
    );
    check_live_skip(workspace);
}

fn check_live_skip(workspace: &Path) {
    // This gate deliberately qualifies the credential-free branch even when a
    // host normally requires live-provider acceptance in its shell environment.
    let output = run_env(
        workspace,
        &[
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
        ],
        &[],
        &["OPENCODE_GO_API_KEY", "DELEGATE_TASK_REQUIRE_OPENCODE_GO"],
    );
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
}
