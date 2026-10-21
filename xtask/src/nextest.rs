//! Pinned offline process scheduling; Cargo retains explicit doctest ownership.

use std::{path::Path, process::Command};

use crate::checks::Checks;

pub(crate) fn version_gate(workspace: &Path, gates: &mut Checks) {
    let mut command = Command::new("cargo");
    command
        .args(["nextest", "--version"])
        .current_dir(workspace);
    gates.captured(
        workspace,
        command,
        "repository-pinned cargo-nextest version",
        |output| {
            let pin: serde_json::Value =
                serde_json::from_str(include_str!("../../.config/test-tools.json")).unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert_eq!(
                stdout.split_whitespace().nth(1),
                pin["nextest"]["version"].as_str(),
                "install the pinned runner with python3 -I -S .github/scripts/install-nextest.py target/test-tools and add target/test-tools to PATH"
            );
        },
    );
}

pub(crate) fn schedule(workspace: &Path, gates: &mut Checks, arguments: &[&str]) {
    let mut command = Command::new("cargo");
    command
        .args([
            "nextest",
            "run",
            "--profile",
            "default",
            "--retries",
            "0",
            "--no-fail-fast",
        ])
        .args(&arguments[1..])
        .current_dir(workspace);
    gates.command(workspace, command, "nextest exit status; retries disabled");
    // Explicit target selectors suppress Cargo's default doctests. Preserve
    // exactly the invocations that previously included them.
    if !arguments
        .iter()
        .any(|arg| matches!(*arg, "--test" | "--lib" | "--bin" | "--example" | "--tests"))
    {
        let mut docs = Command::new("cargo");
        docs.args(arguments).arg("--doc").current_dir(workspace);
        gates.command(workspace, docs, "Cargo doctest exit status");
    }
}

/// Exercise a real failing nextest test through the same aggregate executor.
/// This is an explicit tool qualification, not an ignored workspace test.
pub(crate) fn qualify_failure(workspace: &Path) -> i32 {
    use std::{fs, time::SystemTime};
    let target =
        std::env::var_os("CARGO_TARGET_DIR").map_or_else(|| workspace.join("target"), Into::into);
    let target = if target.is_absolute() {
        target
    } else {
        workspace.join(target)
    };
    let nonce = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = target.join(format!(
        "test-performance/nextest-failure-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(directory.join("src")).expect("create failing runner fixture");
    fs::write(
        directory.join("Cargo.toml"),
        "[package]\nname='runner-failure-fixture'\nversion='0.0.0'\nedition='2024'\n[workspace]\n",
    )
    .unwrap();
    fs::write(
        directory.join("src/lib.rs"),
        "#[test] fn intentional_failure() { panic!(\"qualification failure\"); }\n#[test] fn positive_control() {}\n",
    )
    .unwrap();
    // This isolated workspace has no production packages or WASM binaries for
    // the repository's package/binary selectors to resolve against.
    let config = directory.join("nextest.toml");
    fs::write(
        &config,
        "[profile.default]\ntest-threads=2\nretries=0\nfail-fast=false\n",
    )
    .unwrap();
    let mut checks = Checks::offline();
    version_gate(workspace, &mut checks);
    let mut failure = Command::new("cargo");
    failure
        .args([
            "nextest",
            "run",
            "--retries",
            "0",
            "--no-fail-fast",
            "--config-file",
        ])
        .arg(config)
        .current_dir(&directory)
        .env("CARGO_TARGET_DIR", directory.join("target"));
    checks.command(&directory, failure, "deliberate real nextest test failure");
    checks.internal("must-be-cancelled", workspace, |_| {
        panic!("executor continued after a failed nextest test");
    });
    let report = directory.join("timings.json");
    let code = checks.execute(&crate::checks::Options {
        list: false,
        timings: Some(report.clone()),
    });
    let recorded: serde_json::Value = serde_json::from_slice(&fs::read(report).unwrap()).unwrap();
    // 100 is nextest's test-failure code; build/tool errors must not qualify.
    if code != 100
        || recorded["status"] != "failed"
        || recorded["gates"][1]["exit_code"] != 100
        || recorded["gates"][2]["status"] != "cancelled"
    {
        eprintln!("nextest failure propagation did not qualify");
        return 1;
    }
    println!("real nextest failure propagated as 100; later gate cancelled; report retained");
    0
}
