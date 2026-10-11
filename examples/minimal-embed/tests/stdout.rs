use std::process::Command;

#[test]
fn demo_streams_text_and_tool_settlement() {
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .output()
        .expect("run minimal-embed");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    assert!(stdout.starts_with(&format!("crabber {} (git ", env!("CARGO_PKG_VERSION"))));
    assert!(stdout.contains("Checking tool..."));
    assert!(stdout.contains("tool call settled"));
    assert!(stdout.contains("Done."));
}

#[test]
fn interrupt_then_continue_same_stored_session() {
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .arg("--interrupt-after-first-delta")
        .output()
        .expect("run minimal-embed interrupt demo");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Checking tool..."));
    assert!(stdout.contains(": Interrupted"));
    assert!(stdout.contains("resumed from "));
    assert!(stdout.contains(": Completed"));
}

#[test]
fn resume_flag_looks_up_run_in_configured_store() {
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .args(["--resume", "missing-run"])
        .output()
        .expect("run minimal-embed resume path");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("run not found in configured store"));
}

#[test]
fn native_extension_and_wasm_flags_run_together() {
    let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .current_dir(workspace)
        .args([
            "--store",
            "memory",
            "--provider",
            "fake",
            "--extension",
            "native",
            "--wasm",
            "fixtures/wasm/echo-tool.wasm",
        ])
        .output()
        .expect("run native and WASM demo");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("store=memory provider=fake"), "{stdout}");
    assert!(
        stdout.contains("native guard denied native-echo"),
        "{stdout}"
    );
    assert!(
        stdout.contains("native tool native-echo settled: failed"),
        "{stdout}"
    );
    assert!(
        stdout.contains("WASM tool echo settled: completed"),
        "{stdout}"
    );
    assert!(stdout.contains("status=Completed"), "{stdout}");
    let session = stdout
        .lines()
        .find_map(|line| {
            line.strip_prefix("run=").and_then(|line| {
                line.split_whitespace()
                    .find_map(|field| field.strip_prefix("session="))
            })
        })
        .expect("run session id");
    assert!(
        stdout.contains(&format!("listed session={session} messages=")),
        "{stdout}"
    );
}

#[test]
fn wasm_flag_uses_the_supplied_path() {
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .args(["--wasm", "fixtures/wasm/missing-component.wasm"])
        .output()
        .expect("run with missing WASM path");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No such file") || stderr.contains("not found"),
        "{stderr}"
    );
}

async fn sqlite_output(command: Command) -> std::process::Output {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        tokio::process::Command::from(command)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("SQLite example process timed out")
    .expect("run SQLite minimal-embed")
}

#[tokio::test]
async fn sqlite_interrupt_then_fresh_process_continues_session() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let mut first = Command::new(env!("CARGO_BIN_EXE_minimal-embed"));
    first
        .args(["--store", "sqlite", "--interrupt-after-first-delta"])
        .env("CRABBER_SQLITE_PATH", &path);
    let first = sqlite_output(first).await;
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let stdout = String::from_utf8(first.stdout).unwrap();
    assert!(stdout.contains("store=sqlite"), "{stdout}");
    assert!(stdout.contains(": Interrupted"), "{stdout}");
    assert!(!stdout.contains("resumed from"), "{stdout}");
    let run_id = stdout
        .lines()
        .find_map(|line| line.strip_prefix("interrupted run "))
        .and_then(|line| line.strip_suffix(": Interrupted"))
        .expect("interrupted run ID");
    let mut second = Command::new(env!("CARGO_BIN_EXE_minimal-embed"));
    second
        .args(["--store", "sqlite", "--resume", run_id])
        .env("CRABBER_SQLITE_PATH", &path);
    let second = sqlite_output(second).await;
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let stdout = String::from_utf8(second.stdout).unwrap();
    assert!(stdout.contains("store=sqlite"), "{stdout}");
    let prefix = format!("resumed from {run_id} as ");
    let new_run_id = stdout
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .and_then(|line| line.strip_suffix(": Completed"))
        .expect("completed continuation ID");
    assert_ne!(new_run_id, run_id);
    assert!(path.is_file());

    // An inherited host setting must never select a developer's real file.
    let before = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    let mut missing = Command::new(env!("CARGO_BIN_EXE_minimal-embed"));
    missing
        .args(["--store", "sqlite"])
        .env_remove("CRABBER_SQLITE_PATH")
        .current_dir(directory.path());
    let missing = sqlite_output(missing).await;
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("CRABBER_SQLITE_PATH is required"));
    let after = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(before, after, "missing path creates no file");
}
