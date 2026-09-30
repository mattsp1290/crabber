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
