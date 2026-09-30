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
    let output = Command::new(env!("CARGO_BIN_EXE_minimal-embed"))
        .args(["--extension", "native", "--wasm"])
        .output()
        .expect("run native and WASM demo");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("tool call settled"));
}
