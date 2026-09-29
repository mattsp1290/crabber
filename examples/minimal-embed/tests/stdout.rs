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
