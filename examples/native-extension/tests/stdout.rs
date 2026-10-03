#[test]
fn demonstrates_native_extension() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_native-extension"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    for expected in [
        "runtime tool registered",
        "guard denied rm -rf",
        "tool workspace: default at .",
        "redacted result in request",
        "prompt section in request",
        "observed event",
    ] {
        assert!(stdout.contains(expected), "missing {expected}: {stdout}");
    }
    assert!(!stdout.contains("runtime tool executed: \"rm -rf"));
}
