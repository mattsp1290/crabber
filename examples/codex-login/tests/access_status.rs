#[test]
fn unavailable_access_file_needs_no_credentials_or_network() {
    let path =
        std::env::temp_dir().join(format!("crabber-access-status-{}.json", std::process::id()));
    std::fs::write(
        &path,
        r#"{"schema_version":1,"state":"unavailable","generation":1}"#,
    )
    .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_codex-login"))
        .args(["access-status", "--file"])
        .arg(&path)
        .output()
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "unavailable"
    );
    let missing = std::process::Command::new(env!("CARGO_BIN_EXE_codex-login"))
        .args(["access-status", "--file"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert_eq!(
        String::from_utf8(missing.stderr).unwrap().trim(),
        "Error: Store"
    );
}
