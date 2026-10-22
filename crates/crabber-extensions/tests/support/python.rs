//! Strict, host-provisioned Python fixture discovery.
use std::path::PathBuf;
pub fn python() -> Option<PathBuf> {
    static FOUND: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();
    FOUND.get_or_init(discover).clone()
}

fn discover() -> Option<PathBuf> {
    let found = std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths).find_map(|path| {
            let candidate = path.join("python3");
            std::process::Command::new(&candidate)
                .args(["-c", "import sys; print(sys.executable)"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| {
                    PathBuf::from(String::from_utf8(output.stdout).ok()?.trim())
                        .canonicalize()
                        .ok()
                })
        })
    });
    if let Some(path) = &found {
        if std::env::var("PYTHON_REPL_REQUIRE_PYTHON").as_deref() == Ok("1") {
            assert!(
                std::process::Command::new(path)
                    .args([
                        "-c",
                        "import sys; assert (3,11) <= sys.version_info[:2] <= (3,14)"
                    ])
                    .status()
                    .unwrap()
                    .success(),
                "required Python version outside 3.11–3.14"
            );
        }
    } else {
        assert_ne!(
            std::env::var("BACKGROUND_JOBS_REQUIRE_SHELL").as_deref(),
            Ok("1"),
            "required python3 absent"
        );
        assert_ne!(
            std::env::var("PYTHON_REPL_REQUIRE_PYTHON").as_deref(),
            Ok("1"),
            "required python3 absent"
        );
        eprintln!("process fixtures: python3 absent, skipping");
    }
    found
}
