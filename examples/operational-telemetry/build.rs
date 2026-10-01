use std::{path::PathBuf, process::Command};
fn git(root: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "Git identity unavailable");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
fn main() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../..")
        .canonicalize()
        .unwrap();
    let sha = git(&root, &["rev-parse", "HEAD"]);
    println!("cargo:rustc-env=CRABBER_BUILD_SHA={sha}");
    println!("cargo:rustc-env=CRABBER_BUILD_ROOT={}", root.display());
    // Worktree HEAD, loose branch refs and packed refs live in different Git dirs.
    for name in ["HEAD", "packed-refs"] {
        let path = git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-path", name],
        );
        println!("cargo:rerun-if-changed={path}");
    }
    let reference = git(&root, &["rev-parse", "--symbolic-full-name", "HEAD"]);
    if reference.starts_with("refs/") {
        let path = git(
            &root,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                &reference,
            ],
        );
        println!("cargo:rerun-if-changed={path}");
    }
    // Cargo fingerprints are checkout-specific, but this profile executable is
    // shared. Another worktree can overwrite it while our fingerprint stays
    // fresh. Observe the actual output so restoring this checkout refreshes its
    // stamp and executable rather than running the other checkout's artifact.
    let out = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let profile = out
        .ancestors()
        .nth(3)
        .expect("Cargo profile/build/package/out");
    let executable = if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "windows" {
        "operational-telemetry.exe"
    } else {
        "operational-telemetry"
    };
    println!(
        "cargo:rerun-if-changed={}",
        profile.join(executable).display()
    );
    println!("cargo:rerun-if-changed=build.rs");
}
