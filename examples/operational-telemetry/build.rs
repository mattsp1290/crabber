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
    println!("cargo:rerun-if-changed=build.rs");
}
