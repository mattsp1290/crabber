use std::process::Command;
fn main() {
    // HEAD is resolved by Git for worktrees too. Track the ref and working tree
    // files so changing commits rebuilds the embedded identity.
    let reference = Command::new("git")
        .args(["symbolic-ref", "HEAD"])
        .output()
        .unwrap();
    let reference = String::from_utf8(reference.stdout).unwrap();
    for argument in ["HEAD", reference.trim()] {
        let path = Command::new("git")
            .args(["rev-parse", "--git-path", argument])
            .output()
            .unwrap();
        println!(
            "cargo:rerun-if-changed={}",
            String::from_utf8(path.stdout).unwrap().trim()
        );
    }
    println!("cargo:rerun-if-changed=src/main.rs");
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(output.status.success());
    println!(
        "cargo:rustc-env=CRABBER_COMPILED_SHA={}",
        String::from_utf8(output.stdout).unwrap().trim()
    );
}
