use std::{env, path::PathBuf, process::Command};

fn git(args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .output()
        .expect("git is required to build the example");
    assert!(output.status.success(), "git {args:?} failed");
    String::from_utf8(output.stdout)
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

fn main() {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("Cargo sets manifest dir"));
    let repo = manifest.join("../..");
    env::set_current_dir(repo).expect("enter repository");

    for path in [
        git(&["rev-parse", "--git-path", "HEAD"]),
        git(&["rev-parse", "--git-path", "packed-refs"]),
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    if let Ok(output) = Command::new("git")
        .args(["symbolic-ref", "-q", "HEAD"])
        .output()
        && output.status.success()
    {
        let reference = String::from_utf8(output.stdout).expect("git ref is UTF-8");
        println!(
            "cargo:rerun-if-changed={}",
            git(&["rev-parse", "--git-path", reference.trim()])
        );
    }
    println!(
        "cargo:rustc-env=CRABBER_GIT_SHA={}",
        git(&["rev-parse", "--short=7", "HEAD"])
    );
}
