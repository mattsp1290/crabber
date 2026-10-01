mod journey;
use std::{path::PathBuf, process::Command};
fn identity() {
    let current = std::env::current_dir().unwrap();
    let git = |args: &[&str]| {
        let output = Command::new("git")
            .arg("-C")
            .arg(&current)
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success(), "run inside the built checkout");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let root = PathBuf::from(git(&["rev-parse", "--show-toplevel"]))
        .canonicalize()
        .unwrap();
    assert_eq!(
        root,
        PathBuf::from(env!("CRABBER_BUILD_ROOT")),
        "stale shared target: source root mismatch"
    );
    assert_eq!(
        git(&["rev-parse", "HEAD"]),
        env!("CRABBER_BUILD_SHA"),
        "stale shared target: compiled SHA mismatch"
    );
    assert!(
        git(&["status", "--porcelain"]).is_empty(),
        "identity proof requires committed clean sources"
    );
    println!(
        "build_sha={} source_root={} provider=fake-scripted tool=echo-native store=MemoryStore schema=in-memory units=milliseconds",
        env!("CRABBER_BUILD_SHA"),
        root.display()
    );
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    assert_eq!(std::env::args().skip(1).collect::<Vec<_>>(), ["--check"]);
    identity();
    journey::run().await;
}
