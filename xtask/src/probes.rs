//! Check standalone consumer source against this workspace without altering its
//! tracked Git pins. Published revision adoption remains a separate check.
use sha2::Digest;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::lease::Lease;

const PROBES: [&str; 3] = [
    "workspace-context-probe",
    "prompt-contribution-probe",
    "result-transform-probe",
];

fn cache_root(workspace: &Path) -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| workspace.join("target"), PathBuf::from);
    let target = if target.is_absolute() {
        target
    } else {
        workspace.join(target)
    };
    let canonical = workspace
        .canonicalize()
        .expect("canonical checkout identity");
    let identity = sha2::Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    target
        .join("local-probes")
        .join(&format!("{identity:x}")[..32])
}

fn copy_sources(from: &Path, to: &Path) {
    fs::create_dir(to).expect("create local-probe source directory");
    for entry in fs::read_dir(from).expect("read local-probe sources") {
        let entry = entry.expect("read local-probe source entry");
        let kind = entry.file_type().expect("inspect local-probe source entry");
        let destination = to.join(entry.file_name());
        if kind.is_dir() {
            copy_sources(&entry.path(), &destination);
        } else {
            assert!(kind.is_file(), "local-probe sources must be regular files");
            fs::copy(entry.path(), destination).expect("copy local-probe source");
        }
    }
}

pub(crate) fn check(workspace: &Path) {
    let mut gates = crate::checks::Checks::default();
    schedule(workspace, &mut gates);
    let code = gates.execute(&crate::checks::Options::default());
    if code != 0 {
        std::process::exit(code);
    }
}

pub(crate) fn schedule(workspace: &Path, gates: &mut crate::checks::Checks) {
    let cache = cache_root(workspace);
    for probe in PROBES {
        let directory = cache.join(probe);
        let destination = directory.join("source");
        let original = workspace.join("testdata").join(probe);
        let lease = Lease::default();
        let locked = lease.clone();
        let prepared = destination.clone();
        let locked_directory = directory.clone();
        gates.internal(&format!("prepare-{probe}"), workspace, move |workspace| {
            locked.acquire(&locked_directory.join("probe.lock"));
            prepare(workspace, &original, &prepared);
        });
        let mut command = Command::new("cargo");
        command
            .args(["check", "--all-targets"])
            .current_dir(&destination)
            .env("CARGO_TARGET_DIR", directory.join("target"));
        gates.named_command(&format!("local-probe-{probe}"),&destination,command,"all consumer targets compile against current local sources; original Git pin unchanged");
        gates.internal(&format!("release-{probe}"), workspace, move |_| {
            lease.release();
        });
    }
}

fn prepare(workspace: &Path, original: &Path, destination: &Path) {
    let local = workspace.join("crates/crabber").canonicalize().unwrap();
    if destination.exists() {
        fs::remove_dir_all(destination).expect("remove stale generated probe source");
    }
    fs::create_dir(destination).expect("refresh generated probe source");
    let mut manifest: toml::Value =
        toml::from_str(&fs::read_to_string(original.join("Cargo.toml")).unwrap()).unwrap();
    let dependency = manifest
        .get_mut("dependencies")
        .and_then(toml::Value::as_table_mut)
        .and_then(|dependencies| dependencies.get_mut("crabber"))
        .and_then(toml::Value::as_table_mut)
        .expect("standalone Crabber Git dependency");
    assert_eq!(
        dependency.get("git").and_then(toml::Value::as_str),
        Some("ssh://git@github.com/mattsp1290/crabber.git")
    );
    let revision = dependency
        .get("rev")
        .and_then(toml::Value::as_str)
        .unwrap()
        .to_owned();
    assert!(revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()));
    dependency.remove("git");
    dependency.remove("rev");
    dependency.insert(
        "path".into(),
        toml::Value::String(local.to_str().unwrap().into()),
    );
    fs::write(
        destination.join("Cargo.toml"),
        toml::to_string(&manifest).unwrap(),
    )
    .unwrap();
    fs::copy(workspace.join("Cargo.lock"), destination.join("Cargo.lock")).unwrap();
    for directory in ["src", "tests"] {
        if original.join(directory).is_dir() {
            copy_sources(&original.join(directory), &destination.join(directory));
        }
    }
    println!("local API check: recorded Git pin {revision} is unchanged and not qualified here");
}
