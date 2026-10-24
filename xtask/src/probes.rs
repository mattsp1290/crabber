//! Check standalone consumer source against this workspace without altering its
//! tracked Git pins. Published revision adoption remains a separate check.
use sha2::Digest;
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::lease::Lease;

const CRABBER_REMOTE: &str = "ssh://git@github.com/mattsp1290/crabber.git";
const PROBES: [&str; 5] = [
    "workspace-context-probe",
    "prompt-contribution-probe",
    "result-transform-probe",
    "standard-tools-consumer",
    "extensions-consumer",
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

fn copy_source(from: &Path, to: &Path) {
    let kind = fs::symlink_metadata(from)
        .expect("inspect local-probe source")
        .file_type();
    if kind.is_dir() {
        fs::create_dir_all(to).expect("create local-probe source directory");
        for entry in fs::read_dir(from).expect("read local-probe sources") {
            let entry = entry.expect("read local-probe source entry");
            copy_source(&entry.path(), &to.join(entry.file_name()));
        }
    } else {
        assert!(kind.is_file(), "local-probe sources must be regular files");
        fs::create_dir_all(to.parent().unwrap()).expect("create local-probe source parent");
        fs::copy(from, to).expect("copy local-probe source");
    }
}

fn local_dependencies(workspace: &Path, manifest: &mut toml::Value) -> String {
    let dependencies = manifest
        .get_mut("dependencies")
        .and_then(toml::Value::as_table_mut)
        .expect("standalone dependencies");
    let mut revisions = BTreeSet::new();
    for (name, dependency) in dependencies {
        let Some(dependency) = dependency.as_table_mut() else {
            continue;
        };
        if dependency.get("git").and_then(toml::Value::as_str) != Some(CRABBER_REMOTE) {
            continue;
        }
        let revision = dependency
            .get("rev")
            .and_then(toml::Value::as_str)
            .expect("immutable Crabber revision")
            .to_owned();
        assert!(
            revision == "CRABBER_REV"
                || (revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit())),
            "standalone Crabber revisions must be full SHAs or CRABBER_REV"
        );
        revisions.insert(revision);
        let local = workspace.join("crates").join(name).canonicalize().unwrap();
        dependency.remove("git");
        dependency.remove("rev");
        dependency.insert(
            "path".into(),
            toml::Value::String(local.to_str().unwrap().into()),
        );
    }
    assert!(
        !revisions.is_empty(),
        "standalone Crabber Git dependency required"
    );
    revisions.into_iter().collect::<Vec<_>>().join(", ")
}

fn extra_sources(workspace: &Path, destination: &Path, probe: &str) {
    match probe {
        "standard-tools-consumer" => {
            for name in ["probe.rs", "contexts.rs"] {
                copy_source(
                    &workspace.join("examples/mount-standard/src").join(name),
                    &destination.join("src").join(name),
                );
            }
        }
        "extensions-consumer" => {
            for name in [
                "support",
                "contextual",
                "contextual.rs",
                "ask_user",
                "ask_user.rs",
                "delegate_task",
                "delegate_task.rs",
                "cleanup",
                "cleanup.rs",
                "runtime",
                "runtime.rs",
                "session_invocation_contract.rs",
            ] {
                copy_source(
                    &workspace.join("crates/crabber-extensions/tests").join(name),
                    &destination.join("tests").join(name),
                );
            }
        }
        _ => {}
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
            extra_sources(workspace, &prepared, probe);
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
    if destination.exists() {
        fs::remove_dir_all(destination).expect("remove stale generated probe source");
    }
    fs::create_dir(destination).expect("refresh generated probe source");
    let mut manifest: toml::Value =
        toml::from_str(&fs::read_to_string(original.join("Cargo.toml")).unwrap()).unwrap();
    let revision = local_dependencies(workspace, &mut manifest);
    fs::write(
        destination.join("Cargo.toml"),
        toml::to_string(&manifest).unwrap(),
    )
    .unwrap();
    fs::copy(workspace.join("Cargo.lock"), destination.join("Cargo.lock")).unwrap();
    for directory in ["src", "tests"] {
        if original.join(directory).is_dir() {
            copy_source(&original.join(directory), &destination.join(directory));
        }
    }
    println!("local API check: recorded Git pin {revision} is unchanged and not qualified here");
}
