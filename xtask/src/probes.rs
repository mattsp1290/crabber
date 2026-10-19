//! Check standalone consumer source against this workspace without altering its
//! tracked Git pins. Published revision adoption remains a separate check.
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::SystemTime,
};

const PROBES: [&str; 3] = [
    "workspace-context-probe",
    "prompt-contribution-probe",
    "result-transform-probe",
];

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "crabber-local-probes-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create unique local-probe scratch directory");
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
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
    let scratch = Scratch::new();
    let local = workspace.join("crates/crabber").canonicalize().unwrap();
    for probe in PROBES {
        let original = workspace.join("testdata").join(probe);
        let destination = scratch.0.join(probe);
        fs::create_dir(&destination).unwrap();
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
        // Start with the workspace's dependency choices. Cargo may update this
        // disposable lock for the probe package and its additional dependencies.
        fs::copy(workspace.join("Cargo.lock"), destination.join("Cargo.lock")).unwrap();
        for directory in ["src", "tests"] {
            if original.join(directory).is_dir() {
                copy_sources(&original.join(directory), &destination.join(directory));
            }
        }
        println!(
            "$ cargo check --all-targets # {probe}: local API check; recorded Git pin {revision} is unchanged and not qualified here"
        );
        let status = Command::new("cargo")
            .args(["check", "--all-targets"])
            .current_dir(&destination)
            .status()
            .expect("launch local-probe check");
        assert!(status.success(), "local API check failed for {probe}");
    }
}
