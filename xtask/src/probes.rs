//! Check standalone consumer source against this workspace without altering its
//! tracked Git pins. Published revision adoption remains a separate check.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
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

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CacheOwners {
    version: u8,
    checkout: PathBuf,
    owners: BTreeMap<String, ConsumerOwner>,
}

#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ConsumerOwner {
    manifest: PathBuf,
    gate: String,
    receipt: PathBuf,
}

fn owner(probe: &str) -> ConsumerOwner {
    ConsumerOwner {
        manifest: Path::new(probe).join("source/Cargo.toml"),
        gate: format!("local-probe-{probe}"),
        receipt: Path::new(probe).join("compiler.json"),
    }
}

fn reject_symlinks(path: &Path) {
    for ancestor in path.ancestors() {
        if let Ok(metadata) = fs::symlink_metadata(ancestor) {
            assert!(
                !metadata.file_type().is_symlink(),
                "probe cache must not traverse symlinks"
            );
        }
    }
}

fn write_json(path: &Path, value: &impl Serialize) {
    let temporary = path.with_extension("tmp");
    reject_symlinks(&temporary);
    reject_symlinks(path);
    fs::write(&temporary, serde_json::to_vec_pretty(value).unwrap()).unwrap();
    fs::rename(temporary, path).unwrap();
}

fn retain_owner(workspace: &Path, cache: &Path, probe: &str) {
    let path = cache.join("owners.json");
    reject_symlinks(&path);
    let checkout = workspace.canonicalize().unwrap();
    let mut retained = if path.exists() {
        let retained: CacheOwners =
            serde_json::from_slice(&fs::read(&path).unwrap()).expect("valid retained probe owners");
        assert!(
            retained.version == 1 && retained.checkout == checkout,
            "probe cache ownership changed"
        );
        for (name, record) in &retained.owners {
            assert!(
                PROBES.contains(&name.as_str()) && *record == owner(name),
                "unknown retained probe owner"
            );
        }
        retained
    } else {
        CacheOwners {
            version: 1,
            checkout,
            owners: BTreeMap::new(),
        }
    };
    retained.owners.insert(probe.to_owned(), owner(probe));
    write_json(&path, &retained);
}

fn compiler_artifact(value: &Value) -> Value {
    let profile = &value["profile"];
    assert!(
        ["0", "1", "2", "3", "s", "z"].contains(&profile["opt_level"].as_str().unwrap()),
        "compiler optimization level"
    );
    let debug = &profile["debuginfo"];
    assert!(
        debug.as_u64().is_some_and(|v| v <= 2)
            || debug
                .as_str()
                .is_some_and(|v| ["line-directives-only", "line-tables-only"].contains(&v)),
        "compiler debug level"
    );
    for name in ["debug_assertions", "overflow_checks", "test"] {
        assert!(profile[name].is_boolean(), "compiler profile boolean");
    }
    for list in [&value["features"], &value["target"]["kind"]] {
        assert!(
            list.as_array()
                .is_some_and(|v| v.iter().all(Value::is_string)),
            "compiler string list"
        );
    }
    assert!(
        !value["target"]["kind"].as_array().unwrap().is_empty(),
        "compiler target kind required"
    );
    for text in [
        &value["package_id"],
        &value["target"]["name"],
        &value["target"]["src_path"],
    ] {
        assert!(text.is_string(), "compiler identity string");
    }
    assert!(value["fresh"].is_boolean(), "compiler freshness boolean");
    json!({
        "package_id": value["package_id"], "features": value["features"], "fresh": value["fresh"], "filenames": value["filenames"],
        "target": {"name":value["target"]["name"], "kind":value["target"]["kind"], "src_path":value["target"]["src_path"]},
        "profile": {"opt_level":profile["opt_level"], "debuginfo":profile["debuginfo"],
            "debug_assertions":profile["debug_assertions"], "overflow_checks":profile["overflow_checks"], "test":profile["test"]},
    })
}

fn record_compilation(cache: &Path, target: &Path, probe: &str, stdout: &[u8]) {
    let target = target.canonicalize().unwrap();
    let manifest = cache.join(owner(probe).manifest).canonicalize().unwrap();
    let mut artifacts = Vec::new();
    let mut finished = false;
    for value in serde_json::Deserializer::from_slice(stdout).into_iter::<Value>() {
        let value = value.expect("valid consumer compiler JSON");
        match value["reason"].as_str() {
            Some("compiler-artifact") => {
                assert!(!finished, "compiler artifact after build completion");
                let files = value["filenames"]
                    .as_array()
                    .expect("compiler artifact filenames");
                assert!(!files.is_empty(), "compiler artifact output required");
                for file in files {
                    let path = Path::new(file.as_str().unwrap());
                    reject_symlinks(path);
                    assert!(
                        path.starts_with(&target) && path.exists(),
                        "consumer artifact escaped or is missing from shared cache"
                    );
                }
                artifacts.push(compiler_artifact(&value));
            }
            Some("build-finished") => {
                assert!(
                    !finished && value["success"] == true,
                    "one successful consumer build required"
                );
                finished = true;
            }
            _ => {}
        }
    }
    assert!(
        finished
            && artifacts
                .iter()
                .any(|a| Path::new(a["target"]["src_path"].as_str().unwrap())
                    .starts_with(manifest.parent().unwrap())),
        "consumer root compiler artifact required"
    );
    write_json(
        &cache.join(owner(probe).receipt),
        &json!({
            "version": 1, "manifest": manifest, "target": target,
            "gate": owner(probe).gate,
            "manifest_sha256": format!("{:x}", sha2::Sha256::digest(fs::read(&manifest).unwrap())),
            "completed_unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64(),
            "artifacts": artifacts,
        }),
    );
}

pub(crate) fn schedule(workspace: &Path, gates: &mut crate::checks::Checks) {
    let cache = cache_root(workspace);
    let target = cache.join("shared-target");
    for probe in PROBES {
        let directory = cache.join(probe);
        let destination = directory.join("source");
        let original = workspace.join("testdata").join(probe);
        let source_lease = Lease::default();
        let build_lease = Lease::default();
        let locked_source = source_lease.clone();
        let locked_build = build_lease.clone();
        let prepared = destination.clone();
        let locked_directory = directory.clone();
        let locked_cache = cache.clone();
        let prepared_target = target.clone();
        gates.internal(&format!("prepare-{probe}"), workspace, move |workspace| {
            reject_symlinks(&locked_directory.join("probe.lock"));
            locked_source.acquire(&locked_directory.join("probe.lock"));
            reject_symlinks(&locked_cache.join("build.lock"));
            locked_build.acquire(&locked_cache.join("build.lock"));
            let legacy = locked_directory.join("target");
            reject_symlinks(&legacy);
            if legacy.exists() && legacy != prepared_target {
                assert!(legacy.is_dir(), "legacy probe target must be a directory");
                fs::remove_dir_all(legacy).expect("retire owned legacy probe target");
            }
            reject_symlinks(&prepared);
            prepare(workspace, &original, &prepared);
            extra_sources(workspace, &prepared, probe);
            reject_symlinks(&prepared_target);
            fs::create_dir_all(&prepared_target).unwrap();
            retain_owner(workspace, &locked_cache, probe);
        });
        let mut command = Command::new("cargo");
        command
            .args(["check", "--all-targets", "--target-dir"])
            .arg(&target)
            .args(["--message-format", "json-render-diagnostics"])
            .current_dir(&destination)
            .env("CARGO_BUILD_BUILD_DIR", &target);
        let output_cache = cache.clone();
        let output_target = target.clone();
        gates.captured(Some(&format!("local-probe-{probe}")), &destination, command,
            "all consumer targets compile against current local sources; original Git pin unchanged",
            move |output| {
                eprint!("{}", String::from_utf8_lossy(&output.stderr));
                record_compilation(&output_cache, &output_target, probe, &output.stdout);
                println!("local API checked: {probe}");
            });
        gates.internal(&format!("release-{probe}"), workspace, move |_| {
            build_lease.release();
            source_lease.release();
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
