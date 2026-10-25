//! Real normal executables observed from the existing workspace producers.
use crate::{
    checks::Checks,
    parts::{Owner, Part},
};
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

pub(crate) fn schedule(gates: &mut Checks, directory: &Path, features: &str) {
    gates.preparation(Part::QualityArtifacts, |gates| {
        examples(gates, directory, features);
    });
    gates.owned(Owner::QualityArtifacts, |gates| {
        let artifacts_only = gates.is_part(Part::QualityArtifacts);
        let mut command = Command::new("cargo");
        command
            .current_dir(crate::workspace())
            .args(["build", "--workspace", "--bins"]);
        if !artifacts_only {
            command.arg("--tests");
        }
        command.args(["--example", "protected_workspace", "--features", features]);
        if artifacts_only {
            command.args(["--message-format", "json-render-diagnostics"]);
            let journal = directory.join("normal-output-artifacts.jsonl");
            let output = directory.to_owned();
            gates.named_captured(
                "normal-output-build",
                crate::workspace(),
                command,
                "actual normal programs and the protected example",
                journal.clone(),
                move |_| finish(&output, &journal),
            );
        } else {
            gates.named_command(
                "normal-output-build",
                crate::workspace(),
                command,
                "Cargo exit status",
            );
        }
    });
}

fn examples(gates: &mut Checks, directory: &Path, features: &str) {
    let mut command = Command::new("cargo");
    command.current_dir(crate::workspace()).args([
        "build",
        "--workspace",
        "--examples",
        "--profile",
        "test",
        "--features",
        features,
        "--message-format",
        "json-render-diagnostics",
    ]);
    let journal = directory.join("normal-example-artifacts.jsonl");
    let output = directory.to_owned();
    gates.named_captured(
        "prepare-normal-examples",
        crate::workspace(),
        command,
        "all declared normal examples; zero semantic gate credit",
        journal.clone(),
        move |_| {
            let examples = observed(&output, &journal, "example");
            assert_eq!(
                examples.keys().collect::<Vec<_>>(),
                declared(&output, "example").keys().collect::<Vec<_>>()
            );
            fs::write(
                output.join("prepared-normal-examples.json"),
                serde_json::to_vec_pretty(&examples).unwrap(),
            )
            .unwrap();
        },
    );
}

fn read(path: impl AsRef<Path>) -> Value {
    serde_json::from_slice(&fs::read(path).expect("read actual output evidence"))
        .expect("actual output JSON")
}

fn key(package: &str, target: &Value) -> String {
    format!("{package}::{}", target["name"].as_str().unwrap())
}

fn declared(directory: &Path, kind: &str) -> BTreeMap<String, Value> {
    let metadata = read(directory.join("cargo-metadata.json"));
    let mut result = BTreeMap::new();
    for package in metadata["packages"].as_array().unwrap() {
        if !metadata["workspace_members"]
            .as_array()
            .unwrap()
            .contains(&package["id"])
        {
            continue;
        }
        for target in package["targets"].as_array().unwrap() {
            if target["kind"] == json!([kind]) {
                result.insert(key(package["id"].as_str().unwrap(), target), target.clone());
            }
        }
    }
    result
}

pub(crate) fn journal(directory: &Path, path: &Path) -> Vec<Value> {
    let rows: Vec<Value> = fs::read_to_string(path)
        .expect("retained Cargo artifact journal")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows[0]["kind"], "header");
    assert_eq!(rows.last().unwrap()["kind"], "reader-finished");
    assert_eq!(rows.last().unwrap()["invalid"], false);
    let metadata = fs::read(directory.join("cargo-metadata.json")).unwrap();
    assert_eq!(
        rows[0]["metadata_sha256"],
        format!("{:x}", sha2::Sha256::digest(metadata))
    );
    rows
}

fn observed(directory: &Path, path: &Path, kind: &str) -> BTreeMap<String, Value> {
    let expected = declared(directory, kind);
    let mut actual = BTreeMap::new();
    for mut row in journal(directory, path) {
        if row["kind"] != "artifact"
            || row["target"]["kind"] != json!([kind])
            || row["profile"]["test"] != false
        {
            continue;
        }
        let id = key(row["package_id"].as_str().unwrap(), &row["target"]);
        let target = expected.get(&id).expect("only declared workspace targets");
        assert_eq!(row["target"]["src_path"], target["src_path"]);
        assert!(row["profile"]["opt_level"].is_string());
        assert!(!row["profile"]["debuginfo"].is_null());
        assert_eq!(row["profile"]["debug_assertions"], true);
        assert_eq!(row["profile"]["overflow_checks"], true);
        let executable = PathBuf::from(row["executable"].as_str().unwrap());
        let info = fs::symlink_metadata(&executable).unwrap();
        assert!(executable.is_absolute() && info.file_type().is_file() && info.len() > 0);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_ne!(
                info.permissions().mode() & 0o111,
                0,
                "normal artifact must be executable"
            );
        }
        let name = target["name"].as_str().unwrap();
        let program = if kind == "example" {
            format!("examples/{name}")
        } else {
            name.to_owned()
        };
        assert_eq!(
            executable.as_os_str(),
            crate::complete::binary_command(&program).get_program()
        );
        row["sha256"] = json!(hash(&executable));
        assert!(
            actual.insert(id, row).is_none(),
            "duplicate normal artifact"
        );
    }
    actual
}

fn hash(path: &Path) -> String {
    let mut input = fs::File::open(path).unwrap();
    let mut digest = sha2::Sha256::new();
    let mut buffer = vec![0; 65_536];
    loop {
        let count = input.read(&mut buffer).unwrap();
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    format!("{:x}", digest.finalize())
}

fn finish(directory: &Path, path: &Path) {
    let bins = observed(directory, path, "bin");
    assert_eq!(
        bins.keys().collect::<Vec<_>>(),
        declared(directory, "bin").keys().collect::<Vec<_>>()
    );
    let mut examples: BTreeMap<String, Value> =
        serde_json::from_slice(&fs::read(directory.join("prepared-normal-examples.json")).unwrap())
            .unwrap();
    let produced_examples = observed(directory, path, "example");
    assert!(
        produced_examples
            .values()
            .any(|row| row["target"]["name"] == "protected_workspace")
    );
    examples.extend(produced_examples);
    assert_eq!(
        examples.keys().collect::<Vec<_>>(),
        declared(directory, "example").keys().collect::<Vec<_>>()
    );
    for row in bins.values().chain(examples.values()) {
        assert_eq!(
            row["sha256"],
            hash(Path::new(row["executable"].as_str().unwrap()))
        );
    }
    let xtask = bins
        .values()
        .find(|row| row["target"]["name"] == "xtask")
        .unwrap();
    for feature in ["postgres-check", "session-fixtures"] {
        assert!(
            xtask["features"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == feature)
        );
    }
    let observations = journal(directory, path);
    let header = &observations[0];
    fs::write(directory.join("normal-outputs.json"), serde_json::to_vec_pretty(&json!({
        "source": header["source"], "metadata_sha256": header["metadata_sha256"],
        "normal_bins": bins, "normal_examples": examples,
        "scope": "actual normal executable observations; no canonical transitive-context equivalence claim",
    })).unwrap()).unwrap();
}

pub(crate) fn migration_owner(gates: &mut Checks, directory: &Path) {
    let mut command = Command::new("cargo");
    command.current_dir(crate::workspace()).args([
        "build",
        "-p",
        "xtask",
        "--features",
        "postgres-check,session-fixtures",
        "--message-format",
        "json-render-diagnostics",
    ]);
    let output = directory.to_owned();
    let path = directory.join("library-migration-owner-artifacts.jsonl");
    gates.named_captured("native-library-migration-owner", crate::workspace(), command,
        "timed normal enabled xtask producer immediately before owned template migration",
        path.clone(), move |_| {
            let rows = journal(&output, &path);
            assert_eq!(rows.last().unwrap()["truncated_tail"], false);
            assert_eq!(rows[0]["source"], crate::timings::source_identity());
            let artifacts: Vec<_> = rows.iter().filter(|row| row["kind"] == "artifact").collect();
            assert_eq!(artifacts.len(), 1, "narrow owner must produce only its normal executable");
            let bins = observed(&output, &path, "bin");
            assert_eq!(bins.len(), 1);
            let owner = bins.values().next().unwrap();
            assert_eq!(owner["target"]["name"], "xtask");
            assert_eq!(owner["profile"]["test"], false);
            assert_eq!(owner["profile"]["opt_level"], "0");
            crate::native_artifacts::validate_debuginfo(&owner["profile"]["debuginfo"]);
            let features: std::collections::BTreeSet<_> = owner["features"].as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();
            assert_eq!(features, ["postgres-check", "session-fixtures"].into());
            fs::write(output.join("library-migration-owner.json"), serde_json::to_vec_pretty(&json!({
                "source": rows[0]["source"], "metadata_sha256": rows[0]["metadata_sha256"],
                "argv": rows[0]["argv"], "artifact": owner,
                "scope": "normal enabled xtask preparation; no full dependency-context equivalence claim",
            })).unwrap()).unwrap();
        });
}
