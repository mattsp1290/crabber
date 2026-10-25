//! Admission of actual Cargo and pinned Nextest evidence for library workers.
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

type Cases = BTreeMap<(String, String), usize>;

fn read(path: impl AsRef<Path>) -> Value {
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}

fn target_key(package: &Value, target: &Value) -> (String, String) {
    (
        package.as_str().unwrap().to_owned(),
        target["name"].as_str().unwrap().to_owned(),
    )
}

pub(crate) fn admit_build(directory: &Path, binaries: &Value, stream: &[u8]) {
    let metadata = read(directory.join("cargo-metadata.json"));
    let members = metadata["workspace_members"].as_array().unwrap();
    let mut declared = BTreeMap::new();
    for package in metadata["packages"].as_array().unwrap() {
        if !members.contains(&package["id"]) {
            continue;
        }
        for target in package["targets"].as_array().unwrap() {
            if target["test"] == true && library(&target["kind"]) {
                let key = target_key(&package["id"], target);
                assert!(declared.insert(key, (package, target)).is_none());
            }
        }
    }
    assert!(!declared.is_empty(), "no declared library targets");
    let mut actual = BTreeMap::new();
    for row in serde_json::Deserializer::from_slice(stream).into_iter::<Value>() {
        let row = row.unwrap();
        if row["reason"] != "compiler-artifact" || row["executable"].is_null() {
            continue;
        }
        assert!(
            members.contains(&row["package_id"]),
            "foreign executable artifact"
        );
        assert_eq!(
            row["profile"]["test"], true,
            "normal executable in library build"
        );
        assert!(
            library(&row["target"]["kind"]),
            "nonlibrary executable in library build"
        );
        let key = target_key(&row["package_id"], &row["target"]);
        let (package, target) = declared.get(&key).expect("undeclared library target");
        assert_eq!(row["target"]["src_path"], target["src_path"]);
        assert_eq!(row["target"]["kind"], target["kind"]);
        let node = metadata["resolve"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|node| node["id"] == row["package_id"])
            .unwrap();
        let features = |value: &Value| {
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect::<BTreeSet<_>>()
        };
        assert_eq!(features(&row["features"]), features(&node["features"]));
        assert_eq!(
            row["profile"]["opt_level"],
            if package["name"] == "crabber-core" {
                "1"
            } else {
                "0"
            }
        );
        assert_eq!(row["profile"]["debuginfo"], 0);
        assert_eq!(row["profile"]["debug_assertions"], true);
        assert_eq!(row["profile"]["overflow_checks"], true);
        assert!(
            actual.insert(key, row).is_none(),
            "duplicate library executable"
        );
    }
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        declared.keys().collect::<Vec<_>>(),
        "library targets missing"
    );
    let mut listed = BTreeSet::new();
    for (id, binary) in binaries["rust-binaries"].as_object().unwrap() {
        assert_eq!(binary["binary-id"], id.as_str());
        assert_eq!(binary["build-platform"], "target");
        assert!(binary["kind"] == "lib" || binary["kind"] == "proc-macro");
        let key = (
            binary["package-id"].as_str().unwrap().to_owned(),
            binary["binary-name"].as_str().unwrap().to_owned(),
        );
        let artifact = actual
            .get(&key)
            .expect("binary outside declared library outputs");
        assert_eq!(binary["binary-path"], artifact["executable"]);
        assert!(listed.insert(key), "duplicate listed binary");
    }
    assert_eq!(listed, actual.keys().cloned().collect());
    let rows =
        crate::normal_outputs::journal(directory, &directory.join("library-artifacts.jsonl"));
    assert_eq!(rows[0]["source"], crate::timings::source_identity());
    assert_eq!(rows.last().unwrap()["truncated_tail"], false);
    fs::write(directory.join("library-targets.json"), serde_json::to_vec_pretty(&json!({
        "source": rows[0]["source"], "metadata_sha256": rows[0]["metadata_sha256"],
        "declared_target_count": declared.len(), "artifacts": actual.into_values().collect::<Vec<_>>(),
        "scope": "all workspace testable libraries and proc-macros; no integration or binary harnesses",
    })).unwrap()).unwrap();
}

fn library(kind: &Value) -> bool {
    kind == &json!(["lib"]) || kind == &json!(["proc-macro"])
}

pub(crate) fn admit_manifest(directory: &Path, manifest: &Value) {
    let binaries = read(directory.join("library-binaries.json"));
    let declared = binaries["rust-binaries"].as_object().unwrap();
    let suites = manifest["rust-suites"].as_object().unwrap();
    assert_eq!(
        suites.keys().collect::<Vec<_>>(),
        declared.keys().collect::<Vec<_>>()
    );
    for (id, suite) in suites {
        assert_eq!(suite["status"], "listed");
        for field in [
            "binary-id",
            "binary-name",
            "binary-path",
            "build-platform",
            "kind",
            "package-id",
        ] {
            assert_eq!(suite[field], declared[id][field]);
        }
        for case in suite["testcases"].as_object().unwrap().values() {
            assert_eq!(
                case["filter-match"]["status"], "matches",
                "unpartitioned case excluded"
            );
            assert!(case["ignored"].is_boolean());
        }
    }
    assert!(!selected(manifest).is_empty());
    let count: usize = suites
        .values()
        .map(|suite| suite["testcases"].as_object().unwrap().len())
        .sum();
    assert_eq!(manifest["test-count"].as_u64().unwrap() as usize, count);
    assert_eq!(
        selected(manifest)
            .iter()
            .filter(|((_, name), _)| name == crate::nextest::CLUSTER_ROLE_TEST)
            .count(),
        1
    );
}

pub(crate) fn selected(manifest: &Value) -> Cases {
    let mut cases = BTreeMap::new();
    for (id, suite) in manifest["rust-suites"].as_object().unwrap() {
        assert_eq!(suite["binary-id"], id.as_str());
        for (name, case) in suite["testcases"].as_object().unwrap() {
            assert!(case["ignored"].is_boolean());
            let status = case["filter-match"]["status"]
                .as_str()
                .expect("case filter status");
            assert!(
                ["matches", "mismatch"].contains(&status),
                "unknown case filter status"
            );
            if status == "matches" && case["ignored"] == false {
                assert!(cases.insert((id.clone(), name.clone()), 1).is_none());
            }
        }
    }
    cases
}

pub(crate) fn admit_selection(full: &Value, selection: &Value, role: bool) {
    let full_suites = full["rust-suites"].as_object().unwrap();
    let suites = selection["rust-suites"].as_object().unwrap();
    assert_eq!(
        full_suites.keys().collect::<Vec<_>>(),
        suites.keys().collect::<Vec<_>>()
    );
    for (id, suite) in suites {
        let original = &full_suites[id];
        for field in [
            "binary-id",
            "binary-name",
            "binary-path",
            "build-platform",
            "kind",
            "package-id",
            "package-name",
            "status",
        ] {
            assert_eq!(suite[field], original[field]);
        }
        let cases = suite["testcases"].as_object().unwrap();
        let original = original["testcases"].as_object().unwrap();
        assert_eq!(
            cases.keys().collect::<Vec<_>>(),
            original.keys().collect::<Vec<_>>()
        );
        for (name, case) in cases {
            assert_eq!(case["ignored"], original[name]["ignored"]);
        }
    }
    let cases = selected(selection);
    assert!(!cases.is_empty(), "empty selected library contribution");
    let full = selected(full);
    for key in cases.keys() {
        assert!(full.contains_key(key));
        assert_eq!(key.1 == crate::nextest::CLUSTER_ROLE_TEST, role);
    }
    if role {
        assert_eq!(cases.len(), 1);
    }
}

pub(crate) fn passing(stdout: &[u8], stderr: &[u8]) -> Cases {
    let mut cases = BTreeMap::new();
    for stream in [stdout, stderr] {
        let text = std::str::from_utf8(stream).expect("UTF-8 runner evidence");
        assert!(!text.contains('\u{1b}'), "unexpected runner color controls");
        for line in text.lines() {
            let line = line.trim_start();
            let Some(rest) = line.strip_prefix("PASS ") else {
                continue;
            };
            let rest = rest
                .trim_start()
                .strip_prefix('[')
                .expect("runner duration");
            let (duration, rest) = rest.split_once(']').expect("closed runner duration");
            let duration = duration.trim().strip_suffix('s').expect("runner seconds");
            assert!(
                duration
                    .parse::<f64>()
                    .is_ok_and(|v| v.is_finite() && v >= 0.0())
            );
            let mut rest = rest.trim_start();
            if rest.starts_with('(') {
                let (counter, tail) = rest.split_once(')').expect("runner progress counter");
                let (index, total) = counter[1..]
                    .trim()
                    .split_once('/')
                    .expect("runner progress pair");
                assert!(index.parse::<usize>().is_ok_and(|v| v > 0));
                assert!(total.parse::<usize>().is_ok_and(|v| v > 0));
                rest = tail.trim_start();
            }
            let (binary, case) = rest
                .split_once(char::is_whitespace)
                .expect("runner binary and case");
            let case = case.trim();
            assert!(!binary.is_empty() && !case.is_empty());
            let count = cases
                .entry((binary.to_owned(), case.to_owned()))
                .or_insert(0);
            *count += 1;
            assert_eq!(*count, 1, "duplicate passing result");
        }
    }
    assert!(!cases.is_empty(), "no passing runner evidence");
    cases
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Value {
        json!({"rust-suites": {"crate": {"binary-id": "crate", "testcases": {
            "active": {"ignored": false, "filter-match": {"status": "matches"}},
            "ignored": {"ignored": true, "filter-match": {"status": "matches"}},
            "excluded": {"ignored": false, "filter-match": {"status": "mismatch"}}
        }}}})
    }

    #[test]
    fn passing_evidence_requires_exact_unambiguous_case_multiset() {
        let expected = selected(&manifest());
        assert_eq!(
            passing(b"", b"        PASS [ 0.014s] ( 1/1) crate active\n"),
            expected
        );
        assert_ne!(passing(b"PASS [0.0s] crate extra\n", b""), expected);
        for invalid in [
            "PASS [0.0s] crate active\nPASS [0.0s] crate active\n",
            "payload PASS [0.0s] crate active\n",
            "PASS [NaNs] crate active\n",
            "PASS [0.0s] crate\n",
            "\x1b[0mPASS [0.0s] crate active\n",
            "Summary [1.0s] 1 tests run: 1 passed\n",
        ] {
            assert!(std::panic::catch_unwind(|| passing(invalid.as_bytes(), b"")).is_err());
        }
        let mut invalid = manifest();
        invalid["rust-suites"]["crate"]["testcases"]["active"]["filter-match"]["status"] =
            json!("unknown");
        assert!(std::panic::catch_unwind(|| selected(&invalid)).is_err());
    }

    #[test]
    fn selected_inventory_preserves_ignored_and_rejects_foreign_or_missing_cases() {
        let full = manifest();
        assert_eq!(
            selected(&full).keys().cloned().collect::<Vec<_>>(),
            [("crate".into(), "active".into())]
        );
        admit_selection(&full, &full, false);
        for field in ["ignored", "missing", "foreign", "binary"] {
            let mut altered = full.clone();
            let suite = &mut altered["rust-suites"]["crate"];
            match field {
                "ignored" => suite["testcases"]["active"]["ignored"] = json!(true),
                "missing" => {
                    suite["testcases"]
                        .as_object_mut()
                        .unwrap()
                        .remove("ignored");
                }
                "foreign" => {
                    suite["testcases"]["extra"] =
                        json!({"ignored":false,"filter-match":{"status":"matches"}});
                }
                "binary" => suite["binary-id"] = json!("other"),
                _ => unreachable!(),
            }
            assert!(std::panic::catch_unwind(|| admit_selection(&full, &altered, false)).is_err());
        }
    }
}
