//! Admission of actual Cargo and pinned Nextest evidence for library workers.
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

#[cfg(test)]
use crate::native_test_evidence::passing;
use crate::native_test_evidence::{admit_compiler_context, ignored, selected};

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
        admit_compiler_context(&metadata, &row, package);
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
        let key = (
            binary["package-id"].as_str().unwrap().to_owned(),
            binary["binary-name"].as_str().unwrap().to_owned(),
        );
        let artifact = actual
            .get(&key)
            .expect("binary outside declared library outputs");
        let (_, target) = &declared[&key];
        let proc_macro = target["kind"] == json!(["proc-macro"]);
        assert_eq!(
            binary["kind"],
            if proc_macro { "proc-macro" } else { "lib" }
        );
        assert_eq!(
            binary["build-platform"],
            if proc_macro { "host" } else { "target" }
        );
        assert_eq!(binary["binary-path"], artifact["executable"]);
        assert!(listed.insert(key), "duplicate listed binary");
    }
    assert_eq!(listed, actual.keys().cloned().collect());
    let rows =
        crate::normal_outputs::journal(directory, &directory.join("library-artifacts.jsonl"));
    assert_eq!(rows[0]["source"], crate::timings::source_identity());
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
    crate::native_test_evidence::admit_manifest(&binaries, manifest);
    admit_role(&read(directory.join("cargo-metadata.json")), manifest);
}

fn admit_role(metadata: &Value, manifest: &Value) {
    let members = metadata["workspace_members"].as_array().unwrap();
    let session: Vec<_> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|package| package["name"] == "crabber-session" && members.contains(&package["id"]))
        .collect();
    assert_eq!(session.len(), 1, "one declared workspace Session package");
    let cases = selected(manifest);
    let role: Vec<_> = cases
        .keys()
        .filter(|(_, name)| name == crate::nextest::CLUSTER_ROLE_TEST)
        .collect();
    assert_eq!(role.len(), 1, "one active cluster-role detector");
    let suite = &manifest["rust-suites"][&role[0].0];
    assert_eq!(
        suite["package-id"], session[0]["id"],
        "cluster-role detector must belong to declared Session"
    );
    assert_eq!(suite["package-name"], session[0]["name"]);
    assert_eq!(suite["kind"], "lib");
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
            if ignored(case) {
                assert_eq!(case, &original[name], "ignored case evidence changed");
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> Value {
        json!({"rust-suites": {"crate": {"binary-id": "crate", "testcases": {
            "active": {"ignored": false, "filter-match": {"status": "matches"}},
            "ignored": {"ignored": true, "filter-match": {"status": "mismatch", "reason": "ignored"}},
            "excluded": {"ignored": false, "filter-match": {"status": "mismatch"}}
        }}}})
    }

    #[test]
    fn cluster_role_requires_the_declared_session_library() {
        let metadata = json!({"workspace_members": ["session", "other"], "packages": [
            {"id": "session", "name": "crabber-session"}, {"id": "other", "name": "other-library"}
        ]});
        let role = crate::nextest::CLUSTER_ROLE_TEST;
        let valid = json!({"rust-suites": {"session": {"binary-id": "session", "package-id": "session",
            "package-name": "crabber-session", "kind": "lib", "testcases": {
                role: {"ignored": false, "filter-match": {"status": "matches"}}
            }
        }}});
        admit_role(&metadata, &valid);
        for (field, replacement) in [
            ("package-id", json!("other")),
            ("package-name", json!("other-library")),
            ("kind", json!("test")),
        ] {
            let mut substituted = valid.clone();
            substituted["rust-suites"]["session"][field] = replacement;
            assert!(std::panic::catch_unwind(|| admit_role(&metadata, &substituted)).is_err());
        }
        let mut ignored = valid.clone();
        ignored["rust-suites"]["session"]["testcases"][role]["ignored"] = json!(true);
        assert!(std::panic::catch_unwind(|| admit_role(&metadata, &ignored)).is_err());
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
        for filter in [
            json!({"status": "matches"}),
            json!({"status": "mismatch", "reason": "expression"}),
            json!({"status": "unknown", "reason": "ignored"}),
        ] {
            let mut altered = full.clone();
            altered["rust-suites"]["crate"]["testcases"]["ignored"]["filter-match"] = filter;
            assert!(std::panic::catch_unwind(|| selected(&altered)).is_err());
        }
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
