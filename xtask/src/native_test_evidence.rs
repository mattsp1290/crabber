use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) type Cases = BTreeMap<(String, String), usize>;

pub(crate) fn admit_compiler_context(metadata: &Value, row: &Value, package: &Value) {
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
    crate::native_artifacts::validate_debuginfo(&row["profile"]["debuginfo"]);
    assert_eq!(row["profile"]["debug_assertions"], true);
    assert_eq!(row["profile"]["overflow_checks"], true);
}

pub(crate) fn admit_manifest(binaries: &Value, manifest: &Value) {
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
            if !ignored(case) {
                assert_eq!(
                    case["filter-match"],
                    json!({"status": "matches"}),
                    "active unpartitioned case excluded"
                );
            }
        }
    }
    assert!(!selected(manifest).is_empty());
    let count: usize = suites
        .values()
        .map(|suite| suite["testcases"].as_object().unwrap().len())
        .sum();
    assert_eq!(
        usize::try_from(manifest["test-count"].as_u64().unwrap()).unwrap(),
        count
    );
}

pub(crate) fn admit_packages(metadata: &Value, manifest: &Value) {
    for suite in manifest["rust-suites"].as_object().unwrap().values() {
        let package = metadata["packages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|package| package["id"] == suite["package-id"])
            .expect("declared manifest package");
        assert!(
            metadata["workspace_members"]
                .as_array()
                .unwrap()
                .contains(&package["id"])
        );
        assert_eq!(
            suite["package-name"], package["name"],
            "manifest package name differs from metadata"
        );
    }
}

pub(crate) fn ignored(case: &Value) -> bool {
    let ignored = case["ignored"].as_bool().expect("case ignored flag");
    if ignored {
        assert_eq!(
            case["filter-match"],
            json!({"status": "mismatch", "reason": "ignored"}),
            "ignored case must retain runner exclusion evidence"
        );
    }
    ignored
}

pub(crate) fn selected(manifest: &Value) -> Cases {
    let mut cases = BTreeMap::new();
    for (id, suite) in manifest["rust-suites"].as_object().unwrap() {
        assert_eq!(suite["binary-id"], id.as_str());
        for (name, case) in suite["testcases"].as_object().unwrap() {
            let ignored = ignored(case);
            let status = case["filter-match"]["status"]
                .as_str()
                .expect("case filter status");
            assert!(
                ["matches", "mismatch"].contains(&status),
                "unknown case filter status"
            );
            if status == "matches" && !ignored {
                assert!(cases.insert((id.clone(), name.clone()), 1).is_none());
            }
        }
    }
    cases
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
                    .is_ok_and(|v| v.is_finite() && v >= 0.0)
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

pub(crate) fn execution(manifest: &Value, stdout: &[u8], stderr: &[u8]) -> Value {
    let actual = passing(stdout, stderr);
    assert_eq!(
        actual,
        selected(manifest),
        "executed passing cases differ from admitted selection"
    );
    for stream in [stdout, stderr] {
        for line in std::str::from_utf8(stream)
            .unwrap()
            .lines()
            .map(str::trim_start)
        {
            assert!(
                !["FAIL ", "TIMEOUT ", "LEAK ", "ERROR ", "CANCEL "]
                    .iter()
                    .any(|prefix| line.starts_with(prefix)),
                "nonpassing runner outcome"
            );
        }
    }
    json!({"exact_equal":true,"actual_cases":actual.into_iter().map(|((binary,case),count)|json!([binary,case,count])).collect::<Vec<_>>(),"stdin":"null","success_output":"never","status_level":"pass","color":"never"})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn execution_requires_exact_passes_and_rejects_failed_or_duplicate_outcomes() {
        let manifest = json!({"rust-suites":{"binary":{"binary-id":"binary","testcases":{"case":{"ignored":false,"filter-match":{"status":"matches"}}}}}});
        let pass = b"PASS [0.01s] binary case\n";
        assert_eq!(
            execution(&manifest, pass, b"")["actual_cases"],
            json!([["binary", "case", 1]])
        );
        for output in [
            b"PASS [0.01s] binary other\n".as_slice(),
            b"PASS [0.01s] binary case\nPASS [0.02s] binary case\n".as_slice(),
            b"PASS [0.01s] binary case\nFAIL [0.02s] binary case\n".as_slice(),
        ] {
            assert!(std::panic::catch_unwind(|| execution(&manifest, output, b"")).is_err());
        }
    }
}
