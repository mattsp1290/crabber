use serde_json::Value;

pub(crate) fn validate(metadata: &Value, list: &Value, policy: &Value) {
    let runtime = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "crabber-runtime")
        .unwrap();
    let runtime_id = &runtime["id"];
    let resolved = metadata["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| &node["id"] == runtime_id)
        .unwrap();
    assert!(
        resolved["features"]
            .as_array()
            .unwrap()
            .iter()
            .any(|feature| feature == "postgres"),
        "the facade's postgres feature does not enable runtime PostgreSQL tests"
    );
    let suites = list["rust-suites"].as_object().unwrap();
    let mut selected = Vec::new();
    for suite in suites.values() {
        for (name, case) in suite["testcases"].as_object().unwrap() {
            if case["filter-match"]["status"] == "matches" && case["ignored"] == false {
                selected.push((suite["package-name"].as_str().unwrap(), name.as_str()));
            }
        }
    }
    assert!(
        !selected.is_empty(),
        "the complete behavior manifest is empty"
    );
    for family in policy["changes"].as_array().unwrap() {
        for name in family["retained_tests"].as_array().unwrap() {
            let name = name.as_str().unwrap();
            assert!(
                selected.iter().any(|(_, test)| test.ends_with(name)),
                "required detector {name} not selected"
            );
            if family["id"] == "sql-successor-tool-faults" {
                for backend in ["sqlite", "postgres"] {
                    assert!(
                        selected
                            .iter()
                            .any(|(package, test)| *package == "crabber-session"
                                && test.starts_with(&format!("{backend}::"))
                                && test.ends_with(name)),
                        "required {backend} detector {name} not selected"
                    );
                }
            }
        }
    }
    for behavior in policy["required_behaviors"].as_array().unwrap() {
        for detector in behavior["required_detectors"].as_array().unwrap() {
            let package = detector["package"].as_str().unwrap();
            let suffix = detector["suffix"].as_str().unwrap();
            assert!(
                selected
                    .iter()
                    .any(|(actual_package, test)| *actual_package == package
                        && test.ends_with(suffix)),
                "required behavior {} lacks selected detector {package}::{suffix}",
                behavior["behavior"]
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn policy_requires_both_backend_detectors_and_runtime_postgres() {
        let metadata = json!({"packages":[{"name":"crabber-runtime","id":"runtime"}],"resolve":{"nodes":[{"id":"runtime","features":["postgres"]}]}});
        let manifest = json!({"rust-suites":{"session":{"package-name":"crabber-session","testcases":{"sqlite::detector":{"ignored":false,"filter-match":{"status":"matches"}},"postgres::detector":{"ignored":false,"filter-match":{"status":"matches"}}}}}});
        let policy = json!({"changes":[{"id":"sql-successor-tool-faults","retained_tests":["detector"]}],"required_behaviors":[{"behavior":"detect","required_detectors":[{"package":"crabber-session","suffix":"detector"}]}]});
        validate(&metadata, &manifest, &policy);
        let mut missing = manifest.clone();
        missing["rust-suites"]["session"]["testcases"]
            .as_object_mut()
            .unwrap()
            .remove("postgres::detector");
        assert!(std::panic::catch_unwind(|| validate(&metadata, &missing, &policy)).is_err());
        let mut wrong_feature = metadata;
        wrong_feature["resolve"]["nodes"][0]["features"] = json!([]);
        assert!(std::panic::catch_unwind(|| validate(&wrong_feature, &manifest, &policy)).is_err());
    }
}
