//! Keep all AG-UI consumers on the qualified SDK revision.

use std::{fs, path::Path};

const SDK_GIT: &str = "https://github.com/mattsp1290/ag-ui.git";
const SDK_REV: &str = "705303b26b6b900cdf9bdfa456fbd92a7003ca6b";

pub(crate) fn check(workspace: &Path) {
    check_directory(workspace);
}

fn check_directory(directory: &Path) {
    for entry in fs::read_dir(directory).expect("read manifest directory") {
        let entry = entry.expect("read manifest entry");
        let path = entry.path();
        if entry.file_type().expect("manifest file type").is_dir() {
            if matches!(
                entry.file_name().to_str(),
                Some("target" | ".git" | ".agents" | "reviews" | "node_modules")
            ) {
                continue;
            }
            check_directory(&path);
        } else if entry.file_name() == "Cargo.toml" {
            let text = fs::read_to_string(&path).expect("read Cargo manifest");
            let manifest: toml::Value = toml::from_str(&text).expect("parse Cargo manifest");
            validate(&manifest).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
        }
    }
}

fn validate(value: &toml::Value) -> Result<(), &'static str> {
    match value {
        toml::Value::Table(table) => {
            if table.get("git").and_then(toml::Value::as_str) == Some(SDK_GIT) {
                if table.get("rev").and_then(toml::Value::as_str) != Some(SDK_REV) {
                    return Err("AG-UI dependency must use the qualified full revision");
                }
                if table.contains_key("branch") || table.contains_key("tag") {
                    return Err("AG-UI dependency must not select a branch or tag");
                }
            }
            for child in table.values() {
                validate(child)?;
            }
        }
        toml::Value::Array(array) => {
            for child in array {
                validate(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_nested_dependencies_and_rejects_drift() {
        for selector in [
            format!("rev = '{SDK_REV}'"),
            "rev = '0000000000000000000000000000000000000000'".into(),
            "branch = 'main'".into(),
            format!("rev = '{SDK_REV}', tag = 'v0.2.0'"),
        ] {
            let manifest: toml::Value = toml::from_str(&format!(
                "[target.'cfg(unix)'.dependencies]\nag-ui-core = {{ git = '{SDK_GIT}', {selector} }}"
            ))
            .unwrap();
            assert_eq!(
                validate(&manifest).is_ok(),
                selector == format!("rev = '{SDK_REV}'")
            );
        }
    }
}
