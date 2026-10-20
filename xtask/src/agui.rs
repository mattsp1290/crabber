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
        } else if entry.file_name() == "Cargo.lock" {
            let text = fs::read_to_string(&path).expect("read Cargo lockfile");
            let lock: toml::Value = toml::from_str(&text).expect("parse Cargo lockfile");
            validate_lock(&lock).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
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
            for (name, child) in table {
                let package = child
                    .get("package")
                    .and_then(toml::Value::as_str)
                    .unwrap_or(name);
                if name.starts_with("ag-ui-") || package.starts_with("ag-ui-") {
                    let dependency = child
                        .as_table()
                        .ok_or("AG-UI dependencies must use the qualified Git source")?;
                    if dependency.get("git").and_then(toml::Value::as_str) != Some(SDK_GIT) {
                        return Err("AG-UI dependencies must use the canonical qualified Git URL");
                    }
                }
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

fn validate_lock(value: &toml::Value) -> Result<(), &'static str> {
    let expected = format!("git+{SDK_GIT}?rev={SDK_REV}#{SDK_REV}");
    for package in value
        .get("package")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
    {
        if package
            .get("name")
            .and_then(toml::Value::as_str)
            .is_some_and(|name| name.starts_with("ag-ui-"))
            && package.get("source").and_then(toml::Value::as_str) != Some(expected.as_str())
        {
            return Err("AG-UI lock entries must resolve the qualified Git revision");
        }
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

    #[test]
    fn rejects_forks_aliases_registry_dependencies_and_alternate_urls() {
        for dependency in [
            "'0.2'".to_owned(),
            format!("{{ git = 'https://github.com/other/ag-ui.git', rev = '{SDK_REV}' }}"),
            format!("{{ git = 'ssh://git@github.com/mattsp1290/ag-ui.git', rev = '{SDK_REV}' }}"),
            format!(
                "{{ package = 'ag-ui-core', git = 'https://github.com/other/ag-ui.git', rev = '{SDK_REV}' }}"
            ),
        ] {
            let manifest: toml::Value =
                toml::from_str(&format!("[dependencies]\nag-ui-core = {dependency}")).unwrap();
            assert!(validate(&manifest).is_err());
        }
        let alias: toml::Value = toml::from_str(&format!("[dependencies]\nsdk = {{ package = 'ag-ui-core', git = 'https://github.com/other/ag-ui.git', rev = '{SDK_REV}' }}")).unwrap();
        assert!(validate(&alias).is_err());
    }

    #[test]
    fn lock_source_must_match_both_requested_and_resolved_revision() {
        for source in [
            format!("git+{SDK_GIT}?rev={SDK_REV}#{SDK_REV}"),
            format!("git+{SDK_GIT}?rev={SDK_REV}#{}", "0".repeat(40)),
            "registry+https://github.com/rust-lang/crates.io-index".into(),
            format!("git+https://github.com/other/ag-ui.git?rev={SDK_REV}#{SDK_REV}"),
        ] {
            let lock: toml::Value = toml::from_str(&format!(
                "[[package]]\nname = 'ag-ui-core'\nsource = '{source}'"
            ))
            .unwrap();
            assert_eq!(
                validate_lock(&lock).is_ok(),
                source == format!("git+{SDK_GIT}?rev={SDK_REV}#{SDK_REV}")
            );
        }
    }
}
