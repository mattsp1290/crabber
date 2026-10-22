//! Cargo's caller package context is not configuration for a nested invocation.
use std::process::Command;

pub(super) fn remove_parent_package(command: &mut Command) {
    // Build scripts can register these as rerun-if-env-changed inputs. Keeping
    // xtask's values makes the same unit alternate between parent package
    // contexts in direct Cargo commands, nextest and standalone consumers.
    let package_names = std::env::vars_os()
        .map(|(name, _)| name)
        .chain(command.get_envs().map(|(name, _)| name.to_os_string()))
        .filter(|name| name.as_encoded_bytes().starts_with(b"CARGO_PKG_"))
        .collect::<Vec<_>>();
    for name in package_names {
        command.env_remove(name);
    }
    for name in [
        "CARGO_MANIFEST_DIR",
        "CARGO_MANIFEST_PATH",
        "CARGO_MANIFEST_LINKS",
    ] {
        command.env_remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;

    #[test]
    fn child_keeps_build_configuration_without_parent_package_context() {
        let mut child = Command::new("cargo");
        for name in ["CARGO_PKG_NAME", "CARGO_PKG_CUSTOM", "CARGO_MANIFEST_DIR"] {
            child.env(name, "parent-package");
        }
        let configuration = [
            ("CARGO_TARGET_DIR", "owned-target"),
            ("CARGO_BUILD_JOBS", "4"),
            ("CARGO_PROFILE_TEST_OPT_LEVEL", "0"),
            ("CARGO_ENCODED_RUSTFLAGS", "-Cdebuginfo=0"),
        ];
        child.envs(configuration);
        remove_parent_package(&mut child);
        let variables = child
            .get_envs()
            .collect::<std::collections::HashMap<_, _>>();
        for name in ["CARGO_PKG_NAME", "CARGO_PKG_CUSTOM", "CARGO_MANIFEST_DIR"] {
            assert_eq!(variables.get(OsStr::new(name)), Some(&None));
        }
        for (name, value) in configuration {
            assert_eq!(
                variables.get(OsStr::new(name)),
                Some(&Some(OsStr::new(value)))
            );
        }
    }
}
