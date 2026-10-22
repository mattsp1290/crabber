use super::support::options;
use crate::python_repl::{
    config::{self, PRIVATE_KEYS},
    *,
};
use std::{
    os::unix::fs::{PermissionsExt, symlink},
    time::Duration,
};

fn error(options: Options, code: &str) {
    assert!(
        config::validate(options)
            .err()
            .unwrap()
            .to_string()
            .ends_with(code)
    );
}
#[test]
fn every_limit_bound_and_identity_is_validated() {
    let directory = tempfile::tempdir().unwrap();
    let Some(base) = options(directory.path()) else {
        return;
    };
    macro_rules! bound {
        ($field:ident, $cap:expr) => {
            for value in [0, $cap + 1] {
                let mut o = base.clone();
                o.limits.$field = value;
                error(o, "limits");
            }
            for value in [1, $cap] {
                let mut o = base.clone();
                o.limits.$field = value;
                assert!(config::validate(o).is_ok());
            }
        };
    }
    bound!(max_sessions, 256);
    bound!(max_queued_per_session, 64);
    bound!(max_code_bytes, 1024 * 1024);
    bound!(max_output_bytes_per_stream, 100 * 1024);
    bound!(max_result_bytes, 100 * 1024);
    bound!(max_exception_bytes, 100 * 1024);
    bound!(max_environment_entries, 1024);
    bound!(max_environment_bytes, 256 * 1024);
    for d in [
        Duration::ZERO,
        Duration::from_millis(1500),
        Duration::from_secs(86401),
    ] {
        let mut o = base.clone();
        o.limits.max_timeout = d;
        error(o, "execution-timeout");
    }
    for d in [
        Duration::ZERO,
        Duration::from_millis(1500),
        Duration::from_secs(31),
    ] {
        let mut o = base.clone();
        o.limits.default_timeout = d;
        error(o, "execution-timeout");
    }
    macro_rules! time_bound {
        ($field:ident, $cap:expr) => {
            for d in [Duration::ZERO, Duration::from_secs($cap + 1)] {
                let mut o = base.clone();
                o.limits.$field = d;
                error(o, "termination-bounds");
            }
        };
    }
    time_bound!(runner_start_timeout, 60);
    time_bound!(terminate_grace, 60);
    time_bound!(kill_wait, 60);
    time_bound!(shutdown_grace, 120);
    let mut o = base.clone();
    o.limits.shutdown_grace = Duration::from_secs(3);
    error(o, "shutdown-grace-below-termination");
    for identity in ["".into(), "\n".into(), "x".repeat(257)] {
        let mut o = base.clone();
        o.python_identity = identity.clone();
        error(o, "python-identity");
        let mut o = base.clone();
        o.environment.identity = identity;
        error(o, "environment-identity");
    }
}
#[test]
fn paths_and_environment_validate_in_order() {
    let directory = tempfile::tempdir().unwrap();
    let Some(base) = options(directory.path()) else {
        return;
    };
    for path in ["python", "/tmp/../python", "/tmp/./python", "/tmp/python\0"] {
        let mut o = base.clone();
        o.python_path = path.into();
        error(o, "python-path");
    }
    let mut o = base.clone();
    o.python_path = directory.path().into();
    error(o, "python-executable");
    let mut o = base.clone();
    o.temp_root = base.python_path.clone();
    error(o, "temp-root");
    for mode in [
        EnvironmentMode::ExplicitOnly,
        EnvironmentMode::InheritAndOverride,
    ] {
        for (key, _) in PRIVATE_KEYS {
            let mut o = base.clone();
            o.environment.mode = mode;
            o.environment.overrides.insert(key.into(), "secret".into());
            error(o, "environment-reserved-key");
        }
    }
    for (key, value) in [("", "a"), ("a=b", "a"), ("a\0", "b"), ("a", "b\0")] {
        let mut o = base.clone();
        o.environment.overrides.insert(key.into(), value.into());
        error(o, "environment-entry");
    }
    let mut o = base.clone();
    o.limits.max_environment_entries = 1;
    o.environment
        .overrides
        .extend([("A".into(), "1".into()), ("B".into(), "2".into())]);
    error(o, "environment-entries");
    let mut o = base.clone();
    o.limits.max_environment_bytes = 2;
    o.environment.overrides.insert("A".into(), "1".into());
    error(o, "environment-bytes");
    let mut o = base.clone();
    o.environment.mode = EnvironmentMode::InheritAndOverride;
    let (configuration, _) = config::validate(o).unwrap();
    assert!(
        !configuration
            .environment
            .iter()
            .any(|(k, _)| PRIVATE_KEYS.iter().any(|(key, _)| k == key))
    );
}
#[test]
fn hash_tracks_all_limits_identities_and_not_values_keys_or_temp_root() {
    let directory = tempfile::tempdir().unwrap();
    let Some(base) = options(directory.path()) else {
        return;
    };
    let hash = |o| config::validate(o).unwrap().1;
    let original = hash(base.clone());
    macro_rules! drift {
        ($field:ident,$amount:expr) => {
            let mut o = base.clone();
            o.limits.$field += $amount;
            assert_ne!(hash(o), original, stringify!($field));
        };
    }
    drift!(max_sessions, 1);
    drift!(max_queued_per_session, 1);
    drift!(max_code_bytes, 1);
    drift!(max_output_bytes_per_stream, 1);
    drift!(max_result_bytes, 1);
    drift!(max_exception_bytes, 1);
    // Environment defaults are at caps; lower them to stay valid.
    for field in [true, false] {
        let mut o = base.clone();
        if field {
            o.limits.max_environment_entries -= 1;
        } else {
            o.limits.max_environment_bytes -= 1;
        }
        assert_ne!(hash(o), original);
    }
    drift!(default_timeout, Duration::from_secs(1));
    drift!(max_timeout, Duration::from_secs(1));
    drift!(runner_start_timeout, Duration::from_millis(1));
    drift!(terminate_grace, Duration::from_millis(1));
    drift!(kill_wait, Duration::from_millis(1));
    drift!(shutdown_grace, Duration::from_millis(1));
    let mut o = base.clone();
    o.python_identity.push('x');
    assert_ne!(hash(o), original);
    let mut o = base.clone();
    o.environment.identity.push('x');
    assert_ne!(hash(o), original);
    let mut o = base.clone();
    o.environment.mode = EnvironmentMode::InheritAndOverride;
    assert_ne!(hash(o), original);
    let mut o = base.clone();
    o.environment
        .overrides
        .insert("SECRET".into(), "hidden".into());
    assert_eq!(hash(o), original);
    let another = tempfile::tempdir().unwrap();
    let mut o = base.clone();
    o.temp_root = another.path().into();
    assert_eq!(hash(o), original);
    let link = directory.path().join("python");
    symlink(&base.python_path, &link).unwrap();
    let mut o = base.clone();
    o.python_path = link;
    assert_eq!(hash(o), original);
}
#[test]
fn diagnostics_construction_and_saturating_result_bounds() {
    let directory = tempfile::tempdir().unwrap();
    let Some(mut o) = options(directory.path()) else {
        return;
    };
    o.environment.overrides.insert(
        "SYNTHETIC_SECRET_KEY".into(),
        "SYNTHETIC_SECRET_VALUE".into(),
    );
    let debug = format!("{o:?}");
    for hidden in [
        "SYNTHETIC_SECRET_KEY",
        "SYNTHETIC_SECRET_VALUE",
        directory.path().to_str().unwrap(),
        o.python_path.to_str().unwrap(),
    ] {
        assert!(!debug.contains(hidden));
    }
    let canary = directory.path().join("canary");
    let script = directory.path().join("interpreter");
    std::fs::write(
        &script,
        format!("#!/bin/sh\ntouch '{}'\n", canary.display()),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    o.python_path = script;
    assert!(PythonRepl::new(o.clone()).is_ok());
    assert!(!canary.exists());
    o.limits.max_output_bytes_per_stream = usize::MAX;
    assert_eq!(o.limits.worst_case_execute_bytes(), usize::MAX);
    assert_eq!(Limits::worst_case_clear_bytes(), 64);
}
