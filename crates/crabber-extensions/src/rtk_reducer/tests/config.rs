use super::super::config::*;
use sha2::{Digest, Sha256};
use std::{fs, os::unix::fs::PermissionsExt, time::Duration};

pub(super) fn options(dir: &std::path::Path) -> Options {
    let executable_path = dir.join("rtk");
    fs::write(&executable_path, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&executable_path, fs::Permissions::from_mode(0o700)).unwrap();
    let temp_root = dir.join("private");
    fs::create_dir_all(&temp_root).unwrap();
    fs::set_permissions(&temp_root, fs::Permissions::from_mode(0o700)).unwrap();
    Options {
        order: DEFAULT_ORDER,
        executable_sha256: format!("{:x}", Sha256::digest(fs::read(&executable_path).unwrap())),
        executable_path,
        temp_root,
        bindings: vec![
            Binding {
                tool_name: "fixture_tool".into(),
                match_input: None,
                selection: Selection::Text(Filter::Log),
            },
            Binding {
                tool_name: "fixture_json".into(),
                match_input: Some(InputMatch {
                    field: "cmd".into(),
                    equals: vec!["cargo test".into(), "git diff".into()],
                }),
                selection: Selection::JsonFields(vec![
                    Field {
                        name: "stdout".into(),
                        filter: Filter::CargoTest,
                    },
                    Field {
                        name: "diff".into(),
                        filter: Filter::GitDiff,
                    },
                ]),
            },
        ],
        limits: Limits {
            max_bindings: 8,
            max_fields_per_binding: 8,
            max_input_matches: 8,
            max_config_bytes: 16 * 1024,
            max_input_bytes: 4 * 1024,
            max_field_bytes: 1024 * 1024,
            max_concurrent: 2,
            max_stdout_bytes: 256 * 1024,
            timeout: Duration::from_secs(10),
            kill_wait: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(30),
        },
    }
}
#[test]
fn baseline_validates_and_hash_is_stable() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = options(dir.path());
    let (c, hash) = validate(o.clone()).unwrap();
    assert_eq!(c.executable, o.executable_path.canonicalize().unwrap());
    assert_eq!(c.temp_root, o.temp_root.canonicalize().unwrap());
    assert_eq!(c.bindings.len(), 2);
    assert_eq!(c.order, DEFAULT_ORDER);
    assert_eq!(c.limits.max_concurrent, 2);
    o.bindings.reverse();
    o.bindings[0].match_input.as_mut().unwrap().equals.reverse();
    if let Selection::JsonFields(fields) = &mut o.bindings[0].selection {
        fields.reverse();
    }
    assert_eq!(hash, validate(o).unwrap().1);
    assert_eq!(Filter::GoTest.cli_name(), "go-test");
}

#[path = "../../../tests/rtk_reducer/resume/mutations.rs"]
mod mutation_cases;
use mutation_cases::{Edit, additional_mutations, mutations};
#[test]
fn hash_changes_for_every_behavior_bearing_option() {
    let dir = tempfile::tempdir().unwrap();
    let o = options(dir.path());
    let hash = validate(o.clone()).unwrap().1;
    let mut rows = mutations();
    assert_eq!(rows.len(), 11 + 9);
    rows.extend(additional_mutations());
    assert_eq!(rows.len(), 23);
    for (name, edit) in rows {
        let mut other = o.clone();
        edit(&mut other);
        assert_ne!(hash, validate(other).unwrap().1, "{name}");
    }
}
#[test]
fn hash_ignores_paths() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = options(dir.path());
    let hash = validate(o.clone()).unwrap().1;
    let p = dir.path().join("relocated");
    fs::copy(&o.executable_path, &p).unwrap();
    o.executable_path = p;
    o.temp_root = dir.path().join("other-private");
    fs::create_dir(&o.temp_root).unwrap();
    fs::set_permissions(&o.temp_root, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(hash, validate(o).unwrap().1);
}
#[test]
fn every_error_code_is_reachable() {
    let dir = tempfile::tempdir().unwrap();
    let baseline = options(dir.path());
    let rows: Vec<(&str, Edit)> = vec![
        ("limits", |o| o.limits.max_bindings = 0),
        ("limit-order", |o| {
            o.limits.max_field_bytes = o.limits.max_stdout_bytes - 1
        }),
        ("timeouts", |o| o.limits.timeout = Duration::ZERO),
        ("timeouts", |o| o.limits.kill_wait = Duration::from_secs(61)),
        ("shutdown-grace", |o| {
            o.limits.shutdown_grace = Duration::from_secs(1)
        }),
        ("executable-digest", |o| o.executable_sha256.push('0')),
        ("executable-digest", |o| {
            o.executable_sha256 = "A".repeat(64)
        }),
        ("bindings-required", |o| o.bindings.clear()),
        ("binding-count", |o| o.limits.max_bindings = 1),
        ("config-bytes", |o| o.limits.max_config_bytes = 1),
        ("tool-name", |o| o.bindings[0].tool_name = "bad/name".into()),
        ("duplicate-tool", |o| {
            o.bindings[1].tool_name = o.bindings[0].tool_name.clone()
        }),
        ("json-fields", |o| {
            o.bindings[0].selection = Selection::JsonFields(vec![])
        }),
        ("field-count", |o| o.limits.max_fields_per_binding = 1),
        ("field-name", |o| {
            o.bindings[0].selection = Selection::JsonFields(vec![Field {
                name: "\0".into(),
                filter: Filter::Log,
            }])
        }),
        ("duplicate-field", |o| {
            o.bindings[0].selection = Selection::JsonFields(vec![
                Field {
                    name: "x".into(),
                    filter: Filter::Log
                };
                2
            ])
        }),
        ("match-field", |o| {
            o.bindings[1].match_input.as_mut().unwrap().field.clear()
        }),
        ("input-match-count", |o| {
            o.bindings[1].match_input.as_mut().unwrap().equals.clear()
        }),
        ("input-match-count", |o| o.limits.max_input_matches = 1),
        ("input-match-value", |o| {
            o.bindings[1].match_input.as_mut().unwrap().equals[0] = "\0".into()
        }),
        ("input-match-value", |o| o.limits.max_input_bytes = 1),
        ("duplicate-input-match", |o| {
            o.bindings[1].match_input.as_mut().unwrap().equals = vec!["x".into(); 2]
        }),
        ("executable-path", |o| o.executable_path = "relative".into()),
        ("executable-path", |o| {
            o.executable_path = o.executable_path.with_file_name("missing")
        }),
        ("executable-file", |o| {
            o.executable_path = o.temp_root.clone()
        }),
        ("executable-digest-mismatch", |o| {
            o.executable_sha256 = "0".repeat(64)
        }),
        ("temp-root", |o| o.temp_root = "relative".into()),
        ("temp-root", |o| o.temp_root = o.executable_path.clone()),
    ];
    for (code, edit) in rows {
        let mut o = baseline.clone();
        edit(&mut o);
        let error = validate(o).err().unwrap().to_string();
        assert!(error.ends_with(code), "{code}: {error}");
        assert!(!error.contains(dir.path().to_str().unwrap()));
        assert!(!error.contains(&baseline.executable_sha256));
    }
    fs::set_permissions(&baseline.executable_path, fs::Permissions::from_mode(0o600)).unwrap();
    assert!(
        validate(baseline.clone())
            .err()
            .unwrap()
            .to_string()
            .ends_with("executable-file")
    );
    fs::set_permissions(&baseline.executable_path, fs::Permissions::from_mode(0o700)).unwrap();
    fs::set_permissions(&baseline.temp_root, fs::Permissions::from_mode(0o770)).unwrap();
    assert!(
        validate(baseline)
            .err()
            .unwrap()
            .to_string()
            .ends_with("temp-root-mode")
    );
    assert!(
        validate_owner(1, 2)
            .unwrap_err()
            .to_string()
            .ends_with("temp-root-owner")
    );
}
#[test]
fn construction_never_executes_and_digest_is_construction_time_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = options(dir.path());
    let counter = dir.path().join("counter");
    let script = format!("#!/bin/sh\nprintf ran > '{}'\n", counter.display());
    fs::write(&o.executable_path, script.as_bytes()).unwrap();
    o.executable_sha256 = format!("{:x}", Sha256::digest(script.as_bytes()));
    let (first, _) = validate(o.clone()).unwrap();
    assert!(!counter.exists());
    fs::write(&o.executable_path, "#!/bin/sh\nexit 1\n").unwrap();
    assert!(
        validate(o.clone())
            .err()
            .unwrap()
            .to_string()
            .ends_with("executable-digest-mismatch")
    );
    assert_eq!(first.executable, o.executable_path.canonicalize().unwrap());
}
#[test]
fn symlink_is_accepted_and_debug_omits_paths_and_digest() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = options(dir.path());
    let debug = format!("{o:?}");
    assert!(!debug.contains(o.executable_path.to_str().unwrap()));
    assert!(!debug.contains(o.temp_root.to_str().unwrap()));
    assert!(!debug.contains(&o.executable_sha256));
    let hash = validate(o.clone()).unwrap().1;
    let p = dir.path().join("link");
    std::os::unix::fs::symlink(&o.executable_path, &p).unwrap();
    o.executable_path = p;
    assert_eq!(hash, validate(o).unwrap().1);
}

#[test]
fn oversized_executable_is_rejected_before_hashing() {
    let dir = tempfile::tempdir().unwrap();
    let o = options(dir.path());
    fs::OpenOptions::new()
        .write(true)
        .open(&o.executable_path)
        .unwrap()
        .set_len(128 * 1024 * 1024 + 1)
        .unwrap();
    assert!(
        validate(o)
            .err()
            .unwrap()
            .to_string()
            .ends_with("executable-file")
    );
}
