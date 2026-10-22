use super::*;

fn assert_config(options: Options, code: &str) {
    let error = match BackgroundJobs::new(options) {
        Err(error) => error,
        Ok(_) => panic!("configuration accepted: {code}"),
    };
    assert_eq!(
        error.to_string(),
        format!("extension plan failed: extension configuration invalid: {code}")
    );
}
#[test]
fn config_validation_table() {
    type Edit = fn(&mut Options);
    let edits: Vec<(&str, Edit)> = vec![
        ("shell-path", |o| o.shell_path = "relative".into()),
        ("shell-path", |o| o.shell_path = "/bin/../bin/sh".into()),
        ("shell-path", |o| o.shell_path = "/bin/sh/".into()),
        ("shell-path", |o| {
            o.shell_path = "/definitely-missing-synthetic-shell".into()
        }),
        ("shell-executable", |o| o.shell_path = "/tmp".into()),
        ("shell-identity", |o| o.shell_identity.clear()),
        ("environment-identity", |o| {
            o.environment.identity = "bad\nidentity".into()
        }),
        ("environment-entries", |o| {
            o.limits.max_environment_entries = 1;
            o.environment.overrides.insert("EXTRA".into(), "x".into());
        }),
        ("environment-entry", |o| {
            o.environment.overrides.insert("BAD=KEY".into(), "x".into());
        }),
        ("environment-entry", |o| {
            o.environment
                .overrides
                .insert("KEY".into(), "bad\0value".into());
        }),
        ("environment-bytes", |o| o.limits.max_environment_bytes = 1),
        ("tracked-below-running", |o| o.limits.max_tracked = 1),
        ("max-timeout", |o| o.limits.max_timeout = Duration::ZERO),
        ("max-timeout", |o| {
            o.limits.max_timeout = Duration::from_millis(1001)
        }),
        ("max-timeout", |o| {
            o.limits.max_timeout = Duration::from_secs(86401)
        }),
        ("default-timeout", |o| {
            o.limits.default_timeout = Duration::from_secs(31)
        }),
        ("default-timeout", |o| {
            o.limits.default_timeout = Duration::from_millis(1)
        }),
        ("termination-bounds", |o| {
            o.limits.terminate_grace = Duration::ZERO
        }),
        ("termination-bounds", |o| {
            o.limits.kill_wait = Duration::from_secs(61)
        }),
        ("termination-bounds", |o| {
            o.limits.shutdown_grace = Duration::from_secs(121)
        }),
    ];
    for (code, edit) in edits {
        let mut o = options();
        edit(&mut o);
        assert_config(o, code);
    }
    let fields: [fn(&mut Limits) -> &mut usize; 7] = [
        |l| &mut l.max_running,
        |l| &mut l.max_tracked,
        |l| &mut l.max_command_bytes,
        |l| &mut l.max_working_directory_bytes,
        |l| &mut l.max_output_bytes_per_stream,
        |l| &mut l.max_environment_entries,
        |l| &mut l.max_environment_bytes,
    ];
    for (field, cap) in fields
        .into_iter()
        .zip([64, 256, 65536, 4096, 163840, 1024, 262144])
    {
        for value in [0, cap + 1] {
            let mut o = options();
            *field(&mut o.limits) = value;
            assert_config(o, "limits");
        }
    }
}
#[test]
fn environment_debug_hides_values() {
    let mut o = options();
    o.shell_path = "/synthetic-private-shell".into();
    o.environment
        .overrides
        .insert("SECRET".into(), "synthetic-private-value".into());
    let debug = format!("{o:?} {:?}", o.environment);
    assert!(!debug.contains("synthetic-private"));
    assert!(!debug.contains("SECRET"));
}
#[test]
fn hash_ignores_environment_values_and_changes_with_each_field() {
    let original = BackgroundJobs::new(options()).unwrap().hash;
    let mut changed = options();
    changed
        .environment
        .overrides
        .insert("VALUE".into(), "synthetic".into());
    assert_eq!(BackgroundJobs::new(changed).unwrap().hash, original);
    let edits: [fn(&mut Options); 16] = [
        |o| o.limits.max_running += 1,
        |o| o.limits.max_tracked += 1,
        |o| o.limits.max_command_bytes += 1,
        |o| o.limits.max_working_directory_bytes += 1,
        |o| o.limits.max_output_bytes_per_stream += 1,
        |o| o.limits.max_environment_entries -= 1,
        |o| o.limits.max_environment_bytes -= 1,
        |o| o.limits.default_timeout = Duration::from_secs(1),
        |o| o.limits.max_timeout += Duration::from_secs(1),
        |o| o.limits.terminate_grace += Duration::from_millis(1),
        |o| o.limits.kill_wait += Duration::from_millis(1),
        |o| o.limits.shutdown_grace += Duration::from_millis(1),
        |o| o.shell_identity.push('x'),
        |o| o.environment.identity.push('x'),
        |o| o.environment.mode = EnvironmentMode::InheritAndOverride,
        |o| o.shell_path = "/bin/cat".into(),
    ];
    for edit in edits {
        let mut o = options();
        edit(&mut o);
        assert_ne!(BackgroundJobs::new(o).unwrap().hash, original);
    }
}
#[test]
fn shell_symlink_is_resolved_and_frozen() {
    let dir = tempfile::tempdir().unwrap();
    let link = dir.path().join("shell");
    std::os::unix::fs::symlink("/bin/sh", &link).unwrap();
    let original = BackgroundJobs::new(options()).unwrap().hash;
    let mut o = options();
    o.shell_path = link;
    assert_eq!(BackgroundJobs::new(o).unwrap().hash, original);
}
#[test]
fn inputs_are_strict_bounded_and_normalized() {
    let limits = options().limits;
    let start = input::start(
        &json!({"command":" echo ok ","working_directory":"a/../b","timeout_seconds":0}),
        &limits,
    )
    .unwrap();
    assert_eq!(start.command, " echo ok ");
    assert_eq!(start.directory, std::path::Path::new("b"));
    assert_eq!(start.timeout, 0);
    for value in [
        json!(null),
        json!({}),
        json!({"command":"x","unknown":true}),
        json!({"command":1}),
    ] {
        assert!(input::start(&value, &limits).is_err());
    }
    for value in [
        json!({"command":""}),
        json!({"command":"\0"}),
        json!({"command":"x","working_directory":"../x"}),
        json!({"command":"x","working_directory":"/tmp"}),
        json!({"command":"x","timeout_seconds":-1}),
        json!({"command":"x","timeout_seconds":null}),
    ] {
        assert!(input::start(&value, &limits).is_err());
    }
    assert!(input::list(&json!({})).is_ok());
    assert!(input::list(&json!({"x":1})).is_err());
    assert!(input::id(&json!({"id":"job_bad"})).is_err());
}

#[test]
fn fingerprint_covers_each_permission_and_supervisor_input() {
    let o = options();
    let shell = o.shell_path.canonicalize().unwrap();
    let permissions = [PERMISSION_START, PERMISSION_READ, PERMISSION_KILL];
    let digest = crate::process::supervisor_digest();
    let protocol = crate::process::SUPERVISOR_PROTOCOL;
    let original = config::fingerprint(&o, &shell, permissions, protocol, &digest);
    assert_eq!(original, BackgroundJobs::new(o.clone()).unwrap().hash);
    for index in 0..3 {
        let mut changed = permissions;
        changed[index] = "fixture/changed-permission";
        assert_ne!(
            original,
            config::fingerprint(&o, &shell, changed, protocol, &digest)
        );
    }
    assert_ne!(
        original,
        config::fingerprint(&o, &shell, permissions, "fixture/protocol", &digest)
    );
    assert_ne!(
        original,
        config::fingerprint(&o, &shell, permissions, protocol, "fixture/digest")
    );
}
