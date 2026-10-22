use super::super::{
    Filter,
    config::validate,
    engine::{Engine, Reason, Reduced},
};
use crabber::extension::{ToolInput, ToolOutcomeClass, ToolResultContext};
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

fn context() -> ToolResultContext {
    ToolResultContext::new(
        "fixture_tool".into(),
        true,
        ToolInput::Normalized(serde_json::json!({})),
        "call".into(),
        "session".into(),
        "run".into(),
        ToolOutcomeClass::Succeeded,
    )
}
fn engine(dir: &Path, body: &str, timeout: Duration) -> Arc<Engine> {
    assert!(
        Path::new("/bin/sh").is_file(),
        "synthetic RTK requires /bin/sh"
    );
    let mut o = super::config::options(dir);
    // Evidence lives outside the temporary root; the owner removes its tree.
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}/argv.log'\nprintf '%s\\n' \"$$\" >> '{}/pids'\n{body}\n",
        dir.display(),
        dir.display()
    );
    let mut file = fs::File::create(&o.executable_path).unwrap();
    file.write_all(script.as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    o.executable_sha256 = format!("{:x}", Sha256::digest(script.as_bytes()));
    o.limits.timeout = timeout;
    o.limits.max_concurrent = 1;
    Arc::new(Engine::new(validate(o).unwrap().0))
}
async fn wait_live(engine: &Engine, dir: &Path) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if engine.live_children() != 0 && dir.join("pids").is_file() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}
async fn groups_gone(dir: &Path) {
    if let Ok(pids) = fs::read_to_string(dir.join("pids")) {
        for group in pids.lines() {
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    let ps = std::process::Command::new("ps")
                        .args(["-A", "-o", "pid=,pgid=,stat="])
                        .output()
                        .unwrap();
                    assert!(ps.status.success());
                    if !String::from_utf8(ps.stdout)
                        .unwrap()
                        .lines()
                        .any(|line| line.split_whitespace().nth(1) == Some(group))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
    }
}
async fn cleaned(engine: &Arc<Engine>, dir: &Path) {
    engine.close(Duration::from_secs(4)).await.unwrap();
    assert_eq!(engine.live_children(), 0);
    assert_eq!(fs::read_dir(dir.join("private")).unwrap().count(), 0);
    groups_gone(dir).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_io_and_natural_exit_after_stdout_eof() {
    // Consume stdin before natural exit so these fixtures do not race the
    // writer; early-close/write-failure behavior has separate large-input cases.
    for body in [
        "IFS= read -r line; /bin/cat >/dev/null; printf 'R:%s' \"$line\"",
        "/bin/cat >/dev/null; printf 'R:x'; exec >&-; /bin/sleep 0.2; exit 0",
        "/bin/cat >/dev/null; printf 'R:x'; exit 0",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path(), body, Duration::from_secs(3));
        let lease = e.acquire(&context()).unwrap();
        assert_eq!(
            lease.reduce(Filter::Log, "x\nrest").await,
            Reduced::Output("R:x".into())
        );
        assert_eq!(e.live_children(), 0);
        assert_eq!(e.available_permits(), 0);
        drop(lease);
        assert_eq!(e.available_permits(), 1);
        assert_eq!(
            fs::read_to_string(dir.path().join("argv.log")).unwrap(),
            "pipe --filter log\n"
        );
        cleaned(&e, dir.path()).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_failure_classes_are_bounded_and_reaped() {
    let cases = [
        (
            "/bin/cat >/dev/null; printf nope; exit 3",
            "input",
            Reason::ExitFailure,
        ),
        ("/bin/cat >/dev/null", "input", Reason::EmptyOutput),
        (
            "/bin/cat >/dev/null; printf '\\377\\376'",
            "input",
            Reason::InvalidUtf8,
        ),
        (
            // Keep the overflow producer alive through TERM so this positive
            // cleanup fixture reaches KILL without a Darwin exit-transition race.
            "/bin/cat >/dev/null; trap '' TERM; exec /usr/bin/head -c 2097152 /dev/zero",
            "input",
            Reason::Overflow,
        ),
        ("printf 'R:x'; exit 0", "large", Reason::WriteFailed),
        (
            "exec <&-; printf R:x; exec >&-; /bin/sleep 0.2; exit 0",
            "large",
            Reason::WriteFailed,
        ),
    ];
    for (body, input, reason) in cases {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path(), body, Duration::from_secs(3));
        let lease = e.acquire(&context()).unwrap();
        let input = if input == "large" {
            "x".repeat(512 * 1024)
        } else {
            input.into()
        };
        let started = tokio::time::Instant::now();
        assert_eq!(
            lease.reduce(Filter::Log, &input).await,
            Reduced::Unchanged(reason),
            "{body}"
        );
        if body.starts_with("exec <&-") {
            assert!(started.elapsed() >= Duration::from_millis(150));
        }
        assert_eq!(e.stats().process_failed, 1);
        assert!(!e.stats().quarantined, "{body}: {:?}", e.stats());
        drop(lease);
        cleaned(&e, dir.path()).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_environment_and_literal_stdin() {
    let dir = tempfile::tempdir().unwrap();
    // Inspect the executable's environment, not the shell's synthesized PATH.
    let body = format!(
        "/usr/bin/env > '{}/environment'; /bin/cat > '{}/input'; printf R:x",
        dir.path().display(),
        dir.path().display()
    );
    let e = engine(dir.path(), &body, Duration::from_secs(3));
    let lease = e.acquire(&context()).unwrap();
    let input = "$(touch $XDG_DATA_HOME/pwned); `id`";
    assert_eq!(
        lease.reduce(Filter::Log, input).await,
        Reduced::Output("R:x".into())
    );
    assert_eq!(fs::read_to_string(dir.path().join("input")).unwrap(), input);
    let environment = fs::read_to_string(dir.path().join("environment")).unwrap();
    for (key, child) in super::super::config::PRIVATE_KEYS {
        let line = environment
            .lines()
            .find(|s| s.starts_with(&format!("{key}=")))
            .unwrap();
        assert!(line.ends_with(child), "{line}");
        assert!(line.contains("/inv-0/"));
    }
    for (key, value) in super::super::config::FIXED_ENV {
        assert!(environment.lines().any(|s| s == format!("{key}={value}")));
    }
    assert!(
        !environment.lines().any(|s| s.starts_with("PATH=")),
        "{environment}"
    );
    drop(lease);
    cleaned(&e, dir.path()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overflow_keeps_the_reader_open_until_the_group_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        dir.path(),
        // Keep the overflow producer alive through TERM so this positive
        // cleanup fixture reaches KILL without a Darwin exit-transition race.
        "/bin/cat >/dev/null; trap '' TERM; exec /usr/bin/head -c 2097152 /dev/zero",
        Duration::from_secs(10),
    );
    let entered = Arc::new(Semaphore::new(0));
    let gate = Arc::new(Semaphore::new(0));
    {
        let mut hooks = e.hooks.lock().unwrap();
        hooks.at_reap = Some(entered.clone());
        hooks.before_reap = Some(gate.clone());
    }
    let lease = e.acquire(&context()).unwrap();
    let task = tokio::spawn(async move { lease.reduce(Filter::Log, "input").await });
    tokio::time::timeout(Duration::from_secs(3), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let group = e.hooks.lock().unwrap().spawned_groups[0].to_string();
    let ps = std::process::Command::new("ps")
        .args(["-A", "-o", "pid=,pgid=,stat="])
        .output()
        .unwrap();
    assert!(ps.status.success());
    assert!(
        String::from_utf8(ps.stdout).unwrap().lines().any(|line| {
            let fields: Vec<_> = line.split_whitespace().collect();
            fields.len() == 3
                && fields[0] == group
                && fields[1] == group
                && !fields[2].starts_with('Z')
        }),
        "overflow producer must remain alive until the sweep"
    );
    assert_eq!(e.available_permits(), 0);
    gate.add_permits(1);
    assert_eq!(task.await.unwrap(), Reduced::Unchanged(Reason::Overflow));
    assert!(!e.stats().quarantined);
    cleaned(&e, dir.path()).await;
    assert_eq!(e.available_permits(), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_holds_permit_until_reap_and_deadline_spans_fields() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        dir.path(),
        "trap '' TERM; exec /bin/sleep 60",
        Duration::from_millis(100),
    );
    let gate = Arc::new(Semaphore::new(0));
    e.hooks.lock().unwrap().before_reap = Some(gate.clone());
    let lease = e.acquire(&context()).unwrap();
    let task = tokio::spawn(async move {
        let first = lease.reduce(Filter::Log, "input").await;
        (first, lease)
    });
    wait_live(&e, dir.path()).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(e.available_permits(), 0);
    assert_eq!(e.acquire(&context()).err().unwrap(), Reason::Capacity);
    gate.add_permits(1);
    let (result, lease) = task.await.unwrap();
    assert_eq!(result, Reduced::Unchanged(Reason::Timeout));
    assert_eq!(
        lease.reduce(Filter::Log, "input").await,
        Reduced::Unchanged(Reason::Timeout)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("argv.log"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    assert_eq!(e.stats().timed_out, 2);
    assert_eq!(e.stats().capacity_denied, 1);
    drop(lease);
    assert_eq!(e.available_permits(), 1);
    cleaned(&e, dir.path()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_callback_drop_and_detached_tracker_close() {
    for mode in ["cancel", "drop", "close"] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(
            dir.path(),
            "trap '' TERM; exec /bin/sleep 60",
            Duration::from_secs(10),
        );
        let ctx = context();
        let token = ctx.cancellation().clone();
        let reap_gate = (mode == "drop").then(|| Arc::new(Semaphore::new(0)));
        e.hooks.lock().unwrap().before_reap = reap_gate.clone();
        let lease = e.acquire(&ctx).unwrap();
        let task = tokio::spawn(async move { lease.reduce(Filter::Log, "input").await });
        wait_live(&e, dir.path()).await;
        match mode {
            "cancel" => {
                token.cancel();
                assert_eq!(task.await.unwrap(), Reduced::Unchanged(Reason::Cancelled));
            }
            "drop" => {
                task.abort();
                let _ = task.await;
                assert_eq!(
                    e.available_permits(),
                    0,
                    "owner must hold the lease after callback drop"
                );
                assert_eq!(e.live_children(), 1);
                reap_gate.as_ref().unwrap().add_permits(1);
            }
            _ => {
                e.close(Duration::from_secs(4)).await.unwrap();
                assert_eq!(task.await.unwrap(), Reduced::Unchanged(Reason::Closed));
            }
        }
        cleaned(&e, dir.path()).await;
        assert_eq!(e.available_permits(), 1);
        assert_eq!(e.stats().cancelled, 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_races_reserved_owner_and_is_final() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), "printf R:x", Duration::from_secs(10));
    let gate = Arc::new(Semaphore::new(0));
    e.hooks.lock().unwrap().after_live_increment = Some(gate.clone());
    let lease = e.acquire(&context()).unwrap();
    let task = tokio::spawn(async move { lease.reduce(Filter::Log, "input").await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while e.live_children() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let engine = e.clone();
    let close = tokio::spawn(async move { engine.close(Duration::from_secs(3)).await });
    e.closing.cancelled().await;
    assert!(!close.is_finished());
    gate.add_permits(1);
    assert_eq!(task.await.unwrap(), Reduced::Unchanged(Reason::Closed));
    close.await.unwrap().unwrap();
    assert_eq!(e.acquire(&context()).err().unwrap(), Reason::Closed);
    assert!(e.stats().closed);
    assert!(!dir.path().join("argv.log").exists());
    cleaned(&e, dir.path()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_failure_quarantines_and_close_retries() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(
        dir.path(),
        "/bin/cat >/dev/null; printf R:x",
        Duration::from_secs(3),
    );
    e.hooks.lock().unwrap().fail_remove = true;
    let lease = e.acquire(&context()).unwrap();
    assert_eq!(
        lease.reduce(Filter::Log, "input").await,
        Reduced::Output("R:x".into())
    );
    drop(lease);
    assert_eq!(e.live_children(), 1);
    assert!(e.stats().quarantined);
    assert_eq!(e.acquire(&context()).err().unwrap(), Reason::Quarantined);
    e.hooks.lock().unwrap().fail_remove = false;
    cleaned(&e, dir.path()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_removal_timeout_quarantines_without_hanging_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let initial = engine(
        dir.path(),
        "/bin/cat >/dev/null; printf R:x",
        Duration::from_secs(3),
    );
    let mut c = initial.configuration.clone();
    c.limits.kill_wait = Duration::from_millis(50);
    let e = Arc::new(Engine::new(c));
    e.hooks.lock().unwrap().before_remove = Some(Arc::new(Semaphore::new(0)));
    let lease = e.acquire(&context()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), lease.reduce(Filter::Log, "input"))
            .await
            .unwrap(),
        Reduced::Output("R:x".into())
    );
    drop(lease);
    assert_eq!(e.available_permits(), 1);
    assert_eq!(e.live_children(), 1);
    assert!(e.stats().quarantined);
    cleaned(&e, dir.path()).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unproved_reap_and_panic_never_release_permit() {
    for panic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(
            dir.path(),
            "/bin/cat >/dev/null; printf R:x",
            Duration::from_secs(3),
        );
        {
            let mut hooks = e.hooks.lock().unwrap();
            hooks.panic_after_spawn = panic;
            hooks.reap_timeout_once = !panic;
        }
        let owner = crabber::extension::CleanupOwner::new();
        let ctx = context().with_cleanup(owner.tracker());
        let lease = e.acquire(&ctx).unwrap();
        let result = lease.reduce(Filter::Log, "input").await;
        assert!(matches!(result, Reduced::Unchanged(_)));
        drop(lease);
        // A panic drops the sender before the outer guard records quarantine.
        owner.join(Duration::from_secs(3)).await.unwrap();
        assert_eq!(e.available_permits(), 0);
        assert_eq!(e.live_children(), 1);
        assert!(e.stats().quarantined);
        assert!(
            e.close(Duration::from_millis(300))
                .await
                .unwrap_err()
                .to_string()
                .ends_with("cleanup-incomplete")
        );
        assert_eq!(e.live_children(), 1);
        assert_eq!(fs::read_dir(dir.path().join("private")).unwrap().count(), 0);
        let groups = e.hooks.lock().unwrap().spawned_groups.clone();
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let ps = std::process::Command::new("ps")
                    .args(["-A", "-o", "pid=,pgid=,stat="])
                    .output()
                    .unwrap();
                assert!(ps.status.success());
                let listing = String::from_utf8(ps.stdout).unwrap();
                if !listing.lines().any(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|v| v.parse::<i32>().ok())
                        .is_some_and(|g| groups.contains(&g))
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if panic {
            assert_eq!(e.stats().process_failed, 1);
        }
        groups_gone(dir.path()).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_collision_and_independent_instance_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let a = engine(
        dir.path(),
        "/bin/cat >/dev/null; printf R:x",
        Duration::from_secs(3),
    );
    let b = Arc::new(Engine::new(a.configuration.clone()));
    let stale = dir
        .path()
        .join("private")
        .join(format!(".rtk-reducer-{}-0", std::process::id()));
    fs::create_dir(&stale).unwrap();
    fs::write(stale.join("keep"), "stale").unwrap();
    for e in [&a, &b] {
        let lease = e.acquire(&context()).unwrap();
        assert_eq!(
            lease.reduce(Filter::Log, "input").await,
            Reduced::Output("R:x".into())
        );
    }
    assert_ne!(a.state.lock().unwrap().root, b.state.lock().unwrap().root);
    a.close(Duration::from_secs(3)).await.unwrap();
    b.close(Duration::from_secs(3)).await.unwrap();
    assert_eq!(fs::read_to_string(stale.join("keep")).unwrap(), "stale");
    fs::remove_dir_all(stale).unwrap();
    groups_gone(dir.path()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_non_utf8_environment_and_collision_exhaustion_are_pass_through() {
    use std::os::unix::{ffi::OsStringExt, fs::PermissionsExt};
    for mode in ["spawn", "non_utf8", "collisions"] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(
            dir.path(),
            "/bin/cat >/dev/null; printf R:x",
            Duration::from_secs(3),
        );
        let e = if mode == "non_utf8" {
            let mut c = e.configuration.clone();
            c.temp_root = c.temp_root.join(std::ffi::OsString::from_vec(vec![0xff]));
            if let Err(error) = fs::create_dir(&c.temp_root) {
                #[cfg(not(target_os = "macos"))]
                panic!("non-UTF-8 fixture directory: {error}");
                #[cfg(target_os = "macos")]
                {
                    // APFS rejects this native name before it can become an
                    // invocation environment. Prove that earlier public boundary;
                    // filesystems accepting the name run the encoding proof below.
                    assert_eq!(error.raw_os_error(), Some(92), "expected EILSEQ");
                    let mut options = super::config::options(dir.path());
                    options.temp_root = c.temp_root;
                    assert!(
                        validate(options)
                            .err()
                            .unwrap()
                            .to_string()
                            .ends_with("temp-root")
                    );
                    assert!(!dir.path().join("argv.log").exists());
                    assert_eq!(e.live_children(), 0);
                    assert_eq!(e.available_permits(), 1);
                    cleaned(&e, dir.path()).await;
                    continue;
                }
            }
            fs::set_permissions(&c.temp_root, fs::Permissions::from_mode(0o700)).unwrap();
            Arc::new(Engine::new(c))
        } else {
            e
        };
        if mode == "spawn" {
            fs::remove_file(&e.configuration.executable).unwrap();
        }
        if mode == "collisions" {
            for n in 0..16 {
                fs::create_dir(
                    e.configuration
                        .temp_root
                        .join(format!(".rtk-reducer-{}-{n}", std::process::id())),
                )
                .unwrap();
            }
        }
        let lease = e.acquire(&context()).unwrap();
        assert_eq!(
            lease.reduce(Filter::Log, "input").await,
            Reduced::Unchanged(if mode == "spawn" {
                Reason::Spawn
            } else {
                Reason::PrivateDirs
            })
        );
        assert!(!dir.path().join("argv.log").exists());
        assert_eq!(e.live_children(), 0);
        drop(lease);
        e.close(Duration::from_secs(3)).await.unwrap();
        if mode == "collisions" {
            assert_eq!(
                fs::read_dir(&e.configuration.temp_root).unwrap().count(),
                16
            );
        } else {
            assert_eq!(fs::read_dir(&e.configuration.temp_root).unwrap().count(), 0);
        }
    }
}
