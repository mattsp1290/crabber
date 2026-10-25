use super::*;

fn isolated(test: &str) -> bool {
    const HELPER: &str = "XTASK_ORPHAN_REAP_HELPER";
    if std::env::var(HELPER).as_deref() != Ok(test) {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture", "--test-threads=1"])
            .env(HELPER, test)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated supervisor failed\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return false;
    }
    enable_tree_supervision().unwrap();
    true
}

#[test]
fn orphan_group_disappears_before_command_exits() {
    if !isolated("process::reaping_tests::orphan_group_disappears_before_command_exits") {
        return;
    }
    let mut command = Command::new("python3");
    command.args([
        "-c",
        r#"
import os
from pathlib import Path
import signal
import time

read, write = os.pipe()
parent = os.fork()
if parent == 0:
    os.close(read)
    os.setsid()
    orphan = os.fork()
    if orphan == 0:
        os.close(write)
        time.sleep(30)
        os._exit(0)
    os.write(write, f"{os.getpid()} {orphan}".encode())
    os._exit(0)
os.close(write)
group, orphan = map(int, os.read(read, 128).split())
os.close(read)
os.waitpid(parent, 0)
os.killpg(group, signal.SIGKILL)
deadline = time.monotonic() + 3
while time.monotonic() < deadline:
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        print(f"orphan group {group} disappeared before command {os.getpid()} exited")
        raise SystemExit(37)
    time.sleep(0.01)
print(f"supervisor={os.getppid()} command={os.getpid()} group={group} orphan={orphan}")
print(Path(f"/proc/{orphan}/stat").read_text())
raise SystemExit(91)
"#,
    ]);
    let output = output(&mut command, &AtomicUsize::new(0)).unwrap();
    assert_eq!(
        output.status.code(),
        Some(37),
        "orphan group must disappear while the command runs and its nonzero exit must be preserved\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("disappeared before command"));
    verify_drained().unwrap();
}

#[test]
fn other_managed_children_keep_their_exit_status() {
    if !isolated("process::reaping_tests::other_managed_children_keep_their_exit_status") {
        return;
    }
    let mut sibling = spawn(Command::new("sh").args(["-c", "exit 29"])).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{}/stat", sibling.child.id())).unwrap();
        if stat.rsplit_once(") ").unwrap().1.starts_with("Z ") {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "child did not exit");
        std::thread::sleep(Duration::from_millis(10));
    }
    let status = status(
        Command::new("sh").args(["-c", "exit 37"]),
        &AtomicUsize::new(0),
    )
    .unwrap();
    assert_eq!(status.code(), Some(37));
    assert_eq!(sibling.child.wait().unwrap().code(), Some(29));
    verify_drained().unwrap();
}

#[test]
fn concurrent_spawn_keeps_exit_statuses() {
    if !isolated("process::reaping_tests::concurrent_spawn_keeps_exit_statuses") {
        return;
    }
    std::thread::scope(|scope| {
        for code in 20..24 {
            scope.spawn(move || {
                for _ in 0..8 {
                    let status = status(
                        Command::new("sh").args(["-c", &format!("exit {code}")]),
                        &AtomicUsize::new(0),
                    )
                    .unwrap();
                    assert_eq!(status.code(), Some(code));
                }
            });
        }
    });
    verify_drained().unwrap();
}

fn escaped_helper_command(path: &std::path::Path, keep_parent: bool) -> Command {
    let mut command = Command::new("python3");
    command.args([
        "-c",
        r#"
import os
from pathlib import Path
import sys
import time

ready = Path(sys.argv[1])
child = os.fork()
if child == 0:
    os.setsid()
    ready.write_text(str(os.getpid()))
    time.sleep(30)
    os._exit(0)
deadline = time.monotonic() + 3
while not ready.exists():
    assert time.monotonic() < deadline, "escaped helper did not start"
    time.sleep(0.01)
if sys.argv[2] == "hold":
    time.sleep(30)
"#,
    ]);
    command
        .arg(path)
        .arg(if keep_parent { "hold" } else { "exit" });
    command
}

#[test]
fn leaked_escaped_helper_is_reported_and_reaped() {
    if !isolated("process::reaping_tests::leaked_escaped_helper_is_reported_and_reaped") {
        return;
    }
    let path = std::env::temp_dir().join(format!("xtask-leaked-helper-{}", std::process::id()));
    let error = status(
        &mut escaped_helper_command(&path, false),
        &AtomicUsize::new(0),
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("command left a running descendant")
    );
    assert_helper_gone(&path);
    verify_drained().unwrap();
}

#[test]
fn cancellation_reaps_escaped_helper() {
    if !isolated("process::reaping_tests::cancellation_reaps_escaped_helper") {
        return;
    }
    let path = std::env::temp_dir().join(format!("xtask-cancelled-helper-{}", std::process::id()));
    let signal = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !path.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            signal.store(2, Ordering::SeqCst);
        });
        assert!(
            !status(&mut escaped_helper_command(&path, true), &signal)
                .unwrap()
                .success()
        );
    });
    assert_helper_gone(&path);
    verify_drained().unwrap();
}

fn assert_helper_gone(path: &std::path::Path) {
    let pid = std::fs::read_to_string(path).unwrap();
    assert!(!std::path::Path::new(&format!("/proc/{pid}")).exists());
    std::fs::remove_file(path).unwrap();
}
