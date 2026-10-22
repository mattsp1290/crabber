use super::*;
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct Shells {
    bash: PathBuf,
    posix: bool,
}
struct Fixture {
    directory: tempfile::TempDir,
    bin: PathBuf,
}

// Each child is owned by its watchdog thread. The thread polls without holding
// a mutex across wait, kills at two seconds, and reaps before returning.
fn run(
    shell: &Path,
    args: &[&str],
    script: &str,
    f: &Fixture,
    path: &str,
    capture: bool,
) -> std::process::Output {
    let mut command = Command::new(shell);
    command
        .args(args)
        .arg("-c")
        .arg("--")
        .arg(script)
        .env_clear()
        .env("PATH", path)
        .env("HOME", f.directory.path())
        .env("ENV", "")
        .env("BASH_ENV", "")
        .env("CANARY", f.directory.path().join("canary"))
        .env("ARGV_OUT", f.directory.path().join("argv"))
        .current_dir(f.directory.path())
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(if capture {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    let mut child = command.spawn().unwrap();
    std::thread::spawn(move || {
        let begin = Instant::now();
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if begin.elapsed() >= Duration::from_secs(2) {
                let _ = child.kill();
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        child.wait_with_output().unwrap()
    })
    .join()
    .unwrap()
}
fn write_executable(path: &Path, text: &str) {
    std::fs::write(path, text).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let bin = directory.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        write_executable(
            &bin.join("blocked"),
            "#!/bin/sh\n: > \"$CANARY.blocked\"\nexit 1\n",
        );
        write_executable(
            &bin.join("git"),
            "#!/bin/sh\nif [ \"$1\" = push ]; then : > \"$CANARY.git\"; fi\nexit 1\n",
        );
        write_executable(&bin.join("tool"), "#!/bin/sh\nexit 0\n");
        write_executable(
            &bin.join("sudo"),
            "#!/bin/sh\nwhile [ $# -gt 0 ]; do\ncase \"$1\" in -u|-g|-D) shift; [ $# -gt 0 ] || exit 1; shift;; -n|-E|-H|--user=*|--group=*|--chdir=*) shift;; --) shift; break;; -*) exit 1;; *=*) shift;; *) break;; esac\ndone\n[ $# -gt 0 ] || exit 1\nexec \"$@\"\n",
        );
        write_executable(
            &bin.join("timeout"),
            "#!/bin/sh\nwhile [ $# -gt 0 ]; do\ncase \"$1\" in -s|-k) shift; [ $# -gt 0 ] || exit 1; shift;; --signal=*|--kill-after=*|--foreground|--preserve-status|-v|--verbose) shift;; --) shift; break;; -*) exit 1;; *) break;; esac\ndone\n[ $# -ge 2 ] || exit 1\nshift\nexec \"$@\"\n",
        );
        Self { directory, bin }
    }
    fn path(&self) -> String {
        format!("{}:/usr/bin:/bin", self.bin.display())
    }
    fn reset(&self) {
        for name in ["canary.blocked", "canary.git"] {
            let _ = std::fs::remove_file(self.directory.path().join(name));
        }
    }
    fn matched(&self) -> bool {
        ["canary.blocked", "canary.git"]
            .into_iter()
            .any(|name| self.directory.path().join(name).exists())
    }
}
fn skip(text: &str) {
    if std::env::var("COMMAND_GUARD_REQUIRE_SHELLS").as_deref() == Ok("1") {
        panic!("{text}");
    }
    eprintln!("{text}");
}
fn discover(f: &Fixture) -> Option<Shells> {
    let candidates = match std::env::var_os("COMMAND_GUARD_TEST_BASH") {
        Some(path) => vec![PathBuf::from(path)],
        None => ["/opt/homebrew/bin/bash", "/usr/local/bin/bash", "/bin/bash"]
            .into_iter()
            .map(PathBuf::from)
            .collect(),
    };
    let version = |shell: &Path, bash: bool| {
        let args = if bash {
            vec!["--noprofile", "--norc"]
        } else {
            vec![]
        };
        let output = run(
            shell,
            &args,
            "printf %s \"${BASH_VERSINFO[0]}\"",
            f,
            "/usr/bin:/bin",
            true,
        );
        String::from_utf8_lossy(&output.stdout).parse::<u32>().ok()
    };
    let bash = candidates
        .into_iter()
        .find(|p| p.exists() && version(p, true).is_some_and(|n| n >= 5));
    let Some(bash) = bash else {
        skip("SKIPPED: command_guard shell differential (no Bash 5+)");
        return None;
    };
    // Dash rejects Bash's array syntax, producing no stdout; it is usable.
    let posix = Path::new("/bin/sh").exists()
        && version(Path::new("/bin/sh"), false).is_none_or(|n| n >= 5);
    if !posix {
        skip("SKIPPED: command_guard posix shell rows (/bin/sh is Bash <5)");
    }
    Some(Shells { bash, posix })
}
fn invocation(shells: &Shells, d: Dialect) -> Vec<(&Path, Vec<&'static str>)> {
    if d == Dialect::Bash {
        vec![(&shells.bash, vec!["--noprofile", "--norc"])]
    } else {
        let mut rows = vec![(
            shells.bash.as_path(),
            vec!["--noprofile", "--norc", "--posix"],
        )];
        if shells.posix {
            rows.push((Path::new("/bin/sh"), vec![]));
        }
        rows
    }
}
#[test]
fn accepted_scripts_are_valid_shell() {
    let f = Fixture::new();
    let Some(shells) = discover(&f) else {
        return;
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/go-reference-corpus.json")).unwrap();
    let p = policy();
    let mut checked = 0;
    for row in fixture["rows"].as_array().unwrap() {
        let d = if row["dialect"] == "bash" {
            Dialect::Bash
        } else {
            Dialect::Posix
        };
        let s = row["script"].as_str().unwrap();
        if !matches!(
            p.analyze_script(s, d),
            Outcome::Abstain | Outcome::RuleMatch
        ) {
            continue;
        }
        for (shell, mut args) in invocation(&shells, d) {
            args.push("-n");
            assert!(
                run(shell, &args, s, &f, "/usr/bin:/bin", false)
                    .status
                    .success(),
                "accepted invalid {d:?}: {s:?}"
            );
            checked += 1;
        }
    }
    eprintln!("shell syntax checks run: {checked}");
    assert!(checked > 0);
}
#[test]
fn known_argv_matches_real_shell() {
    let f = Fixture::new();
    let Some(shells) = discover(&f) else {
        return;
    };
    write_executable(
        &f.bin.join("tool"),
        "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$ARGV_OUT\"\n",
    );
    let rows: Vec<(&str, Vec<&str>)> = vec![
        ("tool ''", vec![""]),
        ("tool λ", vec!["λ"]),
        ("tool \"a\\qb\"", vec!["a\\qb"]),
        ("tool \"a\\$b\"", vec!["a$b"]),
        ("tool a\\ b", vec!["a b"]),
        ("tool '*'", vec!["*"]),
        ("tool \\~", vec!["~"]),
        ("tool \"a\\\\b\"", vec!["a\\b"]),
        ("tool 'a\r\nb'", vec!["a\r\nb"]),
        ("tool a\\ b \"c d\" 'e'f", vec!["a b", "c d", "ef"]),
        ("tool \"$'x'\" \"cost$\"", vec!["$'x'", "cost$"]),
        ("tool '{a,b}' '*?[~'", vec!["{a,b}", "*?[~"]),
        ("tool a\\\nb", vec!["ab"]),
        ("tool \\", vec!["\\"]),
    ];
    for (s, expected) in rows {
        let mut o = options();
        o.rules = vec![Rule {
            id: "argv".into(),
            executable: "tool".into(),
            arg_prefix: expected.iter().map(|s| (*s).into()).collect(),
        }];
        let p = Policy::new(o).unwrap();
        assert_eq!(
            p.analyze_script(s, Dialect::Posix),
            Outcome::RuleMatch,
            "unknown decoded argv: {s:?}"
        );
        let expected: Vec<u8> = expected
            .iter()
            .flat_map(|s| s.bytes().chain(std::iter::once(0)))
            .collect();
        for (shell, args) in invocation(&shells, Dialect::Posix) {
            assert!(
                run(shell, &args, s, &f, f.bin.to_str().unwrap(), false)
                    .status
                    .success()
            );
            assert_eq!(
                std::fs::read(f.directory.path().join("argv")).unwrap(),
                expected,
                "{s:?}"
            );
        }
    }
}
#[test]
fn execution_canary_implies_deny() {
    let f = Fixture::new();
    let Some(shells) = discover(&f) else {
        return;
    };
    let path = f.path();
    for (shell, args) in invocation(&shells, Dialect::Posix) {
        let output = run(shell, &args, "command -v sudo", &f, &path, true);
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            f.bin.join("sudo").to_str().unwrap()
        );
    }
    let mut rows = generated_corpus(0xca7a_7e11, 3000);
    for s in &rows {
        // Closed generator has no absolute executable paths. Its -p spellings
        // never combine with sudo or timeout in this pinned corpus.
        assert!(
            !(s.contains("command -p") && (s.contains("sudo") || s.contains("timeout"))),
            "unsafe generated wrapper lookup"
        );
    }
    rows.extend(
        serde_json::from_str::<Vec<String>>(include_str!("fixtures/adversarial.json")).unwrap(),
    );
    rows.push(format!("echo {}\nblocked", "\\".repeat(2)));
    let p = policy();
    let mut written = 0;
    for s in rows {
        for d in [Dialect::Posix, Dialect::Bash] {
            for (shell, args) in invocation(&shells, d) {
                f.reset();
                let _ = run(shell, &args, &s, &f, &path, false);
                if f.matched() {
                    assert!(
                        p.analyze_script(&s, d).denies(),
                        "executed guarded command: {d:?} {s:?}"
                    );
                    written += 1;
                }
            }
        }
    }
    eprintln!("execution canaries written: {written}");
    assert!(written > 0);
}
#[test]
fn implicit_execution_references() {
    let f = Fixture::new();
    let Some(shells) = discover(&f) else {
        return;
    };
    // Bash 5 as `sh` demonstrates this implicit interpretation. Dash in this
    // environment does not expand an inherited PS4's external substitution.
    std::os::unix::fs::symlink(&shells.bash, f.bin.join("sh")).unwrap();
    let path = f.path();
    let p = policy();
    for s in [
        "OPTIND='a[$(blocked)0]'",
        "RANDOM='a[$(blocked)0]'",
        "for OPTIND in 'a[$(blocked)0]'; do :; done",
        "PS4='$(blocked)'; set -x; echo ok",
        "env 'PS4=$(blocked)' sh -x -c 'echo ok'",
        "env 'BASH_ENV=$(blocked)' bash -c 'echo ok'",
        "echo a # c \\\nblocked",
        "time blocked",
        "$'blocked'",
        "echo $'\\''\nblocked\necho '",
        "! ! blocked",
        "$\\\n(blocked)",
    ] {
        assert!(p.analyze_script(s, Dialect::Bash).denies(), "{s:?}");
        f.reset();
        let _ = run(
            &shells.bash,
            &["--noprofile", "--norc"],
            s,
            &f,
            &path,
            false,
        );
        assert!(f.matched(), "implicit reference did not execute: {s:?}");
    }
}

#[test]
fn joined_heredoc_delimiter_canaries_are_denied() {
    let f = Fixture::new();
    let Some(shells) = discover(&f) else {
        return;
    };
    let path = f.path();
    let p = policy();
    for s in [
        "cat <<EOF\nE\\\nOF\nblocked\nEOF\n",
        "cat <<EOF\n\\\nEOF\nblocked\nEOF\n",
        "cat <<EOF\nE\\\nO\\\nF\nblocked\nEOF\n",
        "cat <<-EOF\n\tE\\\nOF\nblocked\nEOF\n",
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(p.analyze_script(s, d), Outcome::Unanalysable);
            for (shell, args) in invocation(&shells, d) {
                f.reset();
                let _ = run(shell, &args, s, &f, &path, false);
                if shell == shells.bash {
                    assert!(f.matched(), "joined delimiter did not execute: {s:?}");
                }
            }
        }
    }
    for s in [
        "cat <<'EOF'\nE\\\nOF\nblocked\nEOF\n",
        "cat <<-\"EOF\"\n\tE\\\nOF\nblocked\nEOF\n",
    ] {
        for d in [Dialect::Posix, Dialect::Bash] {
            assert_eq!(p.analyze_script(s, d), Outcome::Abstain);
            for (shell, args) in invocation(&shells, d) {
                f.reset();
                let _ = run(shell, &args, s, &f, &path, false);
                assert!(!f.matched(), "quoted delimiter executed body: {s:?}");
            }
        }
    }
}
