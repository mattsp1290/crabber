//! Deny dialect, identity, and capability-resolution contracts.
use crabber_tools_core::{PathDenyPolicy, WorkspaceRoot, category, deny};
use std::{os::unix::fs::symlink, path::Path};

fn policy(patterns: &[&str]) -> PathDenyPolicy {
    PathDenyPolicy::new(patterns.iter().map(|pattern| (*pattern).to_owned())).unwrap()
}

#[test]
fn dialect_and_ancestor_matching() {
    for (patterns, accepted, excluded) in [
        (vec!["*.pem"], vec!["key.pem", "a/b/key.pem"], vec!["pem/x"]),
        (
            vec![".git/config"],
            vec![".git/config"],
            vec!["sub/.git/config"],
        ),
        (
            vec!["**/.git/config"],
            vec![".git/config", "sub/.git/config"],
            vec!["config"],
        ),
        (
            vec![".env.*"],
            vec![".env.local", "a/.env.local"],
            vec![".env"],
        ),
        (
            vec!["secrets"],
            vec!["secrets/a/b", "sub/secrets/a"],
            vec!["secret/a"],
        ),
    ] {
        let deny = policy(&patterns);
        for path in accepted {
            assert!(deny.matches(Path::new(path)), "{patterns:?}: {path}");
        }
        for path in excluded {
            assert!(!deny.matches(Path::new(path)), "{patterns:?}: {path}");
        }
        for root in ["", "."] {
            assert!(!deny.matches(Path::new(root)));
        }
    }
}

#[test]
fn invalid_patterns_are_plan_errors() {
    for pattern in [
        "", "/x", "./x", "a/../b", "a//b", "a/", "x\0y", "[ab]", "!x",
    ] {
        assert!(
            matches!(PathDenyPolicy::new([pattern.to_owned()]), Err(crabber::ExtensionError::Plan(message)) if message == "deny")
        );
    }
    for patterns in [
        vec!["x".to_owned(); 257],
        vec!["x".repeat(4097)],
        vec!["{".repeat(17)],
    ] {
        assert!(PathDenyPolicy::new(patterns).is_err());
    }
    assert!(PathDenyPolicy::new(["x".repeat(4096)]).is_ok());
}

#[test]
fn serialization_is_sorted_and_deduplicated() {
    assert_eq!(policy(&["b", "a", "a"]), policy(&["a", "b"]));
    assert_eq!(
        serde_json::to_value(policy(&["b", "a", "a"])).unwrap(),
        serde_json::json!(["a", "b"])
    );
    assert_eq!(
        serde_json::to_value(PathDenyPolicy::none()).unwrap(),
        serde_json::json!([])
    );
}

#[test]
fn resolving_checks_deny_aliases_and_missing_tails() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join(".git")).unwrap();
    std::fs::write(temp.path().join(".env"), "synthetic fixture").unwrap();
    symlink(".env", temp.path().join("notes")).unwrap();
    symlink("notes", temp.path().join("chain")).unwrap();
    symlink(".git", temp.path().join("d")).unwrap();
    symlink("loop", temp.path().join("loop")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&[".env", ".git/config"]);
    for name in [".env", "notes", "chain", "d/config", "loop"] {
        let error = deny::check(&root, &deny, Path::new(name)).unwrap_err();
        assert_eq!(error.category, category::PATH_DENIED, "{name}");
        assert_eq!(error.message, "path denied by host policy");
    }
    assert_eq!(
        deny::check(&root, &deny, Path::new("d/new/file"))
            .unwrap()
            .resolved,
        Path::new(".git/new/file")
    );
    assert!(deny::check(&root, &deny, Path::new("new/file")).is_ok());
    let none = PathDenyPolicy::none();
    for name in ["loop", ".env/child", "missing/child"] {
        assert!(deny::check(&root, &none, Path::new(name)).is_ok());
    }
    assert_eq!(
        deny::check(&root, &deny, Path::new(".env/child"))
            .unwrap_err()
            .category,
        category::PATH_DENIED
    );
    std::fs::write(temp.path().join("plain"), "").unwrap();
    assert_eq!(
        deny::check(&root, &deny, Path::new("plain/child"))
            .unwrap_err()
            .category,
        category::NOT_DIRECTORY
    );
}

#[cfg(target_os = "linux")]
#[test]
fn opened_handle_is_checked_before_reading() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join(".env"), "synthetic fixture").unwrap();
    std::fs::write(temp.path().join("ok"), "allowed").unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&[".env"]);
    let file = root.dir().open(".env").unwrap();
    assert_eq!(
        deny::verify_open(&root, &deny, &file).unwrap_err().category,
        category::PATH_DENIED
    );
    let allowed = root.dir().open("ok").unwrap();
    assert!(deny::verify_open(&root, &deny, &allowed).is_ok());
    std::fs::remove_file(temp.path().join("ok")).unwrap();
    assert_eq!(
        deny::verify_open(&root, &deny, &allowed)
            .unwrap_err()
            .category,
        category::PATH_DENIED
    );
    assert!(deny::verify_open(&root, &PathDenyPolicy::none(), &allowed).is_ok());
}
