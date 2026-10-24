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

#[test]
fn intermediate_symlink_routes_are_checked_before_the_final_target() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("public")).unwrap();
    std::fs::create_dir(temp.path().join("secrets")).unwrap();
    std::fs::write(temp.path().join("public/file"), "allowed fixture").unwrap();
    symlink("../public", temp.path().join("secrets/link")).unwrap();
    symlink("secrets/link", temp.path().join("alias")).unwrap();
    symlink("public", temp.path().join("blocked-link")).unwrap();
    symlink("blocked-link", temp.path().join("basename-alias")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["secrets", "blocked-link"]);
    for name in [
        "alias/file",
        "alias/new",
        "basename-alias/file",
        "basename-alias/new",
    ] {
        let error = deny::check(&root, &deny, Path::new(name)).unwrap_err();
        assert_eq!(error.category, category::PATH_DENIED, "{name}");
        assert_eq!(error.message, "path denied by host policy");
    }
    assert!(deny::check(&root, &deny, Path::new("public/file")).is_ok());
}

#[test]
fn stepwise_resolution_preserves_capability_and_missing_tail_contracts() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("a/b")).unwrap();
    std::fs::create_dir(temp.path().join("public")).unwrap();
    std::fs::write(temp.path().join("public/file"), "allowed fixture").unwrap();
    symlink("../../public", temp.path().join("a/b/up")).unwrap();
    symlink("public", temp.path().join("allowed-alias")).unwrap();
    symlink("new/nested", temp.path().join("missing")).unwrap();
    symlink("../outside", temp.path().join("escape")).unwrap();
    symlink(temp.path().join("public"), temp.path().join("absolute")).unwrap();
    symlink("public/file/../new", temp.path().join("not-directory")).unwrap();
    symlink("public/file/", temp.path().join("trailing-slash")).unwrap();
    symlink("public/file/.", temp.path().join("trailing-dot")).unwrap();
    symlink("new/../public", temp.path().join("missing-parent")).unwrap();
    for index in 0..41 {
        symlink(
            format!("hop{}", index + 1),
            temp.path().join(format!("hop{index}")),
        )
        .unwrap();
    }
    symlink("public", temp.path().join("hop41")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["excluded"]);
    for (requested, resolved) in [
        ("a/b/up/file", "public/file"),
        ("allowed-alias/new/file", "public/new/file"),
    ] {
        assert_eq!(
            deny::check(&root, &deny, Path::new(requested))
                .unwrap()
                .resolved,
            Path::new(resolved)
        );
    }
    for (name, category) in [
        ("escape/file", category::PATH_ESCAPE),
        ("absolute/file", category::PATH_ESCAPE),
        ("not-directory", category::NOT_DIRECTORY),
        ("trailing-slash", category::NOT_DIRECTORY),
        ("trailing-dot", category::NOT_DIRECTORY),
        ("missing-parent", category::PATH_DENIED),
        ("missing/file", category::PATH_DENIED),
        ("hop0/file", category::PATH_DENIED),
    ] {
        assert_eq!(
            deny::check(&root, &deny, Path::new(name))
                .unwrap_err()
                .category,
            category,
            "{name}"
        );
    }
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

#[test]
fn empty_policy_preserves_unreadable_prefix_and_enabled_policy_fails_closed() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let directory = temp.path().join("blocked");
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o0)).unwrap();
    let none = deny::check(&root, &PathDenyPolicy::none(), Path::new("blocked/file"));
    let enabled = deny::check(&root, &policy(&[".env"]), Path::new("blocked/file"));
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(none.is_ok());
    // Root can traverse mode-000 directories; normal CI workers cannot.
    if !rustix::process::geteuid().is_root() {
        let error = enabled.unwrap_err();
        assert_eq!(error.category, category::PATH_DENIED);
        assert_eq!(error.message, "path denied by host policy");
    }
}
