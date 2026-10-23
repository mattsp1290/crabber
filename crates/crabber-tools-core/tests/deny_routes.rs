//! Scoped alias regressions, shared with PR #38 (b2700fe458047e38a8d5d27a19dec4aed5a45a4b).
use crabber_tools_core::{PathDenyPolicy, WorkspaceRoot, category, deny};
use std::{os::unix::fs::symlink, path::Path};

fn policy(patterns: &[&str]) -> PathDenyPolicy {
    PathDenyPolicy::new(patterns.iter().map(|pattern| (*pattern).to_owned())).unwrap()
}

#[test]
fn intermediate_symlink_routes_cannot_skip_denied_names() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/nested")).unwrap();
    std::fs::create_dir(temp.path().join("links")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("real", temp.path().join("denied-route")).unwrap();
    symlink("denied-route", temp.path().join("alias")).unwrap();
    symlink("../denied-route", temp.path().join("links/alias")).unwrap();
    symlink("real/file", temp.path().join("denied-file")).unwrap();
    symlink("denied-file", temp.path().join("file-alias")).unwrap();
    symlink("denied-route/../real", temp.path().join("parent-route")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["denied-route", "denied-file"]);
    for name in [
        "alias/file",
        "alias/new/tail",
        "links/alias/file",
        "file-alias",
        "parent-route/file",
    ] {
        let error = deny::check(&root, &deny, Path::new(name)).unwrap_err();
        assert_eq!(error.category, category::PATH_DENIED, "{name}");
        assert_eq!(error.message, "path denied by host policy");
    }
}

#[test]
fn intermediate_symlink_descendant_pattern_is_checked_before_route_disappears() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("real")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("real", temp.path().join("route")).unwrap();
    symlink("route", temp.path().join("alias")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["route/file", "route/new/*"]);
    for name in ["alias/file", "alias/new/tail"] {
        assert_eq!(
            deny::check(&root, &deny, Path::new(name))
                .unwrap_err()
                .category,
            category::PATH_DENIED,
            "{name}"
        );
    }
}

#[test]
fn hop_resolution_preserves_allowed_writer_tails_and_capability_errors() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/nested")).unwrap();
    std::fs::create_dir(temp.path().join("links")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    for (name, target) in [
        ("alias", "real"),
        ("chain", "alias"),
        ("links/up", "../real"),
        ("parent", "real/nested/.."),
        ("root", "real/.."),
        ("dangling", "absent"),
        ("bad-parent", "real/file/../nested"),
        ("bad-dot", "real/file/."),
        ("bad-slash", "real/file/"),
        ("escape", "../outside"),
        ("absolute", "/etc"),
        ("loop", "loop"),
    ] {
        symlink(target, temp.path().join(name)).unwrap();
    }
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["unrelated"]);
    for name in [
        "alias/new/file",
        "chain/new/file",
        "links/up/new/file",
        "parent/new/file",
    ] {
        assert_eq!(
            deny::check(&root, &deny, Path::new(name)).unwrap().resolved,
            Path::new("real/new/file"),
            "{name}"
        );
    }
    assert_eq!(
        deny::check(&root, &deny, Path::new("root/new/file"))
            .unwrap()
            .resolved,
        Path::new("new/file")
    );
    for (name, expected) in [
        ("dangling", category::PATH_DENIED),
        ("dangling/new", category::PATH_DENIED),
        ("loop", category::PATH_DENIED),
        ("bad-parent", category::NOT_DIRECTORY),
        ("bad-dot", category::NOT_DIRECTORY),
        ("bad-slash", category::NOT_DIRECTORY),
        ("escape", category::PATH_ESCAPE),
        ("absolute", category::PATH_ESCAPE),
        ("../real", category::PATH_ESCAPE),
    ] {
        assert_eq!(
            deny::check(&root, &deny, Path::new(name))
                .unwrap_err()
                .category,
            expected,
            "{name}"
        );
    }
}

#[test]
fn route_walk_has_a_bounded_total_symlink_budget() {
    let temp = tempfile::tempdir().unwrap();
    symlink(".", temp.path().join("back")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["unrelated"]);
    let allowed = std::iter::repeat_n("back", 40)
        .collect::<Vec<_>>()
        .join("/");
    assert_eq!(
        deny::check(&root, &deny, Path::new(&allowed))
            .unwrap()
            .resolved,
        Path::new(".")
    );
    let exhausted = format!("{allowed}/back");
    assert_eq!(
        deny::check(&root, &deny, Path::new(&exhausted))
            .unwrap_err()
            .category,
        category::PATH_DENIED
    );
}

#[test]
fn normalized_intermediate_descendants_remain_denied() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/sub")).unwrap();
    std::fs::create_dir(temp.path().join("safe")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("real", temp.path().join("route")).unwrap();
    symlink("route/sub/..", temp.path().join("parent")).unwrap();
    symlink("../safe", temp.path().join("real/blocked")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    for (name, pattern) in [
        ("parent/file", "route/file"),
        ("parent/new/file", "route/new/*"),
        ("parent/blocked", "route/blocked"),
    ] {
        assert_eq!(
            deny::check(&root, &policy(&[pattern]), Path::new(name))
                .unwrap_err()
                .category,
            category::PATH_DENIED,
            "{name}: {pattern}"
        );
    }
    for (name, target) in [("identity-dot", "."), ("identity-parent", "../real")] {
        symlink(target, temp.path().join("real").join(name)).unwrap();
        let caller = format!("via-{name}");
        symlink(format!("route/{name}/sub/.."), temp.path().join(&caller)).unwrap();
        assert_eq!(
            deny::check(
                &root,
                &policy(&["route/file"]),
                Path::new(&format!("{caller}/file"))
            )
            .unwrap_err()
            .category,
            category::PATH_DENIED,
            "{name}"
        );
    }
}

#[test]
fn route_frames_preserve_actual_navigation_not_lexical_parent_collapse() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/sub")).unwrap();
    std::fs::create_dir_all(temp.path().join("other/nested")).unwrap();
    std::fs::write(temp.path().join("other/file"), "fixture").unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("real", temp.path().join("route")).unwrap();
    symlink("../other/nested", temp.path().join("real/link")).unwrap();
    symlink("route/link/..", temp.path().join("different")).unwrap();
    symlink("route/../real", temp.path().join("reenter")).unwrap();
    symlink("real/sub/..", temp.path().join("target-parent")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    assert_eq!(
        deny::check(&root, &policy(&["route/file"]), Path::new("different/file"))
            .unwrap()
            .resolved,
        Path::new("other/file")
    );
    assert_eq!(
        deny::check(&root, &policy(&["route/file"]), Path::new("reenter/file"))
            .unwrap()
            .resolved,
        Path::new("real/file")
    );
    assert!(
        deny::check(
            &root,
            &policy(&["target-parent/sub"]),
            Path::new("target-parent/file")
        )
        .is_ok()
    );
}

#[test]
fn normalized_route_names_compose_across_aliases() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/sub")).unwrap();
    std::fs::create_dir(temp.path().join("base")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("base", temp.path().join("prefix")).unwrap();
    symlink("../real", temp.path().join("base/route")).unwrap();
    symlink("prefix/route/sub/..", temp.path().join("parent")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    for pattern in ["prefix/route/file", "base/route/file"] {
        assert_eq!(
            deny::check(&root, &policy(&[pattern]), Path::new("parent/file"))
                .unwrap_err()
                .category,
            category::PATH_DENIED,
            "{pattern}"
        );
    }
    assert!(
        deny::check(
            &root,
            &policy(&["unrelated/file"]),
            Path::new("parent/file")
        )
        .is_ok()
    );
}

#[test]
fn route_name_budget_is_bounded_but_not_a_depth_limit() {
    let temp = tempfile::tempdir().unwrap();
    for index in 0..9 {
        symlink(".", temp.path().join(format!("back{index}"))).unwrap();
    }
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let deny = policy(&["unrelated"]);
    let within_budget = (0..8)
        .map(|index| format!("back{index}"))
        .collect::<Vec<_>>()
        .join("/");
    assert!(deny::check(&root, &deny, Path::new(&within_budget)).is_ok());
    assert_eq!(
        deny::check(&root, &deny, Path::new(&format!("{within_budget}/back8")))
            .unwrap_err()
            .category,
        category::PATH_DENIED
    );
    let deep = format!(
        "back0/{}",
        std::iter::repeat_n("new", 300)
            .collect::<Vec<_>>()
            .join("/")
    );
    assert!(deny::check(&root, &deny, Path::new(&deep)).is_ok());
}

#[test]
fn identity_route_compositions_preserve_denials_and_allowed_resolution() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(temp.path().join("real/sub")).unwrap();
    std::fs::write(temp.path().join("real/file"), "fixture").unwrap();
    symlink("real", temp.path().join("route")).unwrap();
    symlink(".", temp.path().join("real/dot")).unwrap();
    symlink("../real", temp.path().join("real/up")).unwrap();
    let root = WorkspaceRoot::open(temp.path()).unwrap();
    let forms = ["sub/..", "dot", "up"];
    for (i, first) in forms.iter().enumerate() {
        for (j, second) in forms.iter().enumerate() {
            for (k, third) in forms.iter().enumerate() {
                let name = format!("alias-{i}-{j}-{k}");
                symlink(
                    format!("route/{first}/{second}/{third}"),
                    temp.path().join(&name),
                )
                .unwrap();
                for tail in ["file", "new/file"] {
                    let path = format!("{name}/{tail}");
                    assert_eq!(
                        deny::check(
                            &root,
                            &policy(&["route/file", "route/new/*"]),
                            Path::new(&path)
                        )
                        .unwrap_err()
                        .category,
                        category::PATH_DENIED,
                        "{path}"
                    );
                    assert_eq!(
                        deny::check(&root, &policy(&["unrelated"]), Path::new(&path))
                            .unwrap()
                            .resolved,
                        Path::new(&format!("real/{tail}")),
                        "{path}"
                    );
                }
            }
        }
    }
}
