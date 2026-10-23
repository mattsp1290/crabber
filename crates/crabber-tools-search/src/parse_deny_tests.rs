//! Deterministic screening tests independent of ripgrep's directory ordering.
use super::*;
use std::{os::unix::fs::symlink, path::Path};

fn feed(parser: &mut Parser, kind: &str, path: &Value, number: u64, text: &str) {
    parser.feed(
        &serde_json::to_vec(&json!({"type":kind,"data":{
            "path":path,"lines":{"text":text},"line_number":number,"submatches":[]
        }}))
        .unwrap(),
    );
}
fn fixture() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("real/private")).unwrap();
    std::fs::write(tmp.path().join("real/private/data.txt"), "fixture").unwrap();
    std::fs::write(tmp.path().join("real/visible.txt"), "fixture").unwrap();
    symlink("real", tmp.path().join("alias")).unwrap();
    tmp
}
fn parser(root: &Path, limit: usize) -> Parser {
    Parser::new(1, limit, false).with_deny(
        WorkspaceRoot::open(root).unwrap(),
        PathDenyPolicy::new(["real/private".to_owned(), ".env*".to_owned()]).unwrap(),
    )
}

#[test]
fn denied_raw_paths_and_context_are_filtered_before_limits() {
    let tmp = fixture();
    let mut parser = parser(tmp.path(), 1);
    for path in [
        json!({"text":".env"}),
        json!({"bytes":STANDARD.encode(b".env-\xff")}),
        json!({"text":"alias/private/data.txt"}),
        json!({"bytes":STANDARD.encode(b"./alias/private/data.txt")}),
        json!({"bytes":"%%%"}),
        json!({"text":"../outside"}),
        json!({"text":"/outside"}),
        json!({"text":"bad\0name"}),
        json!({"text":"."}),
        json!({"text":""}),
        json!({}),
    ] {
        for kind in ["context", "match"] {
            // Enough denied records to exceed the retained byte budget if screened late.
            for _ in 0..70 {
                feed(&mut parser, kind, &path, 1, &"hidden".repeat(1000));
            }
        }
    }
    assert_eq!(parser.matches, [] as [Value; 0]);
    assert!(parser.reason.is_none());
    assert!(!parser.malformed);
    feed(
        &mut parser,
        "context",
        &json!({"text":"alias/visible.txt"}),
        1,
        "visible before",
    );
    feed(
        &mut parser,
        "match",
        &json!({"text":"alias/visible.txt"}),
        2,
        "visible match",
    );
    assert_eq!(parser.matches.len(), 1);
    assert_eq!(parser.matches[0]["before"][0]["line"], "visible before");
    assert_eq!(parser.reason, Some("matches"));
    assert!(!parser.matches[0].to_string().contains("hidden"));
}

#[test]
fn denied_context_cannot_attach_to_allowed_match() {
    let tmp = fixture();
    let mut parser = parser(tmp.path(), 10);
    feed(
        &mut parser,
        "match",
        &json!({"text":"alias/visible.txt"}),
        2,
        "visible match",
    );
    feed(
        &mut parser,
        "context",
        &json!({"text":"alias/private/data.txt"}),
        3,
        "hidden after",
    );
    feed(
        &mut parser,
        "context",
        &json!({"text":"alias/visible.txt"}),
        3,
        "visible after",
    );
    assert_eq!(
        parser.matches[0]["after"],
        json!([{"line_number":3,"line":"visible after"}])
    );
}

#[test]
fn unresolved_aliases_are_filtered_but_missing_tails_keep_existing_contract() {
    let tmp = fixture();
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), tmp.path().join("escape")).unwrap();
    symlink("loop", tmp.path().join("loop")).unwrap();
    let mut parser = parser(tmp.path(), 10);
    for path in ["escape/file", "loop/file", "real/visible.txt/child"] {
        feed(&mut parser, "match", &json!({"text":path}), 1, "hidden");
    }
    assert_eq!(parser.matches, [] as [Value; 0]);
    // The shared check supports absent tails, including disappeared search results.
    feed(
        &mut parser,
        "match",
        &json!({"text":"alias/missing.txt"}),
        1,
        "visible",
    );
    assert_eq!(parser.matches.len(), 1);
    assert!(parser.reason.is_none());
}

#[test]
fn empty_policy_does_not_resolve_paths() {
    let tmp = fixture();
    symlink("loop", tmp.path().join("loop")).unwrap();
    let mut parser = Parser::new(0, 10, false).with_deny(
        WorkspaceRoot::open(tmp.path()).unwrap(),
        PathDenyPolicy::none(),
    );
    assert!(parser.deny.is_none());
    feed(
        &mut parser,
        "match",
        &json!({"text":"loop/file"}),
        1,
        "visible",
    );
    assert_eq!(parser.matches.len(), 1);
}

#[cfg(not(target_os = "macos"))]
#[test]
fn base64_non_utf8_alias_names_resolve_without_lossy_conversion() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt};
    let tmp = fixture();
    let name = OsString::from_vec(b"alias-\xff".to_vec());
    symlink("real", tmp.path().join(name)).unwrap();
    let mut parser = parser(tmp.path(), 1);
    feed(
        &mut parser,
        "match",
        &json!({"bytes":STANDARD.encode(b"alias-\xff/private/data.txt")}),
        1,
        "hidden",
    );
    assert_eq!(parser.matches, [] as [Value; 0]);
    feed(
        &mut parser,
        "match",
        &json!({"bytes":STANDARD.encode(b"alias-\xff/visible.txt")}),
        1,
        "visible",
    );
    assert_eq!(parser.matches.len(), 1);
    assert_eq!(parser.matches[0]["path"], "alias-\u{fffd}/visible.txt");
}
