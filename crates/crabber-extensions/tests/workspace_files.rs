use crabber::extension::{
    Extension, WorkspaceContext, WorkspaceReadErrorKind as Kind, WorkspaceReader,
    WorkspaceReaderResolver,
};
use crabber_extensions::workspace_instructions::{
    Limits, Options, ReaderRoot, WorkspaceInstructions,
    host::{AllowListResolver, BoundaryReader, WorkspaceRootReader},
};
use std::{path::Path, sync::Arc, time::Duration};
fn options() -> Options {
    Options {
        order: 100,
        file_names: vec!["AGENTS.md".into()],
        reader_root: ReaderRoot::Boundary,
        boundaries: vec!["/admitted".into()],
        limits: Limits {
            max_file_names: 4,
            max_chain_depth: 8,
            max_file_bytes: 1024,
            max_section_bytes: 4096,
            max_in_flight: 4,
            max_wait: Duration::from_secs(1),
        },
    }
}
#[tokio::test]
async fn boundary_reader_reads_only_the_admitted_ancestor_chain_and_refreshes_files() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("a/b");
    std::fs::create_dir_all(&nested).unwrap();
    for (path, content) in [
        ("AGENTS.md", "parent"),
        ("a/AGENTS.md", "middle"),
        ("a/b/AGENTS.md", "child"),
    ] {
        std::fs::write(root.path().join(path), content).unwrap();
    }
    let reader = BoundaryReader::for_workspace(root.path(), &nested).unwrap();
    assert_eq!(reader.directory(), nested.canonicalize().unwrap());
    for (path, content) in [
        ("AGENTS.md", "parent"),
        ("a/AGENTS.md", "middle"),
        ("a/b/AGENTS.md", "child"),
    ] {
        assert_eq!(
            reader.read_limited(path, 20).await.unwrap(),
            content.as_bytes()
        );
    }
    assert_eq!(
        reader
            .read_limited("nonexistent-sibling/AGENTS.md", 20)
            .await
            .unwrap_err()
            .kind(),
        Kind::Denied
    );
    assert_eq!(
        reader
            .read_limited("a/b/nonexistent-child/AGENTS.md", 20)
            .await
            .unwrap_err()
            .kind(),
        Kind::Denied
    );
    assert_eq!(
        reader.read_limited("missing", 20).await.unwrap_err().kind(),
        Kind::NotFound
    );
    assert_eq!(
        reader
            .read_limited("AGENTS.md", 5)
            .await
            .unwrap_err()
            .kind(),
        Kind::TooLarge
    );
    assert_eq!(
        reader.read_limited("AGENTS.md", 6).await.unwrap(),
        b"parent"
    );
    std::fs::write(root.path().join("AGENTS.md"), "new").unwrap();
    assert_eq!(reader.read_limited("AGENTS.md", 20).await.unwrap(), b"new");
    for path in [
        "../AGENTS.md",
        "a//AGENTS.md",
        "/AGENTS.md",
        "a/./AGENTS.md",
        "a/../AGENTS.md",
        "a/",
        "a\\b",
        "a:b",
        "\0",
        "",
    ] {
        assert_eq!(
            reader.read_limited(path, 20).await.unwrap_err().kind(),
            Kind::InvalidPath,
            "{path:?}"
        );
    }
}
#[tokio::test]
async fn workspace_reader_refuses_directory_paths_and_exact_resolver_checks_both_fields() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("AGENTS.md"), "content").unwrap();
    let reader = Arc::new(WorkspaceRootReader::open(root.path()).unwrap());
    assert_eq!(
        reader.read_limited("AGENTS.md", 7).await.unwrap(),
        b"content"
    );
    assert_eq!(
        reader
            .read_limited("child/AGENTS.md", 20)
            .await
            .unwrap_err()
            .kind(),
        Kind::Denied
    );
    let directory = reader.directory().to_str().unwrap().to_owned();
    let resolver = AllowListResolver::new(vec![("ws".into(), directory.clone(), reader)]).unwrap();
    assert!(
        resolver
            .resolve(&WorkspaceContext::from_persisted("ws", &directory))
            .await
            .is_ok()
    );
    for c in [
        WorkspaceContext::from_persisted("other", &directory),
        WorkspaceContext::from_persisted("ws", "/other"),
        WorkspaceContext::from_persisted("", ""),
    ] {
        assert!(matches!(resolver.resolve(&c).await, Err(e) if e.kind() == Kind::Denied));
    }
}
#[cfg(unix)]
#[tokio::test]
async fn symlinks_and_nonregular_files_fail_and_root_handles_remain_pinned() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(temp.path().join("outside"), "outside").unwrap();
    let reader = BoundaryReader::for_workspace(&root, &root).unwrap();
    symlink(temp.path().join("outside"), root.join("AGENTS.md")).unwrap();
    assert_eq!(
        reader
            .read_limited("AGENTS.md", 100)
            .await
            .unwrap_err()
            .kind(),
        Kind::InvalidPath
    );
    std::fs::remove_file(root.join("AGENTS.md")).unwrap();
    std::fs::create_dir(root.join("AGENTS.md")).unwrap();
    assert_eq!(
        reader
            .read_limited("AGENTS.md", 100)
            .await
            .unwrap_err()
            .kind(),
        Kind::InvalidPath
    );
    std::fs::remove_dir(root.join("AGENTS.md")).unwrap();
    std::fs::write(root.join("AGENTS.md"), "admitted").unwrap();
    std::fs::rename(&root, temp.path().join("moved")).unwrap();
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("AGENTS.md"), "replacement").unwrap();
    assert_eq!(
        reader.read_limited("AGENTS.md", 100).await.unwrap(),
        b"admitted"
    );
}
#[cfg(unix)]
#[tokio::test]
async fn intermediate_symlink_substitution_is_refused() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let child = root.path().join("child");
    std::fs::create_dir(&child).unwrap();
    std::fs::write(child.join("AGENTS.md"), "admitted").unwrap();
    let reader = BoundaryReader::for_workspace(root.path(), &child).unwrap();
    std::fs::rename(&child, root.path().join("moved")).unwrap();
    symlink(root.path().join("moved"), &child).unwrap();
    assert_eq!(
        reader
            .read_limited("child/AGENTS.md", 100)
            .await
            .unwrap_err()
            .kind(),
        Kind::InvalidPath
    );
}
#[test]
fn validates_configuration_hashes_and_host_admission() {
    let a = WorkspaceInstructions::new(options()).unwrap();
    assert_eq!(
        a.config_hash(),
        WorkspaceInstructions::new(options()).unwrap().config_hash()
    );
    let mut o = options();
    o.boundaries.push("/second".into());
    assert_ne!(
        a.config_hash(),
        WorkspaceInstructions::new(o).unwrap().config_hash()
    );
    let mut o = options();
    o.reader_root = ReaderRoot::Workspace;
    o.boundaries.clear();
    assert_ne!(
        a.config_hash(),
        WorkspaceInstructions::new(o).unwrap().config_hash()
    );
    for boundary in ["", "relative", "/a/", "/a//b", "/a/..", "/a/.", "/a/\u{1}"] {
        let mut o = options();
        o.boundaries = vec![boundary.into()];
        assert!(WorkspaceInstructions::new(o).is_err());
    }
    for name in ["../AGENTS.md", "a/b", ".", "", "nul\0"] {
        let mut o = options();
        o.file_names = vec![name.into()];
        assert!(WorkspaceInstructions::new(o).is_err());
    }
    let mut o = options();
    o.reader_root = ReaderRoot::Workspace;
    assert!(WorkspaceInstructions::new(o).is_err());
    let mut o = options();
    o.boundaries.clear();
    assert!(WorkspaceInstructions::new(o).is_err());
    let mut o = options();
    o.boundaries.push("/admitted".into());
    assert!(WorkspaceInstructions::new(o).is_err());
    let mut o = options();
    o.limits.max_section_bytes = 0;
    assert!(WorkspaceInstructions::new(o).is_err());
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    assert!(BoundaryReader::for_workspace(root.path(), other.path()).is_err());
    assert!(WorkspaceRootReader::open(Path::new("relative")).is_err());
}
