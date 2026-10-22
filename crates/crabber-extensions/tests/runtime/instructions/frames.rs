use super::*;

#[tokio::test]
async fn exact_boundary_frame_uses_only_chain_paths_and_workspace_mode_only_root_names() {
    for (mode, boundaries, expected_paths, expected_frame) in [
        (
            ReaderRoot::Boundary,
            vec!["/boundary".into()],
            vec!["AGENTS.md", "child/AGENTS.md"],
            "<workspace_instructions version=\"crabber-v1\">\n<file path=\"../AGENTS.md\">\nparent\n</file>\n<file path=\"./AGENTS.md\">\nchild\n</file>\n</workspace_instructions>",
        ),
        (
            ReaderRoot::Workspace,
            vec![],
            vec!["AGENTS.md"],
            "<workspace_instructions version=\"crabber-v1\">\n<file path=\"./AGENTS.md\">\nparent\n</file>\n</workspace_instructions>",
        ),
    ] {
        let reader =
            MemoryReader::new(&[("AGENTS.md", b"parent\n"), ("child/AGENTS.md", b"child")]);
        let provider = Arc::new(FakeProvider::scripted(vec![done()]));
        let agent = builder(
            provider.clone(),
            options(mode, boundaries),
            fixed(reader.clone()),
        )
        .build()
        .unwrap();
        assert_eq!(
            run(&agent, "ws", "/boundary/child").await.unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            provider.requests()[0].system.as_deref().unwrap(),
            format!("base\n{expected_frame}")
        );
        assert_eq!(*reader.reads.lock().unwrap(), expected_paths);
        agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn invalid_empty_and_nul_content_skip_and_oversized_files_have_no_partial_content() {
    for bytes in [&[0xff][..], &[0][..], b" \n".as_slice()] {
        let reader = MemoryReader::new(&[("AGENTS.md", bytes)]);
        let provider = Arc::new(FakeProvider::scripted(vec![done()]));
        let agent = builder(
            provider.clone(),
            options(ReaderRoot::Workspace, vec![]),
            fixed(reader),
        )
        .build()
        .unwrap();
        run(&agent, "ws", "/admitted").await.unwrap();
        assert_eq!(provider.requests()[0].system.as_deref(), Some("base"));
        agent.close_extensions().await.unwrap();
    }
    let provider = Arc::new(FakeProvider::scripted(vec![done()]));
    let reader = MemoryReader::new(&[("AGENTS.md", b"sensitive-prefix-too-large")]);
    let mut o = options(ReaderRoot::Workspace, vec![]);
    o.limits.max_file_bytes = 3;
    let agent = builder(provider.clone(), o, fixed(reader)).build().unwrap();
    run(&agent, "ws", "/admitted").await.unwrap();
    let frame = provider.requests()[0].system.clone().unwrap();
    assert!(frame.contains("truncated=\"true\"") && frame.contains("[omitted: file exceeds"));
    assert!(!frame.contains("sensitive-prefix"));
    agent.close_extensions().await.unwrap();
}
