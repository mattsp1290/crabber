use super::*;
pub(super) async fn restart(sqlite: bool) -> Result<(), Error> {
    use std::os::unix::fs::MetadataExt;
    let root = tempfile::tempdir()?;
    let database = tempfile::tempdir()?;
    let url = database.path().join("state.sqlite");
    let store: Arc<dyn Store> = if sqlite {
        SqliteStore::migrate(&url).await?;
        let store = SqliteStore::connect(&url).await?;
        Arc::new(store)
    } else {
        Arc::new(MemoryStore::new())
    };
    let mut batch = turn("file_write", json!({"path":"effect","content":"once"}));
    batch.pop();
    batch.extend(turn("user_interact", json!({"question":"First?"})));
    batch.pop();
    batch.extend(turn("user_interact", json!({"question":"Second?"})));
    let provider = Arc::new(FakeProvider::scripted(vec![batch, done()]));
    let agent = host(root.path(), store.clone(), provider.clone(), policy())?;
    let mut snapshot = paused(&agent).await?;
    let session = snapshot.session_id.clone();
    let inode = std::fs::metadata(root.path().join("effect"))?.ino();
    let mut active = agent;
    let mut reopened = false;
    let mut answered = std::collections::BTreeSet::new();
    let mut active_store = store;
    loop {
        assert!(!snapshot.interactions.is_empty());
        let mut revision = snapshot.revision;
        for (index, question) in snapshot.interactions.iter().enumerate() {
            assert!(
                answered.insert(question.call_id.to_string()),
                "question answered twice"
            );
            let mut request = answer(
                &snapshot,
                index,
                ToolInteractionReply::Answer {
                    value: json!(question.description.prompt()),
                },
            );
            request.expected_revision = revision;
            revision = active.answer_tool_interaction(request).await?.revision;
            if index + 1 < snapshot.interactions.len() {
                assert!(
                    active
                        .continue_tool(continuation(&snapshot, revision))
                        .await
                        .is_err()
                );
            }
        }
        if !reopened {
            active.close_extensions().await?;
            drop(active);
            if sqlite {
                drop(active_store);
                active_store = Arc::new(SqliteStore::connect(&url).await?);
            }
            active = host(
                root.path(),
                active_store.clone(),
                provider.clone(),
                policy(),
            )?;
            reopened = true;
        }
        let request = continuation(&snapshot, revision);
        let ToolContinuation::Started { handle, .. } =
            active.continue_tool(request.clone()).await?
        else {
            return Err("fresh continuation replayed".into());
        };
        let status = handle.done().await?.status;
        assert!(matches!(
            active.continue_tool(request).await?,
            ToolContinuation::Replayed(_)
        ));
        if status == RunStatus::Completed {
            break;
        }
        assert_eq!(status, RunStatus::Paused);
        assert!(answered.len() < 2);
        snapshot = active
            .tool_continuation_snapshot(&snapshot.session_id, &snapshot.run_id)
            .await?
            .ok_or("missing next continuation")?;
    }
    assert_eq!(answered.len(), 2);
    assert!(reopened);
    assert_eq!(std::fs::metadata(root.path().join("effect"))?.ino(), inode);
    assert_eq!(std::fs::read_to_string(root.path().join("effect"))?, "once");
    let records = results(&*active_store, &session).await?;
    assert_eq!(records.iter().filter(|r| r.name == "file_write").count(), 1);
    assert!(
        records
            .iter()
            .all(|r| r.status == ToolCallStatus::Completed)
    );
    active.close_extensions().await?;
    check(if sqlite {
        "sqlite_reopen_parallel_resume_effect_once"
    } else {
        "memory_restart_parallel_resume_effect_once"
    });
    Ok(())
}
