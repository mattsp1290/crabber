use super::*;
pub(super) async fn decisions() -> Result<(), Error> {
    for (reply, category) in [
        (ToolInteractionReply::Deny, "denied"),
        (ToolInteractionReply::Cancel, "cancelled"),
        (ToolInteractionReply::Expire, "timeout"),
    ] {
        let root = tempfile::tempdir()?;
        let store = Arc::new(MemoryStore::new());
        let mut p = policy();
        if category == "timeout" {
            p.surface = Surface::Durable {
                authority: authority(),
                expires_after: Some(Duration::from_millis(1)),
            };
        }
        let agent = host(
            root.path(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                turn("user_interact", json!({"question":"Q?"})),
                done(),
            ])),
            p,
        )?;
        let snapshot = paused(&agent).await?;
        let receipt = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match agent
                    .answer_tool_interaction(answer(&snapshot, 0, reply.clone()))
                    .await
                {
                    Ok(receipt) => break Ok::<_, Error>(receipt),
                    Err(_) if category == "timeout" => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Err(error) => break Err(error.into()),
                }
            }
        })
        .await??;
        finish(&agent, &snapshot, receipt.revision).await?;
        let records = results(&*store, &snapshot.session_id).await?;
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(value(&records[0])["error"]["category"], category);
        agent.close_extensions().await?;
        check(category);
    }
    Ok(())
}
pub(super) async fn controls() -> Result<(), Error> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(MemoryStore::new());
    let agent = host(
        root.path(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            turn("user_interact", json!({"question":"Q?"})),
            done(),
        ])),
        policy(),
    )?;
    let snapshot = paused(&agent).await?;
    let valid = answer(
        &snapshot,
        0,
        ToolInteractionReply::Answer {
            value: json!("yes"),
        },
    );
    let mut invalids = vec![];
    let mut stale = valid.clone();
    stale.expected_revision = SessionRevision::new(snapshot.revision.get() - 1)?;
    invalids.push(stale);
    let mut cross = valid.clone();
    cross.session_id = crabber::core::SessionId::new();
    invalids.push(cross);
    let mut unauthorized = valid.clone();
    unauthorized.resolver.authority = AuthorityRequirement::new("wrong", "1")?;
    invalids.push(unauthorized);
    for value in [
        json!(true),
        json!("x".repeat(9 * 1024)),
        json!("x".repeat(70 * 1024)),
    ] {
        let mut invalid = valid.clone();
        invalid.reply = ToolInteractionReply::Answer { value };
        invalids.push(invalid);
    }
    for invalid in invalids {
        assert!(agent.answer_tool_interaction(invalid).await.is_err());
        assert_eq!(
            store.session_revision(&snapshot.session_id).await?,
            snapshot.revision
        );
    }
    let receipt = agent.answer_tool_interaction(valid.clone()).await?;
    assert_eq!(agent.answer_tool_interaction(valid.clone()).await?, receipt);
    let mut conflicting = valid;
    conflicting.reply = ToolInteractionReply::Answer { value: json!("no") };
    assert!(agent.answer_tool_interaction(conflicting).await.is_err());
    finish(&agent, &snapshot, receipt.revision).await?;
    agent.close_extensions().await?;
    check("duplicate_conflicting_stale_cross_session_unauthorized_and_oversized");
    Ok(())
}
pub(super) async fn model_answer() -> Result<(), Error> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(MemoryStore::new());
    let agent = host(
        root.path(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            turn("user_interact", json!({"question":"Q?","answer":"model"})),
            done(),
        ])),
        policy(),
    )?;
    let run = agent.prompt(None, "ask").await?;
    let session = run.session_id().clone();
    assert_eq!(run.done().await?.status, RunStatus::Completed);
    let records = results(&*store, &session).await?;
    assert_eq!(records[0].status, ToolCallStatus::Failed);
    assert_eq!(value(&records[0])["error"]["category"], "validation");
    agent.close_extensions().await?;
    check("model_answer_rejected");
    Ok(())
}
