//! Builder lifecycle and persistence through the public facade.
#![cfg(feature = "sqlite")]
use crabber::{
    Agent, AgentConfig, FakeProvider, Selection, StreamDelta,
    core::{Role, RunStatus},
    session::{SqliteStore, Store, StoreError},
};
use std::sync::Arc;

#[tokio::test]
async fn sqlite_builder_requires_migration_and_persists_a_completed_prompt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("builder.sqlite");
    let Err(error) = Agent::builder().sqlite(&path).await else {
        panic!("missing store must be rejected");
    };
    assert_eq!(
        error,
        StoreError::Validation("SQLite operation failed".into())
    );
    assert!(!path.exists());
    SqliteStore::migrate(&path).await.unwrap();
    let agent = Agent::builder()
        .sqlite(&path)
        .await
        .unwrap()
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("durable answer".into()),
            StreamDelta::Completed,
        ]])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "hello")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let reopened = SqliteStore::connect(&path).await.unwrap();
    let run = reopened.get_run(&result.run_id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Completed);
    let messages = reopened.list_all_messages(&run.session_id).await.unwrap();
    assert!(messages.iter().any(|message| message.role == Role::Assistant && message.parts.iter().any(|part| matches!(&part.content, crabber::core::ContentBlock::Text {text} if text == "durable answer"))));
}
