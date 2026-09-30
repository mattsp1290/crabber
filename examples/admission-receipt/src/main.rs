use crabber::{
    Admission, AdmissionKey, AdmissionOptions, Agent, AgentConfig, FakeProvider, InputFingerprint,
    Selection, SessionId, StreamDelta,
    core::Role,
    session::{MemoryStore, Store},
};
use sha2::{Digest, Sha256};
use std::{error::Error, process::Command, sync::Arc};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("Done.".into()),
        StreamDelta::Completed,
    ]]));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(provider.clone())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()?;
    // Persist these values in the host before sending the first request.
    let session = SessionId::new();
    let text = "Complete this one text turn";
    let options = AdmissionOptions {
        key: AdmissionKey::new("turn-1")?,
        fingerprint: InputFingerprint::new(format!("{:x}", Sha256::digest(text)))?,
        behavior_fingerprint: InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest("demo-behavior-v1")
        ))?,
    };
    let (first, concurrent) = tokio::join!(
        agent.prompt_keyed(session.clone(), text, options.clone()),
        agent.prompt_keyed(session.clone(), text, options.clone()),
    );
    let first = first?;
    let concurrent = concurrent?;
    let receipt = first.receipt().clone();
    assert_eq!(receipt, *concurrent.receipt());
    let ((Admission::Started { mut handle, .. }, Admission::Replayed(_))
    | (Admission::Replayed(_), Admission::Started { mut handle, .. })) = (first, concurrent)
    else {
        return Err("expected one executor".into());
    };
    let mut events = handle.events();
    while events.recv().await?.is_some() {}
    let result = handle.done().await?;
    let replay = agent
        .prompt_keyed(session.clone(), text, options.clone())
        .await?;
    assert!(matches!(replay, Admission::Replayed(_)));
    assert_eq!(replay.receipt(), &receipt);
    assert_eq!(
        agent.lookup_admission(&session, &options.key).await?,
        Some(receipt.clone())
    );
    let messages = store.list_all_messages(&session).await?;
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1
    );
    assert_eq!(provider.requests().len(), 1);
    assert_eq!(result.run_id, receipt.run_id);
    let source = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;
    if !source.status.success() {
        return Err("source revision unavailable".into());
    }
    let sha = String::from_utf8(source.stdout)?;
    println!(
        "source={} store=memory schema=admission-v1 provider=fake/scripted",
        sha.trim()
    );
    println!(
        "receipt={} run={} session={} user_message={}",
        receipt.run_id, receipt.run_id, receipt.session_id, receipt.user_message_id
    );
    println!(
        "retry_receipt={} provider_executions=1 user_messages=1 runs=1 assertions=8 status={:?}",
        replay.receipt().run_id,
        result.status
    );
    Ok(())
}
