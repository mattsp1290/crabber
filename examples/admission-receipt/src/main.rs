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
    let mode = std::env::args().nth(1).unwrap_or_else(|| "--memory".into());
    let store = connect(&mode).await?;
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
    let looked_up = agent.lookup_admission(&session, &options.key).await?;
    assert_eq!(looked_up, Some(receipt.clone()));
    let receipt_count = looked_up.iter().count();
    let messages = store.list_all_messages(&session).await?;
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == Role::User)
            .count(),
        1
    );
    let provider_count = provider.requests().len();
    let user_count = messages
        .iter()
        .filter(|message| message.role == Role::User)
        .count();
    let run_count = messages
        .iter()
        .filter_map(|message| message.run_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(provider_count, 1);
    assert_eq!(run_count, 1);
    let persisted_runs = store.get_run(&receipt.run_id).await?.iter().count();
    assert_eq!(persisted_runs, 1);
    #[cfg(feature = "postgres")]
    if mode == "--postgres" {
        verify_postgres_counts(&session).await?;
    }
    assert_eq!(result.run_id, receipt.run_id);
    print_source(&mode)?;
    println!(
        "receipt={} run={} session={} user_message={}",
        receipt.run_id, receipt.run_id, receipt.session_id, receipt.user_message_id
    );
    println!(
        "retry_receipt={} provider_executions={provider_count} user_messages={user_count} runs={persisted_runs} message_run_ids={run_count} receipts={receipt_count} assertions=10 status={:?}",
        replay.receipt().run_id,
        result.status
    );
    Ok(())
}

// PostgreSQL performs asynchronous migration/connect when that mode is enabled.
#[cfg_attr(not(feature = "postgres"), allow(clippy::unused_async))]
async fn connect(mode: &str) -> Result<Arc<dyn Store>, Box<dyn Error>> {
    let store: Arc<dyn Store> = match mode {
        "--memory" => Arc::new(MemoryStore::new()),
        #[cfg(feature = "postgres")]
        "--postgres" => {
            let url = std::env::var("CRABBER_TEST_POSTGRES_URL")
                .map_err(|_| "CRABBER_TEST_POSTGRES_URL is required for --postgres")?;
            crabber::session::PostgresStore::migrate(&url).await?;
            Arc::new(crabber::session::PostgresStore::connect(&url).await?)
        }
        _ => {
            return Err(
                "usage: admission-receipt --memory | --postgres (requires postgres feature)".into(),
            );
        }
    };
    Ok(store)
}

fn print_source(mode: &str) -> Result<(), Box<dyn Error>> {
    let source = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;
    if !source.status.success() {
        return Err("source revision unavailable".into());
    }
    let sha = String::from_utf8(source.stdout)?;
    println!(
        "source={} store={} schema={} provider=fake/scripted",
        sha.trim(),
        mode.trim_start_matches("--"),
        if mode == "--postgres" {
            "2"
        } else {
            "admission-v1"
        }
    );
    Ok(())
}

#[cfg(feature = "postgres")]
async fn verify_postgres_counts(session: &SessionId) -> Result<(), Box<dyn Error>> {
    let url = std::env::var("CRABBER_TEST_POSTGRES_URL")
        .map_err(|_| "CRABBER_TEST_POSTGRES_URL is required for --postgres")?;
    let pool = sqlx::PgPool::connect(&url)
        .await
        .map_err(|_| "database evidence connection failed")?;
    let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM runs WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&pool)
        .await
        .map_err(|_| "run evidence query failed")?;
    let receipts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admission_receipts WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&pool)
            .await
            .map_err(|_| "receipt evidence query failed")?;
    assert_eq!((runs, receipts), (1, 1));
    println!("durable_runs={runs} durable_receipts={receipts} count_assertions=2");
    pool.close().await;
    Ok(())
}
