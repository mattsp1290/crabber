//! Two real process lifetimes over SQLite; never infer a join from a cold cache.
use crate::{agent_support, common::*, scripted::Scripts};
use crabber::{
    Agent, AgentConfig, ExecutionContext, PermissionDecision, StaticPolicy,
    core::{RunId, RunStatus, SessionContext, SessionId, SessionOwnerId},
    extension::Scope,
    session::{SqliteStore, Store, session_owner::SessionOwnerState},
};
use crabber_extensions::delegate_task::{DelegateTask, Runner, START_TOOL, STATUS_TOOL};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn host(
    store: Arc<SqliteStore>,
    scripts: Arc<Scripts>,
    directory: &Path,
    effects: Arc<AtomicUsize>,
) -> (Agent, Arc<DelegateTask>) {
    let runner: Runner = Arc::new(move |_| {
        effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    });
    let mut options = crate::factory_options::delegate(runner);
    options.runner_identity = "consumer-pending.v1".into();
    options.limits.max_background_duration = Duration::from_secs(600);
    let delegate = Arc::new(DelegateTask::new(options).unwrap());
    let mut config = AgentConfig::new(selection());
    config.workspace_id = "restart-workspace".into();
    config.directory = directory.to_str().unwrap().into();
    let agent = agent_support::builder()
        .behavior_version("durable-host-restart.v1")
        .store(store)
        .provider(scripts)
        .config(config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(delegate.clone(), Scope::Global)
        .shared_extension(Arc::new(ask()), Scope::Global)
        .build()
        .unwrap();
    (agent, delegate)
}
pub async fn run(mode: &str, database: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let parent = database.parent().unwrap();
    let state_file = parent.join("restart-state.json");
    let directory = parent.join("restart-workspace");
    if mode == "restart-phase-1" {
        std::fs::create_dir(&directory)?;
        SqliteStore::migrate(database).await?;
        let store = Arc::new(SqliteStore::connect(database).await?);
        let scripts = Arc::new(Scripts::default());
        let effects = Arc::new(AtomicUsize::new(0));
        let (agent, _) = host(store.clone(), scripts.clone(), &directory, effects.clone());
        let execution = ExecutionContext::new(
            SessionContext::new(
                SessionId::new(),
                "restart-workspace",
                directory.to_str().unwrap(),
            ),
            selection(),
        );
        scripts.set(vec![
            batch(&[(
                START_TOOL,
                json!({"task":"unjoined synthetic task","profile":"pending"}),
            )]),
            done(),
        ]);
        let run = agent_support::first_prompt(&agent, &execution, "start detached").await?;
        let parent_run = run.run_id().clone();
        assert_eq!(run.done().await?.status, RunStatus::Completed);
        tokio::time::timeout(Duration::from_secs(5), async {
            while effects.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let calls = records(&*store, execution.session().session_id()).await;
        let start = calls.iter().find(|c| c.name == START_TOOL).unwrap();
        let task = value(start)["task_id"].as_str().unwrap().to_owned();
        let hash = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&(
                execution.session().session_id(),
                &parent_run,
                &start.id
            ))?)
        );
        let owner = SessionOwnerId::from(format!("delegate-task.{}", &hash[..32]));
        assert_eq!(
            store
                .get_session_owner(execution.session().session_id(), &owner)
                .await?
                .state,
            SessionOwnerState::Active
        );
        let asked = ExecutionContext::new(
            SessionContext::new(
                SessionId::new(),
                "restart-workspace",
                directory.to_str().unwrap(),
            ),
            selection(),
        );
        scripts.set(vec![
            batch(&[(crabber_extensions::ask_user::TOOL_NAME, question())]),
            done(),
        ]);
        let run = agent_support::first_prompt(&agent, &asked, "pause durable question").await?;
        let ask_run = run.run_id().clone();
        assert_eq!(run.done().await?.status, RunStatus::Paused);
        std::fs::write(
            state_file,
            serde_json::to_vec_pretty(
                &json!({"delegate_session":execution.session().session_id(),"task":task,"owner":owner,"ask_session":asked.session().session_id(),"ask_run":ask_run}),
            )?,
        )?;
        println!(
            "{}",
            json!({"phase":1,"owner":"Active","ask":"Paused","runner_started":effects.load(Ordering::SeqCst),"graceful_cleanup":false})
        );
        // Simulate process loss after acknowledged durable commits. The runner
        // owns no subprocess, provider, credential or external business effect.
        std::process::exit(0);
    }
    let saved: Value = serde_json::from_slice(&std::fs::read(state_file)?)?;
    let session = SessionId::from(saved["delegate_session"].as_str().unwrap());
    let owner = SessionOwnerId::from(saved["owner"].as_str().unwrap());
    let asked = SessionId::from(saved["ask_session"].as_str().unwrap());
    let ask_run = RunId::from(saved["ask_run"].as_str().unwrap());
    let store = Arc::new(SqliteStore::connect(database).await?);
    let scripts = Arc::new(Scripts::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let (agent, delegate) = host(store.clone(), scripts.clone(), &directory, effects.clone());
    assert_eq!(
        store.get_session_owner(&session, &owner).await?.state,
        SessionOwnerState::Active
    );
    assert!(
        agent
            .drain_session_invocations(&session, true, Duration::from_millis(1))
            .await
            .is_empty()
    );
    assert_eq!(
        (
            delegate.owner_report().live,
            delegate.owner_report().tracked
        ),
        (0, 0)
    );
    scripts.set(vec![done()]);
    answer_question(&agent, &asked, &ask_run).await;
    let completed = records(&*store, &asked).await;
    assert_eq!(completed.len(), 1);
    assert_eq!(value(&completed[0])["selected_option"], 2);
    let context = ExecutionContext::new(
        SessionContext::new(
            session.clone(),
            "restart-workspace",
            directory.to_str().unwrap(),
        ),
        selection(),
    );
    scripts.set(vec![
        batch(&[(STATUS_TOOL, json!({"task_id":saved["task"]}))]),
        done(),
    ]);
    agent_support::next_prompt(&agent, &context, "cold private status")
        .await?
        .done()
        .await?;
    let calls = records(&*store, &session).await;
    let status = calls.iter().find(|c| c.name == STATUS_TOOL).unwrap();
    assert_eq!(status.status, crabber::core::ToolCallStatus::Failed);
    assert_eq!(effects.load(Ordering::SeqCst), 0, "no prior runner resumed");
    assert_eq!(
        store.get_session_owner(&session, &owner).await?.state,
        SessionOwnerState::Active
    );
    let close = agent.close(Duration::from_secs(5)).await;
    assert!(close.local_drained());
    assert_eq!(
        store.get_session_owner(&session, &owner).await?.state,
        SessionOwnerState::Active
    );
    println!(
        "{}",
        json!({"phase":2,"ask_completed_calls":1,"answer_and_continue_replayed":true,"durable_owner":"Active","cold_local_invocations":0,"resumed_processes":0,"local_drained":true})
    );
    Ok(())
}
