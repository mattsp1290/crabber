//! Each step runs in a distinct process against the same SQLite database.
use super::*;
use std::{fs::OpenOptions, io::Write, path::PathBuf, process::Command};

struct DurableEffect {
    inner: Interactive,
    directory: PathBuf,
}
#[async_trait]
impl ResumableTool for DurableEffect {
    fn identity(&self) -> ResumableToolIdentity {
        self.inner.identity()
    }
    async fn start(
        &self,
        context: ToolContext,
        args: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        let mut effect = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join("effect"))
            .expect("effect must execute exactly once");
        effect.write_all(b"one effect\n").unwrap();
        effect.sync_all().unwrap();
        self.inner.start(context, args).await
    }
    async fn resume(
        &self,
        context: ToolContext,
        args: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        let mut resumes = OpenOptions::new()
            .append(true)
            .create(true)
            .open(self.directory.join("resumes"))
            .unwrap();
        writeln!(resumes, "{}", checkpoint.value()["step"]).unwrap();
        resumes.sync_all().unwrap();
        self.inner
            .resume(context, args, checkpoint, responses)
            .await
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // A complete public lifecycle journey.
async fn child() {
    let Ok(directory) = std::env::var("CRABBER_TOOL_CONTINUATION_CHILD") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let step: u8 = std::env::var("CRABBER_TOOL_CONTINUATION_STEP")
        .unwrap()
        .parse()
        .unwrap();
    let database = directory.join("session.sqlite");
    if step == 0 {
        crabber::session::SqliteStore::migrate(&database)
            .await
            .unwrap();
    }
    let store = Arc::new(
        crabber::session::SqliteStore::connect(&database)
            .await
            .unwrap(),
    );
    let provider = FakeProvider::scripted(vec![if step == 0 {
        call("cold-interactive")
    } else {
        done()
    }]);
    let agent = make(
        store.clone(),
        provider.clone(),
        Arc::new(DurableEffect {
            inner: Interactive {
                starts: Arc::default(),
                resumes: Arc::default(),
                effects: Arc::default(),
            },
            directory: directory.clone(),
        }),
    );
    let handle = if step == 0 {
        agent.prompt(None, "work").await.unwrap()
    } else {
        let (session, run): (SessionId, crabber::core::RunId) =
            serde_json::from_slice(&std::fs::read(directory.join("scope")).unwrap()).unwrap();
        let snapshot = agent
            .tool_continuation_snapshot(&session, &run)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.continuation.generation.get(), u64::from(step));
        let question = &snapshot.interactions[0];
        let accepted = agent
            .answer_tool_interaction(ToolInteractionAnswer {
                id: ControlId::new(),
                expected_revision: snapshot.revision,
                session_id: session.clone(),
                run_id: run.clone(),
                continuation: snapshot.continuation.clone(),
                interaction_id: question.id.clone(),
                resolver: ApprovalResolverClaim {
                    resolver_id: "cold-host".into(),
                    authority: question.description.authority().clone(),
                    authority_claim: "verified operator".into(),
                },
                reply: ToolInteractionReply::Answer {
                    value: json!("accepted"),
                },
            })
            .await
            .unwrap();
        let ToolContinuation::Started { handle, .. } = agent
            .continue_tool(ContinueTool {
                id: ControlId::new(),
                expected_revision: accepted.revision,
                session_id: session,
                run_id: run,
                continuation: snapshot.continuation,
            })
            .await
            .unwrap()
        else {
            panic!("fresh process continuation")
        };
        handle
    };
    let scope = (handle.session_id().clone(), handle.run_id().clone());
    assert_eq!(
        handle.done().await.unwrap().status,
        if step == 2 {
            RunStatus::Completed
        } else {
            RunStatus::Paused
        }
    );
    std::fs::write(directory.join("scope"), serde_json::to_vec(&scope).unwrap()).unwrap();
    agent.close_extensions().await.unwrap();
    assert_eq!(provider.requests().len(), usize::from(step != 1));
    if step == 2 {
        let messages = store.list_all_messages(&scope.0).await.unwrap();
        assert_eq!(
            messages
                .iter()
                .filter(|m| m.role == crabber::core::Role::Tool)
                .count(),
            1
        );
        assert!(
            !serde_json::to_string(&messages)
                .unwrap()
                .contains("never result")
        );
    }
}

#[test]
fn two_pauses_survive_fresh_processes_without_repeating_effect() {
    let directory = tempfile::tempdir().unwrap();
    for step in 0..=2 {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tool_continuation::process::child",
                "--nocapture",
            ])
            .env("CRABBER_TOOL_CONTINUATION_CHILD", directory.path())
            .env("CRABBER_TOOL_CONTINUATION_STEP", step.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child {step}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        std::fs::read(directory.path().join("effect")).unwrap(),
        b"one effect\n"
    );
    assert_eq!(
        std::fs::read_to_string(directory.path().join("resumes")).unwrap(),
        "1\n2\n"
    );
}
