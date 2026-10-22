use super::*;
#[tokio::test]
async fn upstream_cli_cases_through_durable_surface() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("../fixtures/go-cases.json")).unwrap();
    for case in cases.iter().skip(1).filter(|c| c["surface"] == "cli") {
        let root = tempfile::tempdir().unwrap();
        let store = Arc::new(MemoryStore::new());
        let agent = agent(
            root.path(),
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                turn("user_interact", case["args"].clone()),
                done(),
            ])),
            UserInteractPolicy::durable(authority()),
        );
        let handle = agent.prompt(None, "fixture").await.unwrap();
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let status = handle.done().await.unwrap().status;
        if status == RunStatus::Paused {
            let snapshot = agent
                .tool_continuation_snapshot(&session, &run)
                .await
                .unwrap()
                .unwrap();
            let reply = if case["durable"]["reply"] == "deny" {
                ToolInteractionReply::Deny
            } else {
                ToolInteractionReply::Answer {
                    value: case["durable"]["value"].clone(),
                }
            };
            let receipt = agent
                .answer_tool_interaction(answer(&snapshot, 0, reply))
                .await
                .unwrap();
            resume(&agent, &snapshot, receipt.revision).await;
        }
        let records = calls(&store, &session).await;
        if case["args"]["question"] == "" {
            // Agent enforces minLength before start; also qualify the native validator.
            assert_eq!(records[0].status, ToolCallStatus::Failed);
            let definition = tool(UserInteractPolicy::durable(authority()));
            let context = support::context(
                &crabber_tools_core::WorkspaceRoot::open(root.path()).unwrap(),
                tokio_util::sync::CancellationToken::new(),
            );
            let crabber::core::ToolPhaseOutcome::Completed(output) = definition
                .executor
                .resumable()
                .unwrap()
                .start(context.clone(), case["args"].clone())
                .await
                .unwrap()
            else {
                panic!("invalid question suspended")
            };
            assert_eq!(
                output.json_value().unwrap()["error"]["category"],
                "validation"
            );
            agent.close_extensions().await.unwrap();
            continue;
        }
        let result = value(&records[0]);
        let expected = case.get("durable_expected").unwrap_or(&case["expected"]);
        assert_eq!(result["outcome"], expected["outcome"], "{}", case["name"]);
        assert_eq!(
            result.get("answer").is_some(),
            expected["answer_key_present"].as_bool().unwrap()
        );
        for key in ["answer", "category"] {
            if let Some(value) = expected.get(key) {
                assert_eq!(
                    if key == "category" {
                        &result["error"][key]
                    } else {
                        &result[key]
                    },
                    value
                );
            }
        }
        agent.close_extensions().await.unwrap();
    }
}
