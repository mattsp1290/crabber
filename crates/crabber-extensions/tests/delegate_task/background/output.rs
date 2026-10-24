use super::*;

#[tokio::test]
async fn wait_returns_done_immediately_when_all_terminal() {
    let count = Arc::new(AtomicUsize::new(0));
    let host: Runner = Arc::new(move |_| {
        let first = count.fetch_add(1, Ordering::SeqCst) == 0;
        Box::pin(async move {
            Ok(if first {
                Response::Completed("ok".into())
            } else {
                Response::Failed("failed".into())
            })
        })
    });
    let mut j = Journey::new(host, limits());
    let a = j.start().await;
    j.wait(std::slice::from_ref(&a)).await;
    let b = j.start().await;
    let reply = j.wait(&[a, b]).await;
    assert_eq!(reply["status"], "done");
    assert_eq!(reply["tasks"][0]["status"], "completed");
    assert_eq!(reply["tasks"][1]["status"], "failed");
    j.close().await;
}

#[tokio::test]
async fn wait_output_budget_omits_later_outputs() {
    let count = Arc::new(AtomicUsize::new(0));
    let host: Runner = Arc::new(move |_| {
        let n = count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(Response::Completed(if n == 3 {
                String::new()
            } else {
                "x".repeat(200)
            }))
        })
    });
    let mut j = Journey::new(
        host,
        Limits {
            max_result_bytes: 256,
            ..limits()
        },
    );
    let mut ids = Vec::new();
    for _ in 0..4 {
        let id = j.start().await;
        j.wait(std::slice::from_ref(&id)).await;
        ids.push(id);
    }
    let reply = j.wait(&ids).await;
    assert_eq!(reply["tasks"][0]["output"], "x".repeat(200));
    for i in [1, 2] {
        assert_eq!(reply["tasks"][i]["output_omitted"], true);
        assert!(reply["tasks"][i].get("output").is_none());
        assert_eq!(
            result_value(&j.call(STATUS_TOOL, json!({"task_id":ids[i]})).await)["output"],
            "x".repeat(200)
        );
    }
    assert!(reply["tasks"][3].get("output_omitted").is_none());
    j.close().await;
}

#[tokio::test]
async fn background_output_at_byte_limit_survives_escaping() {
    let mut j = Journey::new(runner(Response::Completed("\"".repeat(100))), limits());
    let id = j.start().await;
    assert_eq!(j.wait(&[id]).await["tasks"][0]["output"], "\"".repeat(100));
    j.close().await;
}

#[tokio::test]
async fn runtime_join_releases_capacity_after_terminal_status() {
    let mut j = Journey::new(runner(Response::Completed("ok".into())), limits());
    let id = j.start().await;
    assert_eq!(j.wait(&[id]).await["tasks"][0]["status"], "completed");
    j.joined().await;
    assert_eq!(j.ext.live_runners(), 0);
    let id = j.start().await;
    assert_eq!(j.wait(&[id]).await["tasks"][0]["status"], "completed");
    j.joined().await;
    j.close().await;
}

#[tokio::test]
async fn runner_faults_become_error_status_and_release_capacity() {
    for mode in 0..5 {
        let host: Runner = Arc::new(move |_| match mode {
            0 => Box::pin(async { Err(ExtensionError::Tool("HOST-PRIVATE-CANARY".into())) }),
            1 => panic!("HOST-PRIVATE-CANARY"),
            2 => Box::pin(PollPanic),
            3 => Box::pin(async { Ok(Response::Completed("x".repeat(101))) }),
            _ => Box::pin(async { Ok(Response::Completed("HOST-PRIVATE-CANARY\0".into())) }),
        });
        let mut j = Journey::new(host, limits());
        let id = j.start().await;
        let reply = j.wait(&[id]).await;
        assert_eq!(reply["tasks"][0]["status"], "error");
        assert!(reply["tasks"][0].get("output").is_none());
        j.joined().await;
        assert_eq!(
            j.start().await.len(),
            crabber_extensions::delegate_task::TASK_ID_BYTES
        );
        j.close().await;
    }
}

#[tokio::test]
async fn redactor_composition_protects_wait_output() {
    use crabber_extensions::tool_result_redactor::{self, Pattern, ToolResultRedactor};
    let mut j = Journey::new(
        runner(Response::Completed("SYNTHETIC_SECRET".into())),
        limits(),
    );
    let id = j.start().await;
    let redactor = Arc::new(
        ToolResultRedactor::new(tool_result_redactor::Options {
            order: 0,
            excluded_tools: vec![],
            additional_patterns: vec![Pattern {
                id: "synthetic".into(),
                expression: "SYNTHETIC_SECRET".into(),
            }],
            limits: tool_result_redactor::Limits {
                max_field_bytes: 4096,
                max_total_bytes: 16384,
                max_depth: 16,
                max_nodes: 100,
                max_matches_per_field: 16,
                max_patterns: 4,
                max_pattern_bytes: 256,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    );
    let provider = Arc::new(FakeProvider::scripted(vec![
        wait_call(std::slice::from_ref(&id), None),
        status_call(&id),
        done(),
    ]));
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(provider.clone())
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(j.ext.clone(), Scope::Global)
        .shared_extension(redactor, Scope::Global)
        .build()
        .unwrap();
    a.prompt(j.session.clone(), "collect")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&j.store, j.session.as_ref().unwrap()).await;
    for record in &records[1..] {
        let text = format!("{:?}", record.result);
        assert!(text.contains("[REDACTED]"));
        assert!(!text.contains("SYNTHETIC_SECRET"));
    }
    assert!(!format!("{:?}", provider.requests()).contains("SYNTHETIC_SECRET"));
    assert!(format!("{:?}", provider.requests()).contains("[REDACTED]"));
    a.close_extensions().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn worst_case_wait_bytes_bounds_a_serialized_single_id_reply() {
    let configured_limits = Limits {
        max_wait_ids: 1,
        max_wait_reply_bytes: 100,
        ..limits()
    };
    let bound = configured_limits.worst_case_wait_bytes();
    let mut j = Journey::new(
        runner(Response::Unavailable("\u{1}".repeat(100))),
        configured_limits,
    );
    let id = j.start().await;
    let reply = j.wait(&[id]).await;
    assert_eq!(reply["tasks"][0]["output"], "\u{1}".repeat(100));
    assert!(reply.to_string().len() <= bound);
    j.close().await;
}

#[tokio::test]
async fn hard_cap_wait_outputs_survive_typed_output() {
    let text = "\u{1}".repeat(160 * 1024);
    let mut j = Journey::new(
        runner(Response::Completed(text.clone())),
        Limits {
            max_result_bytes: 256 * 1024,
            max_wait_reply_bytes: 320 * 1024,
            ..limits()
        },
    );
    let a = j.start().await;
    let b = j.start().await;
    let (_registry, handle, tools) = mounted(j.ext.clone()).await;
    let reply = tools[WAIT_TOOL]
        .execute_json_with_context(
            context_for(j.session.clone().unwrap(), "workspace"),
            json!({"task_ids":[a,b]}),
        )
        .await
        .unwrap();
    assert_eq!(reply["status"], "done");
    for row in reply["tasks"].as_array().unwrap() {
        assert_eq!(row["output"], text);
        assert!(row.get("output_omitted").is_none());
    }
    handle.close().await.unwrap();
    j.close().await;
}

#[tokio::test]
async fn wait_bound_covers_maximal_detached_metadata_even_when_every_output_is_omitted() {
    use crabber::extension::SessionInvocations;
    let bounds = Limits {
        max_tracked: 64,
        max_wait_ids: 64,
        max_wait_reply_bytes: 1,
        ..limits()
    };
    let bound = bounds.worst_case_wait_bytes();
    let ext = Arc::new(configured(
        runner(Response::Unavailable("xx".into())),
        bounds,
    ));
    let (_, handle, tools) = mounted(ext).await;
    let host = Arc::new(crate::invocation_support::JoinedSpawner::default());
    let mut ids = Vec::new();
    for i in 0..64 {
        let mut context = context(CancellationToken::new())
            .with_session_invocations(SessionInvocations::from_host(host.clone()));
        context.call_id = ToolCallId::from(format!("metadata-{i}"));
        let reply = tools[START_TOOL]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap();
        assert_eq!(reply["status"], "started");
        ids.push(reply["task_id"].clone());
    }
    let reply = tools[WAIT_TOOL]
        .execute_json_with_context(context(CancellationToken::new()), json!({"task_ids":ids}))
        .await
        .unwrap();
    assert_eq!(reply["tasks"].as_array().unwrap().len(), 64);
    assert!(
        reply["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["detached"] == true && r["output_omitted"] == true)
    );
    assert!(serde_json::to_vec(&reply).unwrap().len() <= bound);
    handle.close().await.unwrap();
}
