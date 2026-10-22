use super::*;

#[test]
fn configuration_bounds_and_hash_cover_every_limit_and_identity() {
    let base = extension(Response::Completed(String::new())).config_hash();
    assert_eq!(
        base,
        extension(Response::Failed("different runner".into())).config_hash()
    );
    for (field, drift) in DRIFTS.iter().enumerate() {
        for over in [false, true] {
            let mut l = limits();
            match field {
                0 => l.max_task_bytes = if over { 64 * 1024 + 1 } else { 0 },
                1 => l.max_profile_bytes = if over { 257 } else { 0 },
                2 => l.max_result_bytes = if over { 256 * 1024 + 1 } else { 0 },
                3 => l.max_in_flight = if over { 257 } else { 0 },
                4 => {
                    l.max_wait = if over {
                        Duration::from_secs(3601)
                    } else {
                        Duration::ZERO
                    }
                }
                5 => {
                    l.shutdown_grace = if over {
                        Duration::from_secs(301)
                    } else {
                        Duration::ZERO
                    }
                }
                6 => l.max_tracked = if over { 257 } else { 0 },
                7 => {
                    l.max_background_duration = if over {
                        Duration::from_secs(86401)
                    } else {
                        Duration::ZERO
                    }
                }
                8 => l.max_wait_ids = if over { 65 } else { 0 },
                _ => l.max_wait_reply_bytes = if over { 1024 * 1024 + 1 } else { 0 },
            }
            let err = DelegateTask::new(Options {
                runner_identity: "host-v1".into(),
                runner: runner(Response::Completed(String::new())),
                limits: l,
            })
            .err()
            .unwrap();
            assert_eq!(
                err.to_string(),
                "extension plan failed: extension configuration invalid: delegate-task-limits"
            );
        }
        let mut l = limits();
        drift(&mut l);
        assert_ne!(
            base,
            configured(runner(Response::Completed(String::new())), l).config_hash()
        );
    }
    for identity in ["", " ", "x\n", &"x".repeat(257)] {
        let err = DelegateTask::new(Options {
            runner_identity: identity.into(),
            runner: runner(Response::Completed(String::new())),
            limits: limits(),
        })
        .err()
        .unwrap();
        assert!(err.to_string().ends_with("delegate-task-policy"));
    }
    let other = DelegateTask::new(Options {
        runner_identity: "host-v2".into(),
        runner: runner(Response::Completed(String::new())),
        limits: limits(),
    })
    .unwrap();
    assert_ne!(base, other.config_hash());
}

#[tokio::test]
async fn registered_tools_have_exact_contract_metadata_and_session_scope() {
    let registry = Registry::new();
    let session = SessionId::from("session");
    let handle = registry
        .mount(
            Arc::new(extension(Response::Completed(String::new()))),
            Scope::Session(session.clone()),
        )
        .await
        .unwrap();
    let plan = registry.acquire(&session);
    use crabber_extensions::delegate_task::{
        LIST_TOOL, PERMISSION_DELEGATE_READ, START_TOOL, STATUS_TOOL, WAIT_TOOL,
    };
    assert_eq!(
        plan.tools
            .iter()
            .map(|t| t.info.name.as_str())
            .collect::<Vec<_>>(),
        [TOOL_NAME, START_TOOL, STATUS_TOOL, LIST_TOOL, WAIT_TOOL]
    );
    for tool in &plan.tools[1..] {
        let start = tool.info.name == START_TOOL;
        assert_eq!(tool.info.retry_safe, !start);
        assert_eq!(
            tool.info.required_permissions,
            [if start {
                PERMISSION_DELEGATE
            } else {
                PERMISSION_DELEGATE_READ
            }]
        );
        if !start {
            for status in [
                "running",
                "completed",
                "failed",
                "rejected",
                "unavailable",
                "timed_out",
                "cancelled",
                "error",
            ] {
                assert!(tool.info.description.contains(status));
            }
        }
    }
    assert_eq!(plan.tools[1].info.parameters, plan.tools[0].info.parameters);
    assert_eq!(
        plan.tools[2].info.parameters,
        json!({"type":"object","additionalProperties":false,"required":["task_id"],"properties":{"task_id":{"type":"string"}}})
    );
    assert_eq!(
        plan.tools[3].info.parameters,
        json!({"type":"object","additionalProperties":false,"properties":{}})
    );
    assert_eq!(
        plan.tools[4].info.parameters,
        json!({"type":"object","additionalProperties":false,"required":["task_ids"],"properties":{"task_ids":{"type":"array","minItems":1,"items":{"type":"string"}},"timeout_seconds":{"type":"integer","minimum":1}}})
    );
    let info = &plan.tools[0].info;
    assert_eq!(info.name, TOOL_NAME);
    assert_eq!(
        info.description,
        "Delegate one bounded task under an opaque host profile. Returns completed, failed, rejected, unavailable, or timed_out."
    );
    assert!(!info.retry_safe);
    assert_eq!(info.required_permissions, [PERMISSION_DELEGATE]);
    assert_eq!(
        info.parameters,
        json!({"type":"object","additionalProperties":false,"required":["task","profile"],"properties":{"task":{"type":"string"},"profile":{"type":"string"}}})
    );
    assert!(registry.acquire(&SessionId::from("other")).tools.is_empty());
    drop(plan);
    handle.close().await.unwrap();
}

#[test]
fn worst_case_wait_bytes_saturates_and_matches_formula() {
    let mut limits = limits();
    assert_eq!(limits.worst_case_wait_bytes(), 300 * 6 + 4 * 160 + 64);
    limits.max_wait_reply_bytes = usize::MAX;
    assert_eq!(limits.worst_case_wait_bytes(), usize::MAX);
    limits.max_wait_reply_bytes = 1;
    limits.max_wait_ids = usize::MAX;
    assert_eq!(limits.worst_case_wait_bytes(), usize::MAX);
}

#[test]
fn worst_case_retained_bytes_includes_victims_and_lifecycles_and_saturates() {
    let mut configured = limits();
    assert_eq!(
        configured.worst_case_retained_bytes(),
        (2 * configured.max_tracked + configured.max_in_flight) * configured.max_result_bytes
    );
    configured.max_tracked = 65;
    assert_eq!(
        configured.worst_case_retained_bytes(),
        (129 + configured.max_in_flight) * configured.max_result_bytes
    );
    configured.max_tracked = usize::MAX;
    assert_eq!(configured.worst_case_retained_bytes(), usize::MAX);
    configured.max_tracked = 1;
    configured.max_in_flight = usize::MAX;
    assert_eq!(configured.worst_case_retained_bytes(), usize::MAX);
}
