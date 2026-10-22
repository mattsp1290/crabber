use super::fixture::*;
use crabber::{core::ToolCallStatus, extension::FactoryScope};
use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
};
use serde_json::json;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn shared_job_capacity_is_held_until_reaping_then_reusable() {
    let Some(h) = Harness::with_pools(
        FactoryScope::Session,
        runner(),
        [Some(jobs::SharedCapacity::new(1).unwrap()), None, None],
    ) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws-a", "a");
    let b = context(&h.root.path().join("b"), "ws-b", "b");
    let args = json!({"command":"printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/pgid.$PPID\"; sleep 60"});
    let records = h.first(&a, &[(jobs::START_TOOL, args.clone())]).await;
    let id = value(&records[0])["id"].clone();
    let refused = h.first(&b, &[(jobs::START_TOOL, args.clone())]).await;
    assert_eq!(refused[0].status, ToolCallStatus::Failed);
    assert!(
        value(&refused[0])
            .as_str()
            .unwrap()
            .ends_with("capacity-exhausted")
    );
    assert_eq!(
        h.jobs
            .owner_report("ws-b", Some(b.session().session_id()))
            .unwrap()
            .live,
        0
    );
    h.next(&a, &[(jobs::KILL_TOOL, json!({"id":id}))]).await;
    let admitted = h.next(&b, &[(jobs::START_TOOL, args)]).await;
    assert_eq!(value(&admitted[0])["state"], "running");
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_python_capacity_is_held_by_idle_interpreters_until_clear() {
    let Some(h) = Harness::with_pools(
        FactoryScope::Session,
        runner(),
        [None, Some(python::SharedCapacity::new(1).unwrap()), None],
    ) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws-a", "a");
    let b = context(&h.root.path().join("b"), "ws-b", "b");
    let args = json!({"code":record_code("x=1; x")});
    assert_eq!(
        value(&h.first(&a, &[(python::EXECUTE_TOOL, args.clone())]).await[0])["result"]["text"],
        "1"
    );
    let refused = h.first(&b, &[(python::EXECUTE_TOOL, args.clone())]).await;
    assert_eq!(refused[0].status, ToolCallStatus::Failed);
    assert!(
        value(&refused[0])
            .as_str()
            .unwrap()
            .ends_with("capacity-exhausted")
    );
    h.next(&a, &[(python::CLEAR_TOOL, json!({}))]).await;
    assert_eq!(
        value(&h.next(&b, &[(python::EXECUTE_TOOL, args)]).await[0])["result"]["text"],
        "1"
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_delegate_capacity_returns_after_runner_exit() {
    let Some(h) = Harness::with_pools(
        FactoryScope::Session,
        runner(),
        [None, None, Some(delegate::SharedCapacity::new(1).unwrap())],
    ) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws-a", "a");
    let b = context(&h.root.path().join("b"), "ws-b", "b");
    let args = json!({"task":"hold","profile":"test"});
    assert_eq!(
        value(&h.first(&a, &[(delegate::START_TOOL, args.clone())]).await[0])["status"],
        "started"
    );
    assert_eq!(
        value(&h.first(&b, &[(delegate::START_TOOL, args.clone())]).await[0]),
        json!({"status":"unavailable"})
    );
    assert_eq!(
        h.delegate
            .owner_report("ws-b", Some(b.session().session_id()))
            .unwrap()
            .tracked,
        0
    );
    h.agent
        .close_session_resources(a.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        value(&h.next(&b, &[(delegate::START_TOOL, args)]).await[0])["status"],
        "started"
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn workspace_scope_shares_capacity_but_keeps_session_state_and_workspace_close() {
    let Some(h) = Harness::new(FactoryScope::Workspace, runner()) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws", "a");
    let b = context(&h.root.path().join("a"), "ws", "b");
    let first = h
        .first(
            &a,
            &[
                (python::EXECUTE_TOOL, json!({"code":record_code("x=1; x")})),
                (
                    delegate::START_TOOL,
                    json!({"task":"hold","profile":"test"}),
                ),
            ],
        )
        .await;
    assert_eq!(value(&first[0])["result"]["text"], "1");
    let second = h
        .first(
            &b,
            &[
                (
                    python::EXECUTE_TOOL,
                    json!({"code":record_code("x=globals().get('x',0)+1; x")}),
                ),
                (delegate::LIST_TOOL, json!({})),
                (
                    delegate::START_TOOL,
                    json!({"task":"hold","profile":"test"}),
                ),
            ],
        )
        .await;
    assert_eq!(value(&second[0])["result"]["text"], "1");
    assert_eq!(value(&second[1]), json!({"tasks":[]}));
    assert_eq!(value(&second[2]), json!({"status":"unavailable"}));
    assert_eq!(h.python.owner_report("ws", None).unwrap().tracked, 2);
    h.agent
        .close_session_resources(a.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        value(
            &h.next(&b, &[(python::EXECUTE_TOOL, json!({"code":"x+=1; x"}))])
                .await[0]
        )["result"]["text"],
        "2"
    );
    assert_eq!(h.python.owner_report("ws", None).unwrap().live, 2);
    h.agent
        .close_workspace_resources(b.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        h.agent.prompt_in(&b, "closed workspace", None).await,
        Err(crabber::RuntimeError::ExtensionPlan(
            crabber::ExtensionError::ContextOwnerClosed
        ))
    ));
    h.close().await;
}
