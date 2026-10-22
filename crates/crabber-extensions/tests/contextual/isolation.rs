use super::{agent_support, fixture::*};
use crabber::{
    core::RunStatus,
    extension::{ExtensionFactory, FactoryScope},
};
use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_workspaces_three_sessions_are_isolated_and_close_targets_one_owner() {
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runner: delegate::Runner = {
        let seen = seen.clone();
        Arc::new(move |request| {
            let barrier = barrier.clone();
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock().unwrap().push((
                    request.session_id().clone(),
                    request.workspace().workspace_id().unwrap().to_owned(),
                    request.workspace().directory().unwrap().to_owned(),
                    request.background(),
                ));
                if request.background() {
                    request.cancellation().cancelled().await;
                } else {
                    barrier.wait().await;
                }
                Ok(delegate::Response::Completed("owned".into()))
            })
        })
    };
    let Some(h) = Harness::new(FactoryScope::Session, runner) else {
        return;
    };
    let alias = h.root.path().join("a-alias");
    std::os::unix::fs::symlink(h.root.path().join("a"), &alias).unwrap();
    let a = context(&alias, "ws-a", "session-a");
    let b = context(&alias, "ws-a", "session-b");
    let c = context(&h.root.path().join("b"), "ws-b", "session-c");
    let provider = h.scripts.set(vec![
        seed_calls(),
        seed_calls(),
        seed_calls(),
        done(),
        done(),
        done(),
    ]);
    let runs = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(
            agent_support::first_prompt(&h.agent, &a, "seed"),
            agent_support::first_prompt(&h.agent, &b, "seed"),
            agent_support::first_prompt(&h.agent, &c, "seed")
        )
    })
    .await
    .unwrap();
    let mut job_ids = Vec::new();
    for (execution, run) in [
        (&a, runs.0.unwrap()),
        (&b, runs.1.unwrap()),
        (&c, runs.2.unwrap()),
    ] {
        let id = run.run_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let records = h.records(execution, &id).await;
        assert_eq!(records.len(), 4);
        let job = value(records.iter().find(|r| r.name == jobs::START_TOOL).unwrap());
        assert_eq!(job["state"], "running");
        job_ids.push(job["id"].as_str().unwrap().to_owned());
        let py = value(
            records
                .iter()
                .find(|r| r.name == python::EXECUTE_TOOL)
                .unwrap(),
        );
        assert_eq!(py["status"], "completed");
        // getcwd resolves directory aliases, including macOS /var -> /private/var.
        let python_directory = std::fs::canonicalize(execution.session().directory()).unwrap();
        assert_eq!(
            py["result"]["text"],
            format!("(1, '{}')", python_directory.display())
        );
        let background = value(
            records
                .iter()
                .find(|r| r.name == delegate::START_TOOL)
                .unwrap(),
        );
        assert_eq!(background["status"], "started");
        assert_eq!(
            h.jobs
                .owner_report(
                    execution.session().workspace_id(),
                    Some(execution.session().session_id())
                )
                .unwrap()
                .live,
            1
        );
        assert_eq!(
            h.python
                .owner_report(
                    execution.session().workspace_id(),
                    Some(execution.session().session_id())
                )
                .unwrap()
                .live,
            1
        );
        assert_eq!(
            h.delegate
                .owner_report(
                    execution.session().workspace_id(),
                    Some(execution.session().session_id())
                )
                .unwrap()
                .live,
            1
        );
    }
    for request in provider.requests() {
        for tool in [jobs::START_TOOL, python::EXECUTE_TOOL, delegate::START_TOOL] {
            assert!(request.tools.iter().any(|info| info.name == tool));
        }
    }
    for (execution, own_job) in [(&a, &job_ids[0]), (&b, &job_ids[1]), (&c, &job_ids[2])] {
        let records = h
            .next(
                execution,
                &[
                    (jobs::LIST_TOOL, json!({})),
                    (delegate::LIST_TOOL, json!({})),
                    (python::EXECUTE_TOOL, json!({"code":"x"})),
                ],
            )
            .await;
        let jobs = value(&records[0]);
        assert_eq!(jobs["jobs"].as_array().unwrap().len(), 1);
        assert_eq!(jobs["jobs"][0]["id"], own_job.as_str());
        assert_eq!(value(&records[1])["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(value(&records[2])["result"]["text"], "1");
    }
    let observed = seen.lock().unwrap().clone();
    assert_eq!(observed.len(), 6);
    for execution in [&a, &b, &c] {
        assert_eq!(
            observed
                .iter()
                .filter(|(s, w, d, _)| s == execution.session().session_id()
                    && w == execution.session().workspace_id()
                    && d == execution.session().directory())
                .count(),
            2
        );
    }
    h.agent
        .close_session_resources(a.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(matches!(
        h.agent.prompt_in(&a, "closed", None).await,
        Err(crabber::RuntimeError::ExtensionPlan(
            crabber::ExtensionError::ContextOwnerClosed
        ))
    ));
    let records = h
        .next(&b, &[(python::EXECUTE_TOOL, json!({"code":"x+=1; x"}))])
        .await;
    assert_eq!(value(&records[0])["result"]["text"], "2");
    assert_eq!(
        h.jobs
            .owner_report("ws-a", Some(b.session().session_id()))
            .unwrap()
            .live,
        1
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn factory_creation_and_install_have_no_business_effects() {
    let calls = Arc::new(AtomicUsize::new(0));
    let runner: delegate::Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(delegate::Response::Completed("done".into())) })
        })
    };
    let Some(h) = Harness::new(FactoryScope::Session, runner) else {
        return;
    };
    h.agent.ready().await.unwrap();
    assert!(h.jobs.owner_report("ws", None).is_none());
    let ctx = context(&h.root.path().join("a"), "ws", "session");
    h.scripts.set(vec![done()]);
    agent_support::first_prompt(&h.agent, &ctx, "admit only")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    for report in [
        h.jobs.owner_report("ws", Some(ctx.session().session_id())),
        h.python
            .owner_report("ws", Some(ctx.session().session_id())),
        h.delegate
            .owner_report("ws", Some(ctx.session().session_id())),
    ] {
        let report = report.unwrap();
        assert_eq!((report.live, report.tracked), (0, 0));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_dir(h.root.path().join("temporary"))
            .unwrap()
            .count(),
        0
    );
    h.close().await;
}

#[test]
fn aggregate_capacity_and_scope_enter_factory_fingerprints() {
    let root = tempfile::tempdir().unwrap();
    let a = jobs::Factory::new(super::options::jobs(root.path()), FactoryScope::Session).unwrap();
    let b = jobs::Factory::new(super::options::jobs(root.path()), FactoryScope::Workspace).unwrap();
    let c = jobs::Factory::new(super::options::jobs(root.path()), FactoryScope::Session)
        .unwrap()
        .with_shared_capacity(jobs::SharedCapacity::new(1).unwrap());
    assert_ne!(a.config_hash(), b.config_hash());
    assert_ne!(a.config_hash(), c.config_hash());
    assert!(jobs::SharedCapacity::new(0).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn construction_and_admission_never_execute_host_binaries_or_runner() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let binary = root.path().join("host-binary");
    std::fs::create_dir(root.path().join("temporary")).unwrap();
    std::fs::write(
        &binary,
        "#!/bin/sh\n: > \"${FIXTURE_ROOT:?}/unexpected-execution\"\nexit 77\n",
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700)).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let host: delegate::Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(delegate::Response::Completed("unexpected".into())) })
        })
    };
    let mut job_options = super::options::jobs(root.path());
    job_options.shell_path = binary.clone();
    let jobs = Arc::new(jobs::Factory::new(job_options, FactoryScope::Session).unwrap());
    let python = Arc::new(
        python::Factory::new(
            super::options::python_with_interpreter(root.path(), binary),
            FactoryScope::Session,
        )
        .unwrap(),
    );
    let delegate = Arc::new(
        delegate::Factory::new(super::options::delegate(host), FactoryScope::Session).unwrap(),
    );
    let agent = agent_support::builder()
        .memory()
        .provider(Arc::new(crabber::FakeProvider::scripted(vec![done()])))
        .config(crabber::AgentConfig::new(selection()))
        .extension_factory(jobs.clone())
        .extension_factory(python.clone())
        .extension_factory(delegate.clone())
        .build()
        .unwrap();
    agent.ready().await.unwrap();
    let ctx = context(root.path(), "ws", "session");
    agent_support::first_prompt(&agent, &ctx, "admit only")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!root.path().join("unexpected-execution").exists());
    assert_eq!(
        std::fs::read_dir(root.path().join("temporary"))
            .unwrap()
            .count(),
        0
    );
    for report in [
        jobs.owner_report("ws", Some(ctx.session().session_id())),
        python.owner_report("ws", Some(ctx.session().session_id())),
        delegate.owner_report("ws", Some(ctx.session().session_id())),
    ] {
        let report = report.unwrap();
        assert_eq!((report.live, report.tracked), (0, 0));
    }
    assert!(agent.close(Duration::from_secs(5)).await.local_drained());
}
