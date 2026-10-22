use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn kill_terminates_the_whole_group_and_recovers_the_slot() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.max_running = 1).await;
    let first = h.start("sleep 60 & exec sleep 60").await;
    timeout(Duration::from_secs(5), async {
        while group_members(pgids(h.dir.path())[0]).len() < 4 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        h.execute(START_TOOL, json!({"command":"sleep 60"}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("capacity-exhausted")
    );
    assert_eq!(
        h.kill(id(&first)).await,
        json!({"id":id(&first),"state":"killed","newly_accepted":true})
    );
    groups_gone(h.dir.path()).await;
    assert_eq!(h.ext.live_jobs(), 0);
    let next = h.start("sleep 60").await;
    assert_eq!(h.kill(id(&first)).await["newly_accepted"], false);
    h.kill(id(&next)).await;
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn term_phase_reaches_the_command_before_kill() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    // Exercise the command shell's trap without an unrelated child launch/wait.
    let job = h
        .start("trap 'printf T > \"$CANARY\"; exit 0' TERM; : > ready; while :; do :; done")
        .await;
    wait_file(&h.dir.path().join("ready")).await;
    let start = Instant::now();
    assert_eq!(h.kill(id(&job)).await["state"], "killed");
    assert_eq!(
        std::fs::read_to_string(h.dir.path().join("canary")).unwrap(),
        "T"
    );
    assert!(start.elapsed() < Duration::from_secs(2));
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_delivers_term_then_kill() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.max_running = 1).await;
    let start = Instant::now();
    let job = h
        .execute(
            START_TOOL,
            json!({"command":command("trap '' TERM; : > ready; sleep 60"),"timeout_seconds":1}),
        )
        .await
        .unwrap();
    wait_file(&h.dir.path().join("ready")).await;
    let terminal = h.terminal(id(&job)).await;
    assert_eq!(terminal["state"], "timed_out");
    assert!(terminal.get("exit_code").is_none());
    assert!(start.elapsed() < Duration::from_secs(7));
    groups_gone(h.dir.path()).await;
    assert_eq!(h.ext.live_jobs(), 0);
    let next = h.start("exit 0").await;
    h.terminal(id(&next)).await;
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn default_timeout_applies_to_omitted_and_zero() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.default_timeout = Duration::from_secs(1)).await;
    for args in [
        json!({"command":command("sleep 60")}),
        json!({"command":command("sleep 60"),"timeout_seconds":0}),
    ] {
        let job = h.execute(START_TOOL, args).await.unwrap();
        assert_eq!(job["timeout_seconds"], 1);
        assert_eq!(h.terminal(id(&job)).await["state"], "timed_out");
    }
    let job = h
        .execute(
            START_TOOL,
            json!({"command":command("sleep 60"),"timeout_seconds":5}),
        )
        .await
        .unwrap();
    assert_eq!(job["timeout_seconds"], 5);
    h.kill(id(&job)).await;
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn finished_jobs_are_pruned_oldest_first_only_when_needed() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.max_tracked = 2).await;
    let a = h.start("exit 0").await;
    h.terminal(id(&a)).await;
    let b = h.start("exit 0").await;
    h.terminal(id(&b)).await;
    assert_eq!(
        h.execute(LIST_TOOL, json!({})).await.unwrap()["jobs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let c = h.start("sleep 60").await;
    assert!(
        h.execute(STATUS_TOOL, json!({"id":id(&a)}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("job-not-found")
    );
    assert_eq!(h.status(id(&b)).await["state"], "succeeded");
    h.kill(id(&c)).await;
    h.close().await;
    let h = Harness::new(|o| o.limits.max_tracked = 3).await;
    let running = h.start("sleep 60").await;
    let older = h.start("exit 0").await;
    h.terminal(id(&older)).await;
    let newer = h.start("exit 0").await;
    h.terminal(id(&newer)).await;
    let another = h.start("sleep 60").await;
    assert_eq!(h.status(id(&running)).await["state"], "running");
    assert_eq!(h.status(id(&newer)).await["state"], "succeeded");
    assert!(
        h.execute(STATUS_TOOL, json!({"id":id(&older)}))
            .await
            .is_err()
    );
    h.kill(id(&running)).await;
    h.kill(id(&another)).await;
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn kill_with_a_detached_output_holder_forces_tails() {
    if !shell_available() {
        return;
    }
    let Some(python) = owned_process::python() else {
        return;
    };
    let h = Harness::new(|o| {
        o.limits.kill_wait = Duration::from_millis(500);
        o.environment
            .overrides
            .insert("PYTHON".into(), python.to_str().unwrap().into());
        // Working directory is the per-test root; holder ownership is published there.
        o.environment
            .overrides
            .insert("HOLDER".into(), "holder".into());
    })
    .await;
    let mut cleanup = owned_process::ProcessCleanup::new(h.dir.path());
    let job = h
        .start(&format!("{}; sleep 60", owned_process::HOLDER_COMMAND))
        .await;
    cleanup.set_group(pgids(h.dir.path())[0]);
    wait_file(&h.dir.path().join("holder.ready")).await;
    assert!(cleanup.holder_alive());
    let start = Instant::now();
    assert_eq!(h.kill(id(&job)).await["state"], "killed");
    assert!(start.elapsed() < Duration::from_secs(2));
    let status = h.status(id(&job)).await;
    assert_eq!(status["stdout"]["truncated"], true);
    assert_eq!(status["stderr"]["truncated"], true);
    assert!(status.get("exit_code").is_none());
    assert!(cleanup.holder_alive());
    groups_gone(h.dir.path()).await;
    assert_eq!(h.ext.live_jobs(), 0);
    cleanup.kill_holder().await;
    cleanup.disarm_groups();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn termination_continues_after_kill_caller_cancel() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| o.limits.terminate_grace = Duration::from_secs(2)).await;
    let job = h
        .start("trap 'printf T > \"$CANARY\"' TERM; : > ready; while :; do sleep 0.01; done")
        .await;
    wait_file(&h.dir.path().join("ready")).await;
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        h.tools[KILL_TOOL]
            .execute_json_with_context(context(h.dir.path(), cancel), json!({"id":id(&job)}))
            .await
            .unwrap_err()
            .to_string()
            .ends_with("cancelled")
    );
    assert_eq!(h.status(id(&job)).await["state"], "running");
    let cancel = CancellationToken::new();
    let tool = h.tools[KILL_TOOL].clone();
    let ctx = context(h.dir.path(), cancel.clone());
    let args = json!({"id":id(&job)});
    let caller = tokio::spawn(async move { tool.execute_json_with_context(ctx, args).await });
    // A handled TERM proves the kill cause was accepted before cancellation.
    wait_file(&h.dir.path().join("canary")).await;
    cancel.cancel();
    assert!(
        caller
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .ends_with("cancelled")
    );
    assert_eq!(h.terminal(id(&job)).await["state"], "killed");
    h.close().await;
}
