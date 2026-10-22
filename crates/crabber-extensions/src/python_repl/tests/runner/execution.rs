use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn state_expression_errors_fds_and_flags() {
    let Some(mut runner) = real().await else {
        return;
    };
    assert_eq!(response(&mut runner, "x = 1").await.result.text, "");
    assert_eq!(response(&mut runner, "x + 1").await.result.text, "2");
    let error = response(&mut runner, "x / 0").await;
    assert_eq!(error.status, Status::PythonError);
    assert!(error.exception.text.contains("<python_repl>"));
    assert!(!error.exception.text.contains("<string>"));
    assert_eq!(error.result.text, "");
    assert_eq!(
        response(&mut runner, "import os; os.write(1, b'junk'); input()")
            .await
            .status,
        Status::PythonError
    );
    let flags = response(&mut runner, "import sys; (sys.flags.isolated, sys.flags.dont_write_bytecode, sys.flags.no_site, sys.prefix == sys.base_prefix, sys.executable)").await.result.text;
    assert!(flags.starts_with("(1, 1, 0, True, "));
    assert!(
        flags.contains(python().unwrap().to_str().unwrap()),
        "flags: {flags:?}, python: {:?}",
        python()
    );
    assert_eq!(
        response(&mut runner, "'REQUEST_FD' in globals()")
            .await
            .result
            .text,
        "False"
    );
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn output_utf8_prefix_across_writes_and_unencodable_text() {
    let Some(mut runner) = real().await else {
        return;
    };
    let response = response(&mut runner, "import sys; sys.stdout.write('a'*31); sys.stdout.write('é'); sys.stdout.write('z'); sys.stderr.write('\\ud800')").await;
    assert_eq!(response.stdout.text, "a".repeat(31));
    assert!(response.stdout.truncated);
    assert_eq!(response.stderr.text, "?");
    assert!(!response.stderr.truncated);
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_before_write_is_not_executed() {
    let Some(mut runner) = real().await else {
        return;
    };
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        runner
            .execute(
                "x = 1",
                &cancel,
                &CancellationToken::new(),
                Instant::now() + WAIT
            )
            .await,
        ExecuteOutcome::Interrupted {
            may_have_executed: false
        }
    ));
    stop(&mut runner).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn blocked_write_is_cancellable_and_bounded() {
    for cancellation in [false, true] {
        let cancel = CancellationToken::new();
        let close = CancellationToken::new();
        let Some(result) = start(READY_SLEEP, &cancel, &close, WAIT).await else {
            return;
        };
        let mut runner = result.unwrap_or_else(|e| panic!("fixture startup: {:?}", e.fault));
        if cancellation {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(100)).await;
                cancel.cancel();
            });
        }
        let outcome = runner
            .execute(
                &"x".repeat(90_000),
                &cancel,
                &close,
                Instant::now() + Duration::from_millis(200),
            )
            .await;
        if cancellation {
            assert!(matches!(
                outcome,
                ExecuteOutcome::Interrupted {
                    may_have_executed: false
                }
            ));
        } else {
            assert!(matches!(
                outcome,
                ExecuteOutcome::TimedOut {
                    may_have_executed: false
                }
            ));
        }
        stop(&mut runner).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn close_before_and_after_write() {
    for before in [false, true] {
        let Some(mut runner) = real().await else {
            return;
        };
        let close = CancellationToken::new();
        if before {
            close.cancel();
        } else {
            let close = close.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(100)).await;
                close.cancel();
            });
        }
        let outcome = runner
            .execute(
                "import time; time.sleep(60)",
                &CancellationToken::new(),
                &close,
                Instant::now() + WAIT,
            )
            .await;
        match outcome {
            ExecuteOutcome::Closing { may_have_executed } => assert_eq!(may_have_executed, !before),
            other => panic!("{other:?}"),
        }
        stop(&mut runner).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn descendant_environment_and_null_fds() {
    let Some(path) = python() else { return };
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("home");
    std::fs::create_dir(&private).unwrap();
    // System-wide APM injection can instrument the nested interpreter even
    // with env_clear(), adding exporter work to this descriptor/environment
    // fixture. Keep both processes synthetic and free of host instrumentation.
    let environment = vec![
        ("HOME".into(), private.to_str().unwrap().into()),
        ("DD_INSTRUMENT_SERVICE_WITH_APM".into(), "false".into()),
    ];
    let args = arguments(RUNNER_SOURCE, BOUNDS);
    let mut runner = Runner::start(
        RunnerLaunch {
            program: &path,
            args: &args,
            directory: directory.path(),
            environment: &environment,
        },
        BOUNDS,
        &CancellationToken::new(),
        &CancellationToken::new(),
        Instant::now() + WAIT,
        GRACE,
        WAIT,
    )
    .await
    .unwrap_or_else(|e| panic!("{:?}", e.fault));
    let code = "import subprocess,sys; subprocess.check_output([sys.executable, '-c', 'import os; print(os.environ[\"HOME\"]); print(os.read(0, 1))']).decode()";
    let result = response(&mut runner, code).await.result.text;
    assert!(result.contains(private.to_str().unwrap()));
    assert!(result.contains("b''"));
    stop(&mut runner).await;
}
