use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn argument_layout_matches_python_bootstrap_contract() {
    // This independent Python-side contract catches positional drift even when
    // production and fixtures share the Rust argument constructor.
    let source = format!(
        "import sys\nassert sys.argv[1:] == ['python-repl-runner-v1','100000','100000','32','32','1024','2048']\n{RUNNER_SOURCE}"
    );
    let Some(started) = start(
        &source,
        &CancellationToken::new(),
        &CancellationToken::new(),
        WAIT,
    )
    .await
    else {
        return;
    };
    let mut runner = started.unwrap_or_else(|error| panic!("bootstrap layout: {:?}", error.fault));
    assert_eq!(response(&mut runner, "42").await.result.text, "42");
    stop(&mut runner).await;
}

#[test]
fn runner_source_fits_argument_limits() {
    assert!(RUNNER_SOURCE.len() < 64 * 1024);
    assert_eq!(runner_digest().len(), 64);
}

#[tokio::test]
async fn frames_are_length_prefixed_and_bounded() {
    let frame = encode_frame(&json!({"a": 1}), 7).unwrap();
    assert_eq!(&frame[..4], &7u32.to_be_bytes());
    assert_eq!(
        read_frame(&mut frame.as_slice(), 7).await.unwrap(),
        br#"{"a":1}"#
    );
    assert!(encode_frame(&json!({"a":1}), 6).is_err());
    for raw in [
        vec![],
        vec![0, 0, 0],
        vec![0, 0, 0, 0],
        vec![0, 0, 0, 8],
        vec![0, 0, 0, 1],
    ] {
        assert!(read_frame(&mut raw.as_slice(), 7).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn incompatible_malformed_and_slow_readiness() {
    for (source, fault) in [
        (
            "import os,struct,json; b=json.dumps({'version':'python-repl-runner-v1','phase':'ready','python':[3,10]}).encode(); os.write(1,struct.pack('>I',len(b))+b)",
            StartFault::Bootstrap,
        ),
        ("print('junk')", StartFault::Readiness),
        (
            "import os,struct,time; os.write(1,struct.pack('>I',257)); time.sleep(60)",
            StartFault::Readiness,
        ),
        ("pass", StartFault::Readiness),
        ("import time; time.sleep(60)", StartFault::Timeout),
    ] {
        let Some(result) = start(
            source,
            &CancellationToken::new(),
            &CancellationToken::new(),
            if fault == StartFault::Timeout {
                Duration::from_millis(200)
            } else {
                WAIT
            },
        )
        .await
        else {
            return;
        };
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("unexpected ready"),
        };
        assert_eq!(error.fault, fault);
        // Unconfirmed initial cleanup returns its owner for a bounded retry.
        if let Some(mut runner) = error.runner {
            stop(&mut runner).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn start_is_interrupted_by_close_token() {
    let close = CancellationToken::new();
    let cloned = close.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        cloned.cancel();
    });
    let Some(result) = start(
        "import time; time.sleep(60)",
        &CancellationToken::new(),
        &close,
        WAIT,
    )
    .await
    else {
        return;
    };
    let Err(error) = result else {
        panic!("unexpected ready");
    };
    assert_eq!(error.fault, StartFault::Closing);
    if let Some(mut runner) = error.runner {
        stop(&mut runner).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn responses_require_matching_identity_bounds_and_status_zeroes() {
    let base = json!({"version":RUNNER_PROTOCOL, "id":1, "status":"completed",
        "stdout":{"text":"","truncated":false}, "stderr":{"text":"","truncated":false},
        "result":{"text":"","truncated":false}, "exception":{"text":"","truncated":false}});
    let mut invalid = Vec::new();
    for (field, value) in [
        ("version", json!("wrong")),
        ("id", json!(2)),
        ("status", json!("wrong")),
    ] {
        let mut item = base.clone();
        item[field] = value;
        invalid.push(item);
    }
    for (field, length) in [
        ("stdout", BOUNDS.output),
        ("stderr", BOUNDS.output),
        ("result", BOUNDS.result),
        ("exception", BOUNDS.exception),
    ] {
        let mut item = base.clone();
        item[field]["text"] = json!("a".repeat(length + 1));
        invalid.push(item);
    }
    let mut item = base.clone();
    item["exception"]["truncated"] = json!(true);
    invalid.push(item);
    let mut item = base.clone();
    item["status"] = json!("python_error");
    item["result"]["text"] = json!("forbidden");
    invalid.push(item);
    let mut item = base.clone();
    item["unknown"] = json!(1);
    invalid.push(item);
    for item in invalid {
        let source = format!(
            "import os,struct,json,time\ndef send(b): os.write(1,struct.pack('>I',len(b))+b)\nsend(b'{{\"version\":\"python-repl-runner-v1\",\"phase\":\"ready\",\"python\":[3,14]}}')\nos.read(0,65536)\nsend({}.encode())\ntime.sleep(60)",
            serde_json::to_string(&item.to_string()).unwrap()
        );
        let Some(started) = start(
            &source,
            &CancellationToken::new(),
            &CancellationToken::new(),
            WAIT,
        )
        .await
        else {
            return;
        };
        let mut runner = started.unwrap_or_else(|e| panic!("fixture startup: {:?}", e.fault));
        assert!(matches!(
            runner
                .execute(
                    "1",
                    &CancellationToken::new(),
                    &CancellationToken::new(),
                    Instant::now() + WAIT
                )
                .await,
            ExecuteOutcome::Failed {
                may_have_executed: true
            }
        ));
        stop(&mut runner).await;
    }
}
