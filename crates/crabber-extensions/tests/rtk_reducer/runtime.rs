use super::observe::assert_three_surfaces;
use super::*;
const REDUCE: &str = "/bin/cat >/dev/null; printf R:x";
async fn check(j: &Journey, expected: &Value) {
    let session = j.run().await;
    let records = snapshot_tools(&j.store, &session).await;
    assert_eq!(
        records[0].result.as_ref().unwrap().status,
        crabber::core::ToolResultStatus::Completed
    );
    assert_three_surfaces(&j.store, &session, &j.provider, &records[0].id, expected).await;
    assert_eq!(j.probe.final_results.lock().unwrap()[0].1, *expected);
    assert_eq!(j.executor.executions.load(Ordering::Relaxed), 1);
    j.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn later_field_failure_rolls_back_and_empty_fields_never_spawn() {
    if !shell_available() {
        return;
    }
    let original = json!({"stdout":"x".repeat(300),"diff":"y".repeat(300)});
    let body = "/bin/cat >/dev/null; if [ \"$3\" = cargo-test ]; then exit 3; fi; printf R:x";
    let j = Journey::new(
        body,
        original.clone(),
        "fixture_json",
        json!({"cmd":"git diff"}),
        |_| {},
    );
    check(&j, &original).await;
    assert_eq!(j.argv().len(), 2);
    assert_eq!(j.ext.stats().reduced, 0);
    let j = Journey::new(REDUCE, json!(""), "fixture_text", json!({}), |_| {});
    check(&j, &json!("")).await;
    assert!(j.argv().is_empty());
}
#[tokio::test(flavor = "multi_thread")]
async fn text_reduction_durable_journey() {
    if !shell_available() {
        return;
    }
    let j = Journey::new(
        REDUCE,
        json!("x".repeat(300)),
        "fixture_text",
        json!({}),
        |_| {},
    );
    check(&j, &json!(marker(Filter::Log, 300) + "R:x")).await;
    assert_eq!(j.argv(), ["pipe --filter log"]);
    assert_eq!(j.ext.stats().reduced, 1);
}
#[tokio::test(flavor = "multi_thread")]
async fn json_fields_reduce_selected_members_with_one_lease() {
    if !shell_available() {
        return;
    }
    let value = json!({"stdout":"x".repeat(300), "diff":"y".repeat(400), "stderr":"unchanged", "exit_code":0, "nested":{"out":[1,2,3]}});
    let mut expected = value.clone();
    expected["stdout"] = json!(marker(Filter::CargoTest, 300) + "R:x");
    expected["diff"] = json!(marker(Filter::GitDiff, 400) + "R:x");
    let j = Journey::new(
        REDUCE,
        value,
        "fixture_json",
        json!({"cmd":"cargo test --workspace"}),
        |o| o.limits.max_concurrent = 1,
    );
    check(&j, &expected).await;
    assert_eq!(
        j.argv(),
        ["pipe --filter git-diff", "pipe --filter cargo-test"]
    );
    assert_eq!(j.ext.stats().capacity_denied, 0);
}
#[tokio::test(flavor = "multi_thread")]
async fn encoded_length_decides_acceptance() {
    if !shell_available() {
        return;
    }
    for (original, output, accepted, raw_smaller) in [
        ("a".repeat(100), "a".repeat(30), true, true),
        ("a".repeat(100), "\"".repeat(55), false, true),
        ("\n".repeat(100), "a".repeat(110), true, false),
    ] {
        let candidate = marker(Filter::Log, original.len()) + &output;
        assert_eq!(candidate.len() < original.len(), raw_smaller);
        assert_eq!(
            serde_json::to_string(&candidate).unwrap().len()
                < serde_json::to_string(&original).unwrap().len(),
            accepted
        );
        let body = format!("/bin/cat >/dev/null; printf '%s' '{output}'");
        let j = Journey::new(&body, json!(original), "fixture_text", json!({}), |_| {});
        check(&j, &json!(if accepted { candidate } else { original })).await;
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn no_gain_identity_shapes_and_input_match_pass_through() {
    if !shell_available() {
        return;
    }
    for body in [
        "/bin/cat",
        "/bin/cat >/dev/null; /usr/bin/head -c 500 /dev/zero | /usr/bin/tr '\\000' x",
    ] {
        let value = json!("x".repeat(300));
        let j = Journey::new(body, value.clone(), "fixture_text", json!({}), |_| {});
        check(&j, &value).await;
        assert_eq!(j.ext.stats().no_gain, 1);
    }
    for value in [
        json!("string"),
        json!({"stdout":"x"}),
        json!({"stdout":0,"diff":"x"}),
        json!({"stdout":"\0","diff":"x"}),
    ] {
        let j = Journey::new(
            REDUCE,
            value.clone(),
            "fixture_json",
            json!({"cmd":"git diff"}),
            |_| {},
        );
        check(&j, &value).await;
        assert!(j.argv().is_empty());
    }
    let value = json!({"stdout":"x".repeat(300),"diff":"x".repeat(300)});
    for args in [
        json!({"cmd":"cargo test"}),
        json!({}),
        json!({"cmd":3}),
        json!({"cmd":"x".repeat(4097)}),
    ] {
        let j = Journey::new(REDUCE, value.clone(), "fixture_json", args, |_| {});
        check(&j, &value).await;
        assert!(j.argv().is_empty());
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn field_bound_and_process_failures_pass_through() {
    if !shell_available() {
        return;
    }
    let value = json!("x".repeat(65));
    let j = Journey::new(REDUCE, value.clone(), "fixture_text", json!({}), |o| {
        o.limits.max_field_bytes = 64;
        o.limits.max_stdout_bytes = 64;
    });
    check(&j, &value).await;
    assert!(j.argv().is_empty());
    for body in [
        "printf no; exit 3",
        "/bin/cat >/dev/null",
        "/bin/cat >/dev/null; printf '\\377\\376'",
        // Keep the overflow producer alive through TERM so this positive
        // cleanup fixture reaches KILL without a Darwin exit-transition race.
        "trap '' TERM; exec /usr/bin/head -c 2097152 /dev/zero",
        "printf R:x; exit 0",
    ] {
        let value = json!("x".repeat(512 * 1024));
        let j = Journey::new(body, value.clone(), "fixture_text", json!({}), |_| {});
        check(&j, &value).await;
        assert_eq!(j.ext.stats().process_failed, 1);
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn timeout_passes_through_and_reaps() {
    if !shell_available() {
        return;
    }
    let value = json!("x".repeat(300));
    let j = Journey::new(
        "trap '' TERM; exec /bin/sleep 60",
        value.clone(),
        "fixture_text",
        json!({}),
        |o| o.limits.timeout = Duration::from_secs(1),
    );
    let start = std::time::Instant::now();
    check(&j, &value).await;
    assert!(start.elapsed() < Duration::from_secs(6));
    assert_eq!(j.ext.stats().timed_out, 1);
}
