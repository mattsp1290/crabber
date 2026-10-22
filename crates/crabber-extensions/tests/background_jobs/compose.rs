use super::observe::{
    ProbeMount, ResultProbe, assert_provider_record, assert_settled, terminal_for,
};
use super::*;
use crabber::extension::ToolOutcomeClass;
use crabber_extensions::{command_guard as guard, tool_result_redactor as redact};

#[path = "compose/fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test(flavor = "multi_thread")]
async fn redactor_protects_status_tails_but_not_memory() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.environment
            .overrides
            .insert("FIXTURE_SECRET".into(), MARKER.into());
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let (seed, session, raw) = seed(&h, &store).await;
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(STATUS_TOOL, json!({"id":raw["id"]})),
        done(),
    ]));
    let probe = Arc::new(ResultProbe::default());
    let a = composed(
        &h,
        store.clone(),
        provider.clone(),
        None,
        true,
        probe.clone(),
    );
    a.prompt(Some(session.clone()), "status")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let mut expected = raw.clone();
    expected["stdout"]["text"] = json!("[REDACTED]");
    assert_eq!(result_value(&records[0]), expected);
    assert_provider_record(&provider, 1, &records[0]);
    assert_settled(&store, &session, &records).await;
    protected(&store, &session, &provider).await;
    assert_eq!(
        terminal_for(&h, &session, raw["id"].as_str().unwrap()).await,
        raw
    );
    assert_eq!(probe.classes.lock().unwrap().len(), 1);
    a.close_extensions().await.unwrap();
    seed.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn command_guard_denies_matching_start_before_spawn() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let store = Arc::new(MemoryStore::new());
    let g = guard();
    let probe = Arc::new(ResultProbe::default());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(
            START_TOOL,
            json!({"command":"git push origin; : > \"$CANARY\""}),
        ),
        done(),
    ]));
    let a = composed(
        &h,
        store.clone(),
        provider,
        Some(g.clone()),
        false,
        probe.clone(),
    );
    let run = a.prompt(None, "deny").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records[0].status, ToolCallStatus::Failed);
    assert_eq!(result_value(&records[0]), json!("permission denied"));
    assert_eq!(
        *probe.classes.lock().unwrap(),
        vec![(records[0].id.clone(), ToolOutcomeClass::PermissionDenied)]
    );
    assert_eq!(g.stats().rule_match, 1);
    assert!(pgids(h.dir.path()).is_empty());
    assert!(!h.dir.path().join("canary").exists());
    assert_eq!(h.ext.live_jobs(), 0);
    assert_eq!(
        h.execute(LIST_TOOL, json!({})).await.unwrap(),
        json!({"jobs":[]})
    );
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(START_TOOL, json!({"command":command("printf ok")})),
        done(),
    ]));
    let positive = composed(&h, store.clone(), provider, Some(g.clone()), false, probe);
    positive
        .prompt(Some(session.clone()), "allow")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    let good = records
        .iter()
        .find(|r| r.status == ToolCallStatus::Completed)
        .unwrap();
    let result = result_value(good);
    assert_eq!(
        terminal_for(&h, &session, result["id"].as_str().unwrap()).await["stdout"]["text"],
        "ok"
    );
    assert_eq!(g.stats().rule_match, 1);
    positive.close_extensions().await.unwrap();
    a.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn guard_and_redactor_and_jobs_compose_in_one_turn() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.environment
            .overrides
            .insert("FIXTURE_SECRET".into(), MARKER.into());
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let (seed, session, raw) = seed(&h, &store).await;
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            (START_TOOL, json!({"command":"git push origin"})),
            (START_TOOL, json!({"command":command("printf allowed")})),
            (STATUS_TOOL, json!({"id":raw["id"]})),
        ]),
        done(),
    ]));
    let probe = Arc::new(ResultProbe::default());
    let g = guard();
    let a = composed(
        &h,
        store.clone(),
        provider.clone(),
        Some(g.clone()),
        true,
        probe.clone(),
    );
    a.prompt(Some(session.clone()), "compose")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 3);
    let observed = probe.classes.lock().unwrap().clone();
    assert_eq!(observed.len(), 3);
    for record in &records {
        let class = observed.iter().find(|(id, _)| *id == record.id).unwrap().1;
        assert_eq!(
            class,
            if record.status == ToolCallStatus::Failed {
                ToolOutcomeClass::PermissionDenied
            } else {
                ToolOutcomeClass::Succeeded
            }
        );
        assert_provider_record(&provider, 1, record);
    }
    let final_results = probe.final_results.lock().unwrap().clone();
    assert_eq!(final_results.len(), 3);
    for record in &records {
        assert_eq!(
            final_results
                .iter()
                .find(|(id, _)| *id == record.id)
                .unwrap()
                .1,
            result_value(record)
        );
    }
    let denied = records
        .iter()
        .find(|r| r.status == ToolCallStatus::Failed)
        .unwrap();
    assert_eq!(result_value(denied), json!("permission denied"));
    let status = records.iter().find(|r| r.name == STATUS_TOOL).unwrap();
    assert_eq!(result_value(status)["stdout"]["text"], "[REDACTED]");
    let started = records
        .iter()
        .find(|r| r.name == START_TOOL && r.status == ToolCallStatus::Completed)
        .unwrap();
    assert_eq!(
        terminal_for(&h, &session, result_value(started)["id"].as_str().unwrap()).await["stdout"]["text"],
        "allowed"
    );
    assert_eq!(g.stats().rule_match, 1);
    assert_settled(&store, &session, &records).await;
    protected(&store, &session, &provider).await;
    assert_eq!(
        terminal_for(&h, &session, raw["id"].as_str().unwrap()).await,
        raw
    );
    a.close_extensions().await.unwrap();
    seed.close_extensions().await.unwrap();
    h.close().await;
}
