//! Cross-domain native handoff through public workers and cold adapters.
use super::*;
#[path = "handoff/journey.rs"]
mod fixture;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use fixture::header;
use fixture::journey;
#[tokio::test]
async fn memory_bidirectional_approval_tool_handoff() {
    journey(Arc::new(MemoryStore::new())).await;
    fixture::cancel_handoffs(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_bidirectional_approval_tool_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("handoff.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let (session, run) = journey(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
    let cold = crabber::session::SqliteStore::connect(&path).await.unwrap();
    header(&cold, &session, &run).await;
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    sqlite_corruption(&cold, &pool, &session, &run).await;
    let cold = Arc::new(cold);
    fixture::cancel_handoffs(cold.clone()).await;
    fixture::cancel_with_probe(cold.clone(), |request| async move {
        sqlite_terminal_corruption(cold.as_ref(), &pool, request).await;
    })
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_bidirectional_approval_tool_handoff() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let (session, run) = journey(Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        ))
        .await;
        let cold = crabber::session::PostgresStore::connect(&url)
            .await
            .unwrap();
        header(&cold, &session, &run).await;
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        postgres_corruption(&cold, &pool, &session, &run).await;
        let cold = Arc::new(cold);
        fixture::cancel_handoffs(cold.clone()).await;
        fixture::cancel_with_probe(cold.clone(), |request| async move {
            postgres_terminal_corruption(cold.as_ref(), &pool, request).await;
        })
        .await;
    })
    .await
    .unwrap();
}
#[cfg(feature = "postgres")]
async fn postgres_corruption(
    store: &dyn Store,
    pool: &sqlx::Pool<sqlx::Postgres>,
    session: &SessionId,
    run: &crabber::core::RunId,
) {
    let (id,original):(String,String)=sqlx::query_as("SELECT id,data_json FROM tool_continuations WHERE session_id=$1 AND run_id=$2 ORDER BY generation DESC LIMIT 1").bind(&session.0).bind(&run.0).fetch_one(pool).await.unwrap();
    let value: Value = serde_json::from_str(&original).unwrap();
    let operation: ControlId = serde_json::from_value(value["batch"]["id"].clone()).unwrap();
    let receipt = store
        .lookup_tool_continuation(session, &operation)
        .await
        .unwrap()
        .unwrap();
    let claim: crabber::session::ToolContinuationClaim =
        serde_json::from_value(value["claim"].clone()).unwrap();
    let claimed = store
        .lookup_tool_continuation(session, &claim.id)
        .await
        .unwrap()
        .unwrap();
    for case in 0..3 {
        let mut changed = value.clone();
        if case == 0 {
            changed.as_object_mut().unwrap().remove("handoff");
        } else {
            changed["handoff"]["predecessor"]["operation_id"] = if case == 1 {
                json!(ControlId::new())
            } else {
                json!("x".repeat(1024 * 1024))
            };
        }
        sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
            .bind(&id)
            .bind(serde_json::to_string(&changed).unwrap())
            .execute(pool)
            .await
            .unwrap();
        let bounded = store
            .public_control_header(crabber::session::PublicHeaderRequest {
                session_id: session.clone(),
                run_id: Some(run.clone()),
                expected_revision: store.session_revision(session).await.unwrap(),
                max_bytes: 65536,
                max_proof_bytes: 65536,
                max_interactions: 64,
            })
            .await;
        let approval = store.approval_snapshot(session, run).await;
        let tool = store.tool_continuation_snapshot(session, run).await;
        let replay = store.lookup_tool_continuation(session, &operation).await;
        let claim_replay = store.claim_tool_continuation(claim.clone()).await;
        // Restore fixture integrity before assertions or a later global test.
        sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
            .bind(&id)
            .bind(&original)
            .execute(pool)
            .await
            .unwrap();
        assert!(matches!(
            approval,
            Err(crabber::session::ApprovalError::UnknownStoreOutcome)
        ));
        assert!(matches!(
            tool,
            Err(crabber::session::ToolContinuationError::UnknownStoreOutcome)
        ));
        assert_eq!(replay.unwrap(), Some(receipt.clone()));
        assert!(
            matches!(claim_replay,Ok(crabber::session::ToolContinuationClaimOutcome::Replayed(actual)) if actual==claimed)
        );
        if case == 2 {
            assert!(matches!(
                bounded,
                Ok(crabber::session::PublicHeaderOutcome::Limited(
                    crabber::session::PublicSessionLimit::ProofBytes
                ))
            ));
        }
    }
    header(store, session, run).await;
}

#[cfg(feature = "sqlite")]
async fn sqlite_corruption(
    store: &dyn Store,
    pool: &sqlx::Pool<sqlx::Sqlite>,
    session: &SessionId,
    run: &crabber::core::RunId,
) {
    let (id,original):(String,String)=sqlx::query_as("SELECT id,data_json FROM tool_continuations WHERE session_id=$1 AND run_id=$2 ORDER BY generation DESC LIMIT 1").bind(&session.0).bind(&run.0).fetch_one(pool).await.unwrap();
    let value: Value = serde_json::from_str(&original).unwrap();
    let operation: ControlId = serde_json::from_value(value["batch"]["id"].clone()).unwrap();
    let receipt = store
        .lookup_tool_continuation(session, &operation)
        .await
        .unwrap()
        .unwrap();
    let claim: crabber::session::ToolContinuationClaim =
        serde_json::from_value(value["claim"].clone()).unwrap();
    let claimed = store
        .lookup_tool_continuation(session, &claim.id)
        .await
        .unwrap()
        .unwrap();
    for case in 0..3 {
        let mut changed = value.clone();
        if case == 0 {
            changed.as_object_mut().unwrap().remove("handoff");
        } else {
            changed["handoff"]["predecessor"]["operation_id"] = if case == 1 {
                json!(ControlId::new())
            } else {
                json!("x".repeat(1024 * 1024))
            };
        }
        sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
            .bind(&id)
            .bind(serde_json::to_string(&changed).unwrap())
            .execute(pool)
            .await
            .unwrap();
        let bounded = store
            .public_control_header(crabber::session::PublicHeaderRequest {
                session_id: session.clone(),
                run_id: Some(run.clone()),
                expected_revision: store.session_revision(session).await.unwrap(),
                max_bytes: 65536,
                max_proof_bytes: 65536,
                max_interactions: 64,
            })
            .await;
        let approval = store.approval_snapshot(session, run).await;
        let tool = store.tool_continuation_snapshot(session, run).await;
        let replay = store.lookup_tool_continuation(session, &operation).await;
        let claim_replay = store.claim_tool_continuation(claim.clone()).await;
        // Restore fixture integrity before assertions or a later global test.
        sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
            .bind(&id)
            .bind(&original)
            .execute(pool)
            .await
            .unwrap();
        assert!(matches!(
            approval,
            Err(crabber::session::ApprovalError::UnknownStoreOutcome)
        ));
        assert!(matches!(
            tool,
            Err(crabber::session::ToolContinuationError::UnknownStoreOutcome)
        ));
        assert_eq!(replay.unwrap(), Some(receipt.clone()));
        assert!(
            matches!(claim_replay,Ok(crabber::session::ToolContinuationClaimOutcome::Replayed(actual)) if actual==claimed)
        );
        if case == 2 {
            assert!(matches!(
                bounded,
                Ok(crabber::session::PublicHeaderOutcome::Limited(
                    crabber::session::PublicSessionLimit::ProofBytes
                ))
            ));
        }
    }
    header(store, session, run).await;
}

#[cfg(feature = "postgres")]
async fn postgres_terminal_corruption(
    store: &dyn Store,
    pool: &sqlx::Pool<sqlx::Postgres>,
    request: crabber::session::RunControlRequest,
) {
    let run = &request.target.run_id;
    let (phase, original):(String,String) = sqlx::query_as("SELECT id,outcome_json FROM tool_phases WHERE run_id=$1 AND state='Completed' ORDER BY generation DESC LIMIT 1").bind(&run.0).fetch_one(pool).await.unwrap();
    let mut changed: Value = serde_json::from_str(&original).unwrap();
    let output = ToolOutput::from_json(&json!("forged protected output")).unwrap();
    changed["output"] = serde_json::to_value(&output).unwrap();
    let (call, original_call): (String, sqlx::types::Json<Value>) = sqlx::query_as(
        "SELECT id,data FROM tool_calls WHERE run_id=$1 AND status='completed' ORDER BY id LIMIT 1",
    )
    .bind(&run.0)
    .fetch_one(pool)
    .await
    .unwrap();
    let mut changed_call: Value = original_call.0.clone();
    changed_call["result"]["content"] = serde_json::to_value(output.to_content_blocks()).unwrap();
    sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
        .bind(&phase)
        .bind(serde_json::to_string(&changed).unwrap())
        .execute(pool)
        .await
        .unwrap();
    let changed_record: crabber::core::ToolCallRecord =
        serde_json::from_value(changed_call.clone()).unwrap();
    let changed_json = serde_json::to_string(&changed_record).unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_record=$3,snapshot_bytes=$4 WHERE id=$1")
        .bind(&call)
        .bind(sqlx::types::Json(changed_call))
        .bind(&changed_json)
        .bind(i64::try_from(changed_json.len()).unwrap())
        .execute(pool)
        .await
        .unwrap();
    let revision = store
        .session_revision(&request.target.session_id)
        .await
        .unwrap();
    let result = store.control_run(request.clone()).await;
    let receipt = store
        .lookup_run_control(&request.target.session_id, &request.id)
        .await;
    let after = store
        .session_revision(&request.target.session_id)
        .await
        .unwrap();
    // Restore all private rows before assertions, including on a failure.
    sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
        .bind(&phase)
        .bind(&original)
        .execute(pool)
        .await
        .unwrap();
    let original_record: crabber::core::ToolCallRecord =
        serde_json::from_value(original_call.0.clone()).unwrap();
    let original_json = serde_json::to_string(&original_record).unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_record=$3,snapshot_bytes=$4 WHERE id=$1")
        .bind(&call)
        .bind(original_call)
        .bind(&original_json)
        .bind(i64::try_from(original_json.len()).unwrap())
        .execute(pool)
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "coherent private output replacement became terminal authority"
    );
    assert_eq!(receipt.unwrap(), None);
    assert_eq!(after, revision);
}

#[cfg(feature = "sqlite")]
async fn sqlite_terminal_corruption(
    store: &dyn Store,
    pool: &sqlx::Pool<sqlx::Sqlite>,
    request: crabber::session::RunControlRequest,
) {
    let run = &request.target.run_id;
    let (phase, original):(String,String) = sqlx::query_as("SELECT id,outcome_json FROM tool_phases WHERE run_id=$1 AND state='Completed' ORDER BY generation DESC LIMIT 1").bind(&run.0).fetch_one(pool).await.unwrap();
    let mut changed: Value = serde_json::from_str(&original).unwrap();
    let output = ToolOutput::from_json(&json!("forged protected output")).unwrap();
    changed["output"] = serde_json::to_value(&output).unwrap();
    let (call, original_call): (String, String) = sqlx::query_as(
        "SELECT id,data FROM tool_calls WHERE run_id=$1 AND status='completed' ORDER BY id LIMIT 1",
    )
    .bind(&run.0)
    .fetch_one(pool)
    .await
    .unwrap();
    let mut changed_call: Value = serde_json::from_str(&original_call).unwrap();
    changed_call["result"]["content"] = serde_json::to_value(output.to_content_blocks()).unwrap();
    sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
        .bind(&phase)
        .bind(serde_json::to_string(&changed).unwrap())
        .execute(pool)
        .await
        .unwrap();
    let changed_record: crabber::core::ToolCallRecord =
        serde_json::from_value(changed_call).unwrap();
    let changed_json = serde_json::to_string(&changed_record).unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_bytes=$3 WHERE id=$1")
        .bind(&call)
        .bind(&changed_json)
        .bind(i64::try_from(changed_json.len()).unwrap())
        .execute(pool)
        .await
        .unwrap();
    let revision = store
        .session_revision(&request.target.session_id)
        .await
        .unwrap();
    let result = store.control_run(request.clone()).await;
    let receipt = store
        .lookup_run_control(&request.target.session_id, &request.id)
        .await;
    let after = store
        .session_revision(&request.target.session_id)
        .await
        .unwrap();
    // Restore all private rows before assertions, including on a failure.
    sqlx::query("UPDATE tool_phases SET outcome_json=$2 WHERE id=$1")
        .bind(&phase)
        .bind(&original)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_bytes=$3 WHERE id=$1")
        .bind(&call)
        .bind(&original_call)
        .bind(i64::try_from(original_call.len()).unwrap())
        .execute(pool)
        .await
        .unwrap();
    assert!(
        result.is_err(),
        "coherent private output replacement became terminal authority"
    );
    assert_eq!(receipt.unwrap(), None);
    assert_eq!(after, revision);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_ordinary_terminal_output_has_immutable_provenance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ordinary-terminal.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crabber::session::SqliteStore::connect(&path).await.unwrap());
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    for approval in [true, false] {
        let probe_store = store.clone();
        let pool = &pool;
        fixture::cancel_completed_with_probe(store.clone(), approval, |request| async move {
            sqlite_terminal_corruption(probe_store.as_ref(), pool, request).await;
        })
        .await;
    }
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_ordinary_terminal_output_has_immutable_provenance() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        let store = Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        );
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        for approval in [true, false] {
            let probe_store = store.clone();
            let pool = &pool;
            fixture::cancel_completed_with_probe(store.clone(), approval, |request| async move {
                postgres_terminal_corruption(probe_store.as_ref(), pool, request).await;
            })
            .await;
        }
    })
    .await
    .unwrap();
}
