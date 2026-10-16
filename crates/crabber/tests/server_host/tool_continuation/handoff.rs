//! Cross-domain native handoff through public workers and cold adapters.
use super::*;
#[path = "handoff/journey.rs"]
mod fixture;
use fixture::{header, journey};
#[tokio::test]
async fn memory_bidirectional_approval_tool_handoff() {
    journey(Arc::new(MemoryStore::new())).await;
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
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_bidirectional_approval_tool_handoff() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        return;
    };
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
