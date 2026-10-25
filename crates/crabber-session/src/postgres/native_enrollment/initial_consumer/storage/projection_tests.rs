//! Exact production scope expression, not a Rust normalizer analogue.
use super::*;
#[tokio::test]
async fn postgres_consumer_scope_preserves_private_nul_and_every_backslash_parity() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    // This is routing selection only. The normal fixture separately authenticates
    // the complete actual original and its immutable native provenance.
    let route = scope(TABLES[5]);
    let query = format!(
        "SELECT {route} FROM (SELECT $4::TEXT AS data_json,'other'::TEXT AS session_id,'other'::TEXT AS source_key,'other'::TEXT AS epoch_id) value"
    );
    let session = crabber_core::SessionId::new();
    let epoch = crabber_core::ControlId::new();
    let key = "a".repeat(64);
    for count in 0..8 {
        let private = format!("{}\0\u{1} literal \\u0000", "\\".repeat(count));
        for nested in [false, true] {
            let mut value = serde_json::json!({"correlation":{"key":{"session_id":session.0,"identity":key},"identity":{"epoch":epoch.0}},"private":private});
            let table = if nested { TABLES[0] } else { TABLES[5] };
            if nested {
                value = serde_json::json!({"origin":"opened","record":value});
            }
            let actual_query = query.replace(&route, &scope(table));
            let matches: bool = sqlx::query_scalar(&actual_query)
                .bind(&session.0)
                .bind(&key)
                .bind(&epoch.0)
                .bind(serde_json::to_string(&value).unwrap())
                .fetch_one(&store.pool)
                .await
                .unwrap();
            assert!(
                matches,
                "exact embedded route survives unrelated private values"
            );
        }
    }
    for invalid in ['\0', '\u{1}'] {
        let wrong_session = format!("{}{}", session.0, invalid);
        let wrong_epoch = format!("{}{}", epoch.0, invalid);
        let value = serde_json::json!({"correlation":{"key":{"session_id":wrong_session,"identity":key},"identity":{"epoch":wrong_epoch}},"private":"normal"});
        let matches: bool = sqlx::query_scalar(&query)
            .bind(&session.0)
            .bind(&key)
            .bind(&epoch.0)
            .bind(serde_json::to_string(&value).unwrap())
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert!(
            !matches,
            "private projection marker cannot alias a real routing ID"
        );
    }

    }).await.unwrap();
}
