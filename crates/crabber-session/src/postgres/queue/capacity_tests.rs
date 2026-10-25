//! Actual prepared-capacity routes reject decoded controls without aliasing literals.
use super::pending;
use crate::public_session::v2::proof::Proof;

#[tokio::test]
async fn capacity_refuses_control_markers_in_every_selected_identity() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let (fixture, prepare, _) = super::super::compact_tests::prepared_values(&store).await;
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let audit: String = sqlx::query_scalar(
            "SELECT data_json FROM future_queue_audit WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&fixture.session.0)
        .bind(&prepare.id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let projection: String = sqlx::query_scalar(
            "SELECT data_json FROM future_queue WHERE session_id=$1 AND queue_id=$2",
        )
        .bind(&fixture.session.0)
        .bind(&fixture.queue.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let mut refused = Vec::new();
        for marker in ["\u{0}", "\u{1}"] {
            for path in [
                "/request/id",
                "/request/session_id",
                "/receipt/operation_id",
                "/receipt/session_id",
                "/receipt/summary/queue_id",
            ] {
                let mut changed: serde_json::Value = serde_json::from_str(&audit).unwrap();
                *changed.pointer_mut(path).unwrap() = serde_json::Value::String(marker.into());
                write(&mut tx, &fixture, Some(&prepare.id), &changed.to_string()).await;
                let result = pending(&mut tx, &fixture.session, &mut Proof::bounded(128)).await;
                write(&mut tx, &fixture, Some(&prepare.id), &audit).await;
                refused.push(result.is_err());
            }
            for path in [
                "/prepare_operation",
                "/prepared/operation",
                "/prepared/source_digest",
            ] {
                let mut changed: serde_json::Value = serde_json::from_str(&projection).unwrap();
                *changed.pointer_mut(path).unwrap() = serde_json::Value::String(marker.into());
                write(&mut tx, &fixture, None, &changed.to_string()).await;
                let result = pending(&mut tx, &fixture.session, &mut Proof::bounded(128)).await;
                write(&mut tx, &fixture, None, &projection).await;
                refused.push(result.is_err());
            }
        }
        let restored = pending(&mut tx, &fixture.session, &mut Proof::bounded(128)).await;
        tx.rollback().await.unwrap();
        assert_eq!(refused.len(), 16);
        assert!(refused.into_iter().all(|refused| refused));
        assert!(restored.is_ok());
    })
    .await
    .unwrap();
}

async fn write(
    tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
    fixture: &crate::queue_contract::Fixture,
    operation: Option<&crabber_core::ControlId>,
    data: &str,
) {
    let (query, key) = match operation {
        Some(operation) => (
            "UPDATE future_queue_audit SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
            &operation.0,
        ),
        None => (
            "UPDATE future_queue SET data_json=$3 WHERE session_id=$1 AND queue_id=$2",
            &fixture.queue.0,
        ),
    };
    sqlx::query(query)
        .bind(&fixture.session.0)
        .bind(key)
        .bind(data)
        .execute(&mut **tx)
        .await
        .unwrap();
}

#[tokio::test]
async fn actual_capacity_normalizer_preserves_every_backslash_parity() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        // Exercise the exact SQL expression used for the real projection, including
        // PostgreSQL's pattern and replacement escaping, rather than a Rust analogue.
        let expression = super::CAPACITY
            .split_once("regexp_replace(q.data_json,")
            .unwrap()
            .1
            .split_once("::json AS document")
            .unwrap()
            .0;
        let query = format!("SELECT regexp_replace($1,{expression}");
        for count in 0..=8 {
            let slashes = "\\".repeat(count);
            let key = format!("key{slashes}\u{0}");
            let value = format!("{slashes}\u{0}\u{0}{slashes}u0000{slashes}u0001\u{1}");
            let original = serde_json::json!({key.clone():value.clone()}).to_string();
            let expected = serde_json::json!({key.replace('\u{0}', "\u{1}"):
            value.replace('\u{0}', "\u{1}")});
            assert_normalized(&store, &query, &original, &expected).await;
        }
        let original = r#"{"route":"\u005cu0000\u005cu0001","nul":"\u0000"}"#;
        assert_normalized(
            &store,
            &query,
            original,
            &serde_json::json!({"route":r"\u0000\u0001","nul":"\u{1}"}),
        )
        .await;
    })
    .await
    .unwrap();
}

async fn assert_normalized(
    store: &crate::PostgresStore,
    capacity_query: &str,
    original: &str,
    expected: &serde_json::Value,
) {
    let normalized: String = sqlx::query_scalar(capacity_query)
        .bind(original)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(normalized.len(), original.len());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&normalized).unwrap(),
        *expected
    );
    let receipt_query = format!(
        "SELECT {}",
        super::super::public::RECEIPT.replace("a.data_json", "$1")
    );
    let document = format!(r#"{{"ignored":{{"nul\u0000":"private\u0000"}},"receipt":{original}}}"#);
    let projected: String = sqlx::query_scalar(&receipt_query)
        .bind(document)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(projected.len(), original.len());
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&projected).unwrap(),
        *expected
    );
}
