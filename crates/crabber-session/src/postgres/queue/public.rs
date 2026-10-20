//! FIFO public queue projection; never fetches `QueueInput` or preparation capsules.
//! Budgets bound client transfer/materialization. Database JSON inspection work
//! is not a transfer of the enclosing private audit input.
use super::reads;
use crate::postgres::transaction::Transaction;
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, unknown},
};
use crate::{PublicSessionLimitV2, QueueReceipt, QueueState, QueueSummary};
use crabber_core::{QueueId, SessionId};
use sqlx::{Postgres, Row};
pub(in crate::postgres) async fn pending(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<Vec<QueueSummary>, Failure> {
    super::protocol::whole_scope(tx, session, proof)
        .await
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    let mismatch: bool = sqlx::query_scalar(SCOPE)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if mismatch {
        return Err(unknown("queue membership"));
    }
    let mut result = Vec::new();
    let mut after = 0_i64;
    loop {
        let rows = sqlx::query(IDS)
            .bind(&session.0)
            .bind(after)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            proof.charge(
                usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?)
                    .map_err(unknown)?,
            )?;
        }
        for row in rows {
            let id = row
                .try_get::<Option<String>, _>("id")
                .map_err(unknown)?
                .ok_or_else(|| unknown("queue routing"))?;
            let sequence: i64 = row.try_get("sequence").map_err(unknown)?;
            if sequence <= after {
                return Err(unknown("queue order"));
            }
            let bytes: i64 = sqlx::query_scalar(&COST.replace("{receipt}", RECEIPT))
                .bind(&session.0)
                .bind(&id)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?;
            let allowance = proof.budget().remaining();
            proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
            let query = reads::SUMMARY.replacen("SELECT ",&format!("SELECT CASE WHEN octet_length({RECEIPT})<=16384 AND octet_length({RECEIPT})<=$3 THEN {RECEIPT} END AS immutable_receipt, "),1);
            let row = sqlx::query(&query)
                .bind(&session.0)
                .bind(&id)
                .bind(i64::try_from(allowance).map_err(unknown)?)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?;
            let summary = reads::summary(&row, session).map_err(unknown)?;
            let protected = super::protocol::projected(
                tx,
                &crate::queue::source::Scope {
                    session: session.clone(),
                    queue: QueueId(id.clone()),
                },
                proof,
            )
            .await
            .map_err(crate::public_session::v2::proof::ReadError::public)?;
            let source: QueueReceipt = projected_receipt(
                &row.try_get::<Option<String>, _>("immutable_receipt")
                    .map_err(unknown)?
                    .ok_or_else(|| unknown("queue immutable source"))?,
            )
            .map_err(unknown)?;
            if source.summary != summary
                || source.operation_id.0
                    != row
                        .try_get::<Option<String>, _>("latest_operation")
                        .map_err(unknown)?
                        .ok_or_else(|| unknown("queue source routing"))?
                || source.summary.queue_id != QueueId(id)
                || i64::try_from(source.summary.sequence).ok() != Some(sequence)
            {
                return Err(unknown("queue immutable summary"));
            }
            if let Some(protected) = protected {
                protected.verify_receipt(&source).map_err(unknown)?;
            }
            if matches!(summary.state, QueueState::Queued | QueueState::Claimed) {
                if result.len() >= usize::from(limit) {
                    return Err(Failure::Limited(PublicSessionLimitV2::QueueEntries));
                }
                safe.measure(&summary)?;
                result.push(summary);
            }
            after = sequence;
        }
    }
    Ok(result)
}
const IDS: &str = r"SELECT CAST(octet_length(q.queue_id) AS BIGINT) AS bytes,CASE WHEN octet_length(q.queue_id)<=128 THEN q.queue_id END AS id,q.sequence FROM future_queue q WHERE q.session_id=$1 AND q.sequence>$2 ORDER BY q.sequence LIMIT 32";
const SCOPE: &str = r"SELECT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=$1 AND NOT EXISTS(SELECT 1 FROM future_queue q WHERE q.session_id=a.session_id AND q.queue_id=a.queue_id) UNION ALL SELECT 1 FROM future_queue q WHERE q.session_id=$1 AND NOT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=q.session_id AND a.queue_id=q.queue_id))";
const COST: &str = r"SELECT CAST(octet_length(q.summary_json)+COALESCE(octet_length(r.data_json),0)+COALESCE(octet_length(CAST(ar.data AS TEXT)),0)+octet_length(q.queue_id)+octet_length(q.state)+COALESCE(octet_length(a.operation_id),0)+COALESCE(octet_length({receipt}),0) AS BIGINT) AS bytes FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.queue_id=$2";

// PostgreSQL inspects ignored private keys/values during JSON extraction. Only
// genuine JSON NUL escapes change to the equally long U+0001 escape; paired
// literal backslashes remain exact. Actual decoded U+0001 in selected metadata
// still refuses below before it can authenticate an original Source.
pub(super) const RECEIPT: &str = r"CAST(regexp_replace(a.data_json,
 $rx$(?<!\\)((?:\\\\)*)\\u0000$rx$, $replacement$\1\\u0001$replacement$, 'g')::json->'receipt' AS TEXT)";
pub(super) fn projected_receipt(text: &str) -> Result<QueueReceipt, Failure> {
    let value: serde_json::Value = serde_json::from_str(text).map_err(unknown)?;
    if marked(&value) {
        return Err(unknown("queue receipt projection marker"));
    }
    serde_json::from_value(value).map_err(unknown)
}
fn marked(value: &serde_json::Value) -> bool {
    fn marker(text: &str) -> bool {
        text.contains('\u{1}')
    }
    match value {
        serde_json::Value::String(text) => marker(text),
        serde_json::Value::Array(values) => values.iter().any(marked),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| marker(key) || marked(value)),
        _ => false,
    }
}
#[cfg(test)]
#[test]
fn aggregate_queue_projection_marker_cannot_alias_original_receipt() {
    for depth in 0..5 {
        let literal = format!(
            "original{}u0000{}u0001",
            "\\".repeat(depth + 1),
            "\\".repeat(depth + 1)
        );
        assert!(!marked(
            &serde_json::json!({"nested":[{literal.clone():literal}]})
        ));
    }
    assert!(marked(
        &serde_json::json!({"nested":[{"actual\u{1}":"value"}]})
    ));
    assert!(!marked(&serde_json::json!({"literal":"safe\\u0000"})));
}

#[cfg(test)]
#[tokio::test]
async fn aggregate_queue_projection_rejects_changed_literal_source_identity() {
    use crate::{PublicSessionError, Store};
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let mut f = crate::queue_contract::fixture(&store).await;
    let original = r"original\u0000\u0001";
    let forged = r"original\u0001\u0001";
    f.enqueue.id = crabber_core::ControlId::from(original);
    let outcome = store.mutate_queue(f.enqueue).await.unwrap();
    let request = crate::public_session::v2::tests::request(&store, &f.session, None).await;
    let crate::PublicHeaderOutcomeV2::Header(header) = store
        .public_control_header_v2(request.clone())
        .await
        .unwrap()
    else {
        panic!("valid literal identity must retain its actual source");
    };
    assert_eq!(header.session.session_id, f.session);
    assert_eq!(header.queue, vec![outcome.receipt().summary.clone()]);
    // Keep the authoritative serialized original body unchanged while changing
    // both mutable routing indexes and the copied receipt coherently. A plain
    // replacement projection would now make that different original ID match.
    let mut tx =
        crate::transaction::begin_scoped(&store.pool, crate::transaction::StoreScope::Queue)
            .await
            .unwrap();
    sqlx::query(
        "UPDATE future_queue_audit SET operation_id=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&f.session.0)
    .bind(original)
    .bind(forged)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE operation_receipts SET operation_id=$3,data_json=CAST(jsonb_set(data_json::jsonb,'{operation_id}',to_jsonb(CAST($3 AS TEXT))) AS TEXT) WHERE session_id=$1 AND operation_id=$2").bind(&f.session.0).bind(original).bind(forged).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        store.public_control_header_v2(request).await.unwrap_err(),
        PublicSessionError::UnknownStoreOutcome
    );
}

#[cfg(test)]
#[tokio::test]
async fn actual_receipt_projection_rejects_decoded_markers_in_selected_metadata() {
    use crate::Store;
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let fixture = crate::queue_contract::fixture(&store).await;
    let outcome = store.mutate_queue(fixture.enqueue).await.unwrap();
    let receipt = serde_json::to_value(outcome.receipt()).unwrap();
    let query = format!("SELECT {}", RECEIPT.replace("a.data_json", "$1"));
    for marker in ["\u{0}", "\u{1}"] {
        for path in [
            "/session_id",
            "/operation_id",
            "/summary/session_id",
            "/summary/queue_id",
        ] {
            let mut changed = receipt.clone();
            *changed.pointer_mut(path).unwrap() = serde_json::Value::String(marker.into());
            let document = serde_json::json!({"receipt":changed,
                "ignored":{"nul\u{0}":"private\u{0}"}})
            .to_string();
            let projected: String = sqlx::query_scalar(&query)
                .bind(document)
                .fetch_one(&store.pool)
                .await
                .unwrap();
            assert!(projected_receipt(&projected).is_err());
        }
    }
}
