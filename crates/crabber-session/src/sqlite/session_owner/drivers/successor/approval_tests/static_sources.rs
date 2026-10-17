//! Missing native origins and malformed witness readers are distinct failures.
use super::*;
pub(super) async fn original_sources(store: &crate::SqliteStore, f: &Fixture) {
    for change in 0..5 {
        let mut tx = store.begin_write().await.unwrap();
        let id = crate::approval::derived_id("suspend-attempt", &f.batch.id);
        let native = crate::sqlite::attempt::host_receipt_with_proof(
            &mut tx,
            &f.receipt.session_id,
            &id,
            &mut Proof::bounded(32 << 20),
        )
        .await
        .unwrap()
        .unwrap();
        let mut frame = crate::sqlite::read_proof::event(
            &mut tx,
            &f.receipt.session_id,
            native.event_cursor,
            true,
            &mut Proof::bounded(32 << 20),
        )
        .await
        .unwrap();
        match change {
            0 => {
                sqlx::query(
                    "DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2",
                )
                .bind(&f.receipt.session_id.0)
                .bind(&id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            1 => {
                let replacement: String = sqlx::query_scalar("SELECT data_json FROM operation_receipts WHERE session_id=$1 AND operation_id=(SELECT operation_id FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2 AND kind='attempt' ORDER BY event_seq LIMIT 1)")
                .bind(&f.receipt.session_id.0).bind(&f.receipt.run_id.0).fetch_one(&mut *tx).await.unwrap();
                sqlx::query("UPDATE operation_receipts SET data_json=$3 WHERE session_id=$1 AND operation_id=$2")
                    .bind(&f.receipt.session_id.0).bind(&id.0).bind(replacement).execute(&mut *tx).await.unwrap();
            }
            2 => {
                frame.payload["usage"]["input_tokens"] = serde_json::json!(999);
                sqlx::query("UPDATE events SET data=$3 WHERE session_id=$1 AND seq=$2")
                    .bind(&f.receipt.session_id.0)
                    .bind(i64::try_from(frame.cursor.unwrap().0).unwrap())
                    .bind(serde_json::to_string(&frame).unwrap())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            4 => {
                sqlx::query("DELETE FROM preparation_admission_witnesses WHERE run_id=$1")
                    .bind(&f.receipt.run_id.0)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            _ => {
                sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
                    .bind(&f.receipt.session_id.0)
                    .bind(i64::try_from(frame.cursor.unwrap().0).unwrap())
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
        }
        let result = Box::pin(source::original(
            &mut tx,
            &f.request,
            &mut Proof::bounded(32 << 20),
            None,
        ))
        .await;
        if change == 4 {
            assert!(matches!(result, Err(Error::UnknownStoreOutcome)));
        } else {
            assert!(result.is_err());
        }
        tx.rollback().await.unwrap();
    }
}
pub(super) async fn malformed_witness(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
    run: &crabber_core::Run,
    f: &Fixture,
    begun: &crate::AttemptReceipt,
    clock: &ManualClock,
) {
    let original: String =
        sqlx::query_scalar("SELECT data_json FROM native_parent_witnesses WHERE event_seq=$1")
            .bind(i64::try_from(begun.event_cursor.0).unwrap())
            .fetch_one(&mut **tx)
            .await
            .unwrap();
    for field in ["attempt", "id", "generation"] {
        let mut body: serde_json::Value = serde_json::from_str(&original).unwrap();
        if field == "attempt" {
            body["source"].as_object_mut().unwrap().remove(field);
        } else {
            body["source"]["attempt"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        // The carrier TEXT has no immutable-update trigger in this schema.
        sqlx::query("UPDATE native_parent_witnesses SET data_json=$2 WHERE event_seq=$1")
            .bind(i64::try_from(begun.event_cursor.0).unwrap())
            .bind(body.to_string())
            .execute(&mut **tx)
            .await
            .unwrap();
        let hidden: bool = sqlx::query_scalar(OLD_UNSTARTED)
            .bind(&run.session_id.0)
            .bind(&run.id.0)
            .bind(&f.batch.next_attempt.id.0)
            .bind(i64::try_from(f.batch.next_attempt.generation.get()).unwrap())
            .fetch_one(&mut **tx)
            .await
            .unwrap();
        assert!(
            !hidden,
            "old nullable target predicate silently ignored {field}"
        );
        assert!(
            approval::check(
                tx,
                run,
                &f.batch.continuation,
                &f.batch.next_attempt,
                clock.now(),
                &mut Proof::bounded(32 << 20)
            )
            .await
            .is_err()
        );
    }
    sqlx::query("UPDATE native_parent_witnesses SET data_json=$2 WHERE event_seq=$1")
        .bind(i64::try_from(begun.event_cursor.0).unwrap())
        .bind(original)
        .execute(&mut **tx)
        .await
        .unwrap();
}
const OLD_UNSTARTED: &str = "SELECT EXISTS(SELECT 1 FROM native_parent_witnesses WHERE session_id=$1 AND run_id=$2 AND CASE WHEN length(CAST(data_json AS BLOB))>8192 THEN 1 ELSE json_extract(data_json,'$.source.session_id') IS NOT $1 OR json_extract(data_json,'$.source.run_id') IS NOT $2 OR (json_extract(data_json,'$.source.attempt.id')=$3 AND json_extract(data_json,'$.source.attempt.generation')=$4) END)";
