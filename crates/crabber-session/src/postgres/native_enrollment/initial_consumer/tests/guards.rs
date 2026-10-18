//! Real immutable guards, exact selected-set budget and deliberate projection-loss negatives.
use super::{Proof, SourceKey};
use crate::native_enrollment::initial_consumer as domain;
use crabber_core::SessionId;
const TABLES: [&str; 10] = [
    "initial_consumer_origins",
    "initial_consumer_cohorts",
    "initial_consumer_assignments",
    "initial_consumer_calls",
    "initial_consumer_results",
    "initial_consumer_roster",
    "initial_consumer_reverse",
    "initial_consumer_publications",
    "initial_consumer_heads",
    "initial_consumer_closes",
];
pub(super) async fn verify(store: &crate::PostgresStore, session: &SessionId, key: &SourceKey) {
    let raw: String = sqlx::query_scalar(
        "SELECT data_json FROM initial_consumer_origins WHERE session_id=$1 AND source_key=$2",
    )
    .bind(&session.0)
    .bind(key.identity.as_str())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let origin: domain::Origin = serde_json::from_str(&raw).unwrap();
    let expected = origin.correlation();
    let mut tx = store.session_transaction(session).await.unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let measured = super::super::storage::load(&mut tx, expected, &mut proof)
        .await
        .unwrap();
    drop(measured);
    let used = proof.used();
    assert!(used > 0);
    let exact = super::super::storage::load(&mut tx, expected, &mut Proof::bounded(used)).await;
    let one_less =
        super::super::storage::load(&mut tx, expected, &mut Proof::bounded(used - 1)).await;
    // No budget changes/reset in production: this independently repeats a read-only fixture selection.
    let budget_ok = exact.is_ok()
        && matches!(
            one_less,
            Err(crate::public_session::v2::proof::Failure::Limited(_))
        );
    let mut failed_guards = Vec::new();
    sqlx::query("SAVEPOINT probe")
        .execute(&mut *tx)
        .await
        .unwrap();
    for table in TABLES {
        let changed = sqlx::query(&format!(
            "UPDATE {table} SET data_json='{{}}' WHERE session_id=$1"
        ))
        .bind(&session.0)
        .execute(&mut *tx)
        .await;
        sqlx::query("ROLLBACK TO SAVEPOINT probe")
            .execute(&mut *tx)
            .await
            .unwrap();
        if !guarded(&changed) {
            failed_guards.push((table, "update"));
        }
        let erased = sqlx::query(&format!("DELETE FROM {table} WHERE session_id=$1"))
            .bind(&session.0)
            .execute(&mut *tx)
            .await;
        sqlx::query("ROLLBACK TO SAVEPOINT probe")
            .execute(&mut *tx)
            .await
            .unwrap();
        if !guarded(&erased) {
            failed_guards.push((table, "delete"));
        }
    }

    // Deliberate rollback-only corruption bypass: retain exact independent cohort,
    // erase every operational projection. The real reader must never see absence.
    for table in TABLES
        .into_iter()
        .filter(|table| *table != "initial_consumer_cohorts")
    {
        sqlx::query(&format!("DROP TRIGGER {table}_guard ON {table}"))
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(&format!("DELETE FROM {table} WHERE session_id=$1"))
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    let mut proof = Proof::bounded(32 << 20);
    let selected = super::super::storage::load(&mut tx, expected, &mut proof).await;
    let refused = match selected {
        Ok(selected) => match selected.selection(&mut proof) {
            Ok(view) => view.verify(expected, &mut proof).is_err(),
            Err(_) => true,
        },
        Err(_) => true,
    };
    tx.rollback().await.unwrap(); // Restore exact original rows/guards BEFORE diagnostics assert.
    assert!(
        failed_guards.is_empty(),
        "actual guard failures: {failed_guards:?}"
    );
    assert!(budget_ok, "complete selected set exact N/N-1 boundary");
    assert!(refused, "independent cohort cannot become legacy/absent");
}

fn guarded<T>(result: &Result<T, sqlx::Error>) -> bool {
    result
        .as_ref()
        .err()
        .and_then(|error| error.as_database_error())
        .is_some_and(|error| error.code().as_deref() == Some("P0001"))
}
