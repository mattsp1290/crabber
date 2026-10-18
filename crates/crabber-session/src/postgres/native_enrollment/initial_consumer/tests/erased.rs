//! Native event and Part loss with ALL protected consumer/birth originals left intact.
use super::*;
pub(super) async fn verify(
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    key: &SourceKey,
) {
    let raw: String = sqlx::query_scalar(
        "SELECT data_json FROM initial_consumer_origins WHERE session_id=$1 AND source_key=$2",
    )
    .bind(&session.0)
    .bind(key.identity.as_str())
    .fetch_one(&store.pool)
    .await
    .unwrap();
    let origin: crate::native_enrollment::initial_consumer::Origin =
        serde_json::from_str(&raw).unwrap();
    let mut tx = store.session_transaction(session).await.unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let selected = super::super::storage::load(&mut tx, origin.correlation(), &mut proof)
        .await
        .unwrap();
    let view = selected.selection(&mut proof).unwrap();
    view.verify(origin.correlation(), &mut proof).unwrap();
    let intact = super::super::members::verify(&mut tx, &view, &mut proof).await;
    let cursor=view.results.iter().find_map(|result|match &result.original{
 crate::native_enrollment::initial_consumer::Original::Lifecycle(value) if value.kind==crate::native_enrollment::initial_consumer::InitialLifecycleKind::RunStarted=>Some(value.cursor),_=>None}).unwrap();
    sqlx::query("SAVEPOINT original_event")
        .execute(&mut *tx)
        .await
        .unwrap();
    let removed = sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
        .bind(&session.0)
        .bind(i64::try_from(cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
    // Consumer copies/head/cohort/birth/current Run remain unchanged, so pure
    // projection verification alone is deliberately still successful.
    let projection = view.verify(origin.correlation(), &mut proof);
    let rejected = super::super::members::verify(&mut tx, &view, &mut proof).await;
    sqlx::query("ROLLBACK TO SAVEPOINT original_event")
        .execute(&mut *tx)
        .await
        .unwrap();
    let part = view
        .results
        .iter()
        .find_map(|result| match &result.original {
            crate::native_enrollment::initial_consumer::Original::AssistantMessage(value) => {
                value.message.parts.first().map(|part| &part.id)
            }
            _ => None,
        })
        .unwrap();
    // The Message's cached parts and all consumer originals remain intact.
    let removed_part = sqlx::query("DELETE FROM parts WHERE id=$1")
        .bind(&part.0)
        .execute(&mut *tx)
        .await
        .unwrap()
        .rows_affected();
    let part_projection = view.verify(origin.correlation(), &mut proof);
    let rejected_part = super::super::members::verify(&mut tx, &view, &mut proof).await;
    tx.rollback().await.unwrap(); // Restore native originals BEFORE asserting outcomes.
    assert!(
        intact.is_ok(),
        "actual historical members are independently corroborated"
    );
    assert_eq!(removed, 1);
    assert!(projection.is_ok());
    assert!(
        rejected.is_err(),
        "copied event cannot substitute for erased native original"
    );
    assert_eq!(removed_part, 1);
    assert!(part_projection.is_ok());
    assert!(
        rejected_part.is_err(),
        "cached Message parts cannot substitute for erased native Part"
    );
}
