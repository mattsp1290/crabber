//! Genuine original publishers authenticate all four Tool/Frontend sources.
use super::*;
use crate::successor_driver_contract::tools::fixture;
use crabber_core::{Clock, ManualClock};
use std::sync::Arc;

#[tokio::test]
async fn tool_original_sources_authenticate_all_four_compound_origins() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let native = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = native.with_clock(clock.clone());
    for frontend in [false, true] {
        for recovery in [false, true] {
            for unkeyed in [false, true] {
                let f = Box::pin(fixture(&store, &clock, frontend, recovery, unkeyed)).await;
                let mut tx = store
                    .session_transaction(f.context.session_id())
                    .await
                    .unwrap();
                let join = f.joined.observed_grant().reconciliation().unwrap().join_id;
                // Select the actual immutable completion publisher by its real
                // RootJoin, independently of latest native heads or guessed S.
                let slots: Vec<i64> = sqlx::query_scalar("SELECT sequence FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND data_json::json->'completion_source'->>'root_join_id'=$3 LIMIT 2")
                    .bind(&f.receipt.session_id.0).bind(&f.receipt.run_id.0).bind(&join.0)
                    .fetch_all(&mut *tx).await.unwrap();
                assert_eq!(slots.len(), 1);
                let request = RegisterSuccessorExecutionDriver {
                    context: f.context.clone(),
                    fence: f.native.fence.clone(),
                    claim: crate::session_owner::driver::NativeClaimLocator {
                        issuance: u64::try_from(slots[0]).unwrap(),
                        root_join_id: join,
                    },
                };
                Box::pin(crate::successor_driver_contract::tools::inspect_captured(
                    &store, &f, &request, frontend, recovery,
                ))
                .await;
                let mut proof = Proof::bounded(32 << 20);
                let selected = Box::pin(source::original(&mut tx, &request, &mut proof, None))
                    .await
                    .unwrap();
                assert_eq!(
                    selected.tool,
                    Some((f.batch.continuation.clone(), f.batch.next_attempt.clone()))
                );
                assert_eq!(selected.revision, f.receipt.revision);
                let used = proof.used();
                Box::pin(source::original(
                    &mut tx,
                    &request,
                    &mut Proof::bounded(used),
                    None,
                ))
                .await
                .unwrap();
                assert!(
                    Box::pin(source::original(
                        &mut tx,
                        &request,
                        &mut Proof::bounded(used - 1),
                        None
                    ))
                    .await
                    .is_err()
                );
                assert!(matches!(
                    Box::pin(fresh::fresh(
                        &mut tx,
                        &request,
                        None,
                        clock.now(),
                        &mut Proof::bounded(32 << 20)
                    ))
                    .await,
                    Err(Error::Unsupported)
                ));
                if frontend {
                    Box::pin(substituted_content(&mut tx, &f, &request)).await;
                }
                tx.rollback().await.unwrap();
            }
        }
    }
}

async fn substituted_content(
    tx: &mut Transaction<'_, Postgres>,
    f: &crate::successor_driver_contract::tools::Fixture,
    request: &RegisterSuccessorExecutionDriver,
) {
    let raw: String = sqlx::query_scalar(
        "SELECT data_json FROM tool_continuations WHERE session_id=$1 AND id=$2",
    )
    .bind(&f.receipt.session_id.0)
    .bind(&f.batch.continuation.id.0)
    .fetch_one(&mut **tx)
    .await
    .unwrap();
    let mut record: crate::tool_continuation::ToolContinuationRecord =
        serde_json::from_str(&raw).unwrap();
    crate::successor_driver_contract::tools::replace_frontend_result(&mut record);
    sqlx::query("UPDATE tool_continuations SET data_json=$3 WHERE session_id=$1 AND id=$2")
        .bind(&f.receipt.session_id.0)
        .bind(&f.batch.continuation.id.0)
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&mut **tx)
        .await
        .unwrap();
    assert!(
        Box::pin(source::original(
            tx,
            request,
            &mut Proof::bounded(32 << 20),
            None
        ))
        .await
        .is_err(),
        "full original external content is authenticated"
    );
}
