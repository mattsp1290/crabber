//! A cached genuine worker cannot use coherently restored private projections.
use crate::Store;
use std::sync::Arc;
#[tokio::test]
async fn erased_source_tail_and_coherent_private_restore_cannot_issue_fresh_begin() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();

    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    let (f,(attempt,phases,continuation,execution))=crate::session_owner::preparation_contract::claim_history::fixture_checkpoint(&store,&clock,|run,target,continuation|{let store=&store;async move {
        let attempt:(String,String,String,i64)=sqlx::query_as("SELECT owner,claim_token,run_claim_token,lease_until FROM execution_attempts WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&target.id.0).fetch_one(&store.pool).await.unwrap();
        let phases:Vec<(String,String)>=sqlx::query_as("SELECT id,claim_token FROM tool_phases WHERE run_id=$1").bind(&run.id.0).fetch_all(&store.pool).await.unwrap();
        let data:String=sqlx::query_scalar("SELECT data_json FROM tool_continuations WHERE id=$1").bind(&continuation.0).fetch_one(&store.pool).await.unwrap();
        let execution=store.execution(crabber_core::RunFence{run_id:run.id.clone(),claim_token:run.claim_token.clone()}).await.unwrap();
        (attempt,phases,(continuation,data),execution)
    }}).await;
    let run = &f.before_tail;
    let grant = f.first_grant;
    let crate::ToolContinuationOutcome::ReclaimedUnstarted {
        attempt: source_attempt,
        ..
    } = &f.first.outcome
    else {
        panic!("actual first recovery")
    };
    let target = source_attempt.target.clone();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM native_run_issuances WHERE run_id=$1 AND sequence=(SELECT MAX(sequence) FROM native_run_issuances WHERE run_id=$1)").bind(&run.id.0).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&run.session_id.0)
        .bind(&f.tail.operation_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
        .bind(&run.session_id.0)
        .bind(i64::try_from(f.tail.event_cursor.0).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE runs SET claim_token=$2,lease_until=$3,data=$4::jsonb WHERE id=$1")
        .bind(&run.id.0)
        .bind(&run.claim_token)
        .bind(crate::postgres::micros(run.lease_until))
        .bind(serde_json::to_string(run).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("UPDATE execution_attempts SET owner=$3,claim_token=$4,run_claim_token=$5,lease_until=$6 WHERE run_id=$1 AND id=$2").bind(&run.id.0).bind(&target.id.0).bind(attempt.0).bind(attempt.1).bind(attempt.2).bind(attempt.3).execute(&mut *tx).await.unwrap();
    for (id, token) in phases {
        sqlx::query("UPDATE tool_phases SET claim_token=$2 WHERE id=$1")
            .bind(id)
            .bind(token)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE tool_continuations SET data_json=$2 WHERE id=$1")
        .bind(&continuation.0.0)
        .bind(continuation.1)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    clock.set(run.updated_at);
    let revision = store.session_revision(&run.session_id).await.unwrap();
    assert_eq!(grant.fence.claim_token, run.claim_token);
    let outcome = execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: revision,
                command: crate::AttemptCommand::Begin { target },
            },
            Some(grant.authority),
        )
        .await;
    assert!(
        matches!(outcome, Err(crate::AttemptError::UnknownStoreOutcome)),
        "restored actual old authority must not issue a fresh Begin: {outcome:?}"
    );
    assert_eq!(
        store.session_revision(&run.session_id).await.unwrap(),
        revision
    );
}
