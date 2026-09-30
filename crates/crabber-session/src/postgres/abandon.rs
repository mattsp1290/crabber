use super::{
    PostgresStore, call_status, db, decode, insert_event, insert_message, json, load_run, save_run,
};
use crate::StoreError;
use crate::abandonment::{AbandonEvidence, interrupted_tool, terminal_event};
use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};
use crabber_core::{EventCursor, EventRecord, Run, RunStatus, ToolCallRecord, ToolCallStatus};
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn abandon(
    store: &PostgresStore,
    request: AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let mut tx = store.pool.begin().await.map_err(db)?;
    let (outcome, changed) = match settle(store, &mut tx, request).await {
        Ok(outcome) => outcome,
        Err(error) => {
            // Explicitly release all locks before returning even on semantic
            // denial or a failed artifact write. Never rely on queued rollback.
            tx.rollback().await.map_err(db)?;
            return Err(error);
        }
    };
    if !changed {
        tx.commit().await.map_err(db)?;
        return Ok(outcome);
    }
    #[cfg(test)]
    if store
        .abandon_fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 1
    {
        tx.rollback().await.map_err(db)?;
        return Err(StoreError::Validation("injected precommit failure".into()).into());
    }
    // A commit error is an unknown outcome, never a successful settlement. An
    // identical request on a fresh connection resolves any committed evidence.
    tx.commit().await.map_err(db)?;
    #[cfg(test)]
    if store
        .abandon_fault
        .load(std::sync::atomic::Ordering::SeqCst)
        == 2
    {
        return Err(StoreError::Validation("injected committed response loss".into()).into());
    }
    Ok(outcome)
}

async fn settle(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    request: AbandonRequest,
) -> Result<(AbandonOutcome, bool), AbandonError> {
    let mut run = match load_run(tx, &request.expected.run_id, true).await {
        Err(StoreError::NotFound) => return Err(AbandonError::NotFound),
        result => result?,
    };
    // All ownership operations serialize on this row. Read the store clock only
    // after acquiring it, so waiting for a renewal cannot use a stale timestamp.
    let now = store.clock.now();
    if run.status.is_terminal() {
        return Ok((replay(tx, &run, &request).await?, false));
    }
    if run.owner != request.expected_owner || run.claim_token != request.expected.claim_token {
        return Err(AbandonError::StaleOwner);
    }
    if request.authority == AbandonAuthority::ExpiredLease && run.lease_until > now {
        return Err(AbandonError::LiveLease);
    }
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until = now;
    run.updated_at = now;
    save_run(tx, &run).await?;
    let calls: Vec<ToolCallRecord> = sqlx::query(
        "SELECT data FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running') ORDER BY id",
    )
    .bind(&run.id.0)
    .fetch_all(&mut **tx)
    .await
    .map_err(db)?
    .into_iter()
    .map(|row| decode(row.get("data")))
    .collect::<Result<_, _>>()?;
    let interrupted_tools = calls.iter().map(|call| call.id.clone()).collect();
    for mut call in calls {
        let (result, message, event) = interrupted_tool(&run, &call, now);
        insert_message(tx, &run, &message).await?;
        insert_event(tx, &run, &event).await?;
        call.status = ToolCallStatus::Interrupted;
        call.result = Some(result);
        sqlx::query("UPDATE tool_calls SET status=$2,data=$3 WHERE id=$1")
            .bind(&call.id.0)
            .bind(call_status(call.status))
            .bind(json(&call)?)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
    }
    run.status = RunStatus::Interrupted;
    save_run(tx, &run).await?;
    let evidence = AbandonEvidence {
        request,
        run: run.clone(),
        interrupted_tools,
    };
    let event = terminal_event(&evidence, now)?;
    insert_event(tx, &run, &event).await?;
    let outcome = replay(tx, &run, &evidence.request).await?;
    Ok((outcome, true))
}

async fn replay(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let row = sqlx::query(
        "SELECT seq,data FROM events WHERE run_id=$1 AND data->>'kind'='run_settled' AND data->'payload'->'abandonment_v1'->'run'=$2 ORDER BY seq LIMIT 1",
    )
    .bind(&run.id.0)
    .bind(json(run)?)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?
    .ok_or(AbandonError::AlreadyTerminal)?;
    let mut event: EventRecord = decode(row.get("data"))?;
    let evidence: AbandonEvidence = serde_json::from_value(event.payload["abandonment_v1"].clone())
        .map_err(|_| StoreError::Validation("invalid abandonment evidence".into()))?;
    if &evidence.request != request {
        return Err(AbandonError::StaleOwner);
    }
    event.cursor = Some(EventCursor(
        u64::try_from(row.get::<i64, _>("seq"))
            .map_err(|_| StoreError::Validation("invalid event cursor".into()))?,
    ));
    Ok(AbandonOutcome {
        run: evidence.run,
        terminal_event: event,
        interrupted_tools: evidence.interrupted_tools,
    })
}
