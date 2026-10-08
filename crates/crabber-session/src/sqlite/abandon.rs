use super::records::{call_status, decode_row, insert_event, insert_message, load_run, save_run};
use super::{SqliteStore, db, text};
use crate::StoreError;
use crate::abandonment::{AbandonCommit, AbandonEvidence, interrupted_tool, terminal_event};
use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};
use crabber_core::{EventCursor, EventRecord, Run, RunStatus, ToolCallRecord, ToolCallStatus};
use sqlx::{Row, Sqlite, Transaction};

pub(super) async fn abandon(
    store: &SqliteStore,
    request: AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let mut tx = store.begin_write().await?;
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
        return super::transactions::finish(tx, Ok(outcome), Into::into).await;
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
    super::transactions::finish(tx, Ok(()), Into::<AbandonError>::into).await?;
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
    store: &SqliteStore,
    tx: &mut Transaction<'_, Sqlite>,
    request: AbandonRequest,
) -> Result<(AbandonOutcome, bool), AbandonError> {
    let mut run = match load_run(tx, &request.expected.run_id).await {
        Err(StoreError::NotFound) => return Err(AbandonError::NotFound),
        result => result?,
    };
    // BEGIN IMMEDIATE was acquired before loading the run or reading the clock.
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
    .map(|row| decode_row(&row, "data"))
    .collect::<Result<_, _>>()?;
    let interrupted_tools = calls.iter().map(|call| call.id.clone()).collect();
    for mut call in calls {
        let (result, message, event) = interrupted_tool(&run, &call, now);
        insert_message(tx, &run, &message).await?;
        insert_event(tx, &run, &event).await?;
        call.status = ToolCallStatus::Interrupted;
        call.result = Some(result);
        text(&call)?;
        let accounting = super::snapshot::call_record(&call)?;
        sqlx::query(
            "UPDATE tool_calls SET \
                status=$2,data=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 \
                WHERE id=$1",
        )
        .bind(&call.id.0)
        .bind(call_status(call.status))
        .bind(&accounting.record)
        .bind(accounting.parts)
        .bind(accounting.text)
        .bind(accounting.bytes)
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
    let mut event = terminal_event(&evidence, now)?;
    insert_event(tx, &run, &event).await?;
    let seq: i64 =
        sqlx::query_scalar("SELECT seq FROM events WHERE run_id=$1 ORDER BY seq DESC LIMIT 1")
            .bind(&run.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(db)?;
    event.cursor =
        Some(EventCursor(u64::try_from(seq).map_err(|_| {
            StoreError::Validation("invalid event cursor".into())
        })?));
    let outcome = AbandonOutcome {
        run,
        terminal_event: event,
        interrupted_tools: evidence.interrupted_tools,
    };
    // Ordinary event writers have no path to this table. Publish authenticated
    // provenance only in the transaction that rotated and fully settled the run.
    let commit = AbandonCommit {
        request: evidence.request,
        outcome: outcome.clone(),
    };
    sqlx::query("INSERT INTO abandonment_commits(run_id,event_seq,data) VALUES($1,$2,$3)")
        .bind(&outcome.run.id.0)
        .bind(seq)
        .bind(text(&commit)?)
        .execute(&mut **tx)
        .await
        .map_err(db)?;
    Ok((outcome, true))
}

async fn replay(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &AbandonRequest,
) -> Result<AbandonOutcome, AbandonError> {
    let row = sqlx::query(
        "SELECT abandonment_commits.data,events.seq,events.data AS event FROM \
                abandonment_commits JOIN events ON events.seq=abandonment_commits.event_seq \
                WHERE abandonment_commits.run_id=$1",
    )
    .bind(&run.id.0)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db)?
    .ok_or(AbandonError::AlreadyTerminal)?;
    let commit: AbandonCommit = decode_row(&row, "data")?;
    let mut event: EventRecord = decode_row(&row, "event")?;
    event.cursor = Some(EventCursor(
        u64::try_from(row.try_get::<i64, _>("seq").map_err(db)?)
            .map_err(|_| StoreError::Validation("invalid event cursor".into()))?,
    ));
    if event != commit.outcome.terminal_event {
        return Err(StoreError::Validation("invalid abandonment terminal event".into()).into());
    }
    let unfinished: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running'))")
        .bind(&run.id.0)
.fetch_one(&mut **tx)
.await.map_err(db)?;
    if unfinished {
        return Err(StoreError::Validation("unfinished abandoned tools".into()).into());
    }
    commit.replay(run, request)
}
