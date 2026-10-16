//! Checkpoint cancellation consumes original immutable suspension proof.
use crate::{
    RunControlError as Error, RunControlRequest,
    run_control::{self, MAX_SUSPENSION_PROOF_BYTES, SuspensionProof, unknown},
};
use crabber_core::{Run, ToolCallId, ToolCallStatus};
use sqlx::{Sqlite, Transaction};
pub(in super::super) async fn save(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    conclusion: Option<&crate::AttemptConclusion>,
    receipt: &crate::AttemptReceipt,
) -> Result<(), Error> {
    let Some(conclusion) = conclusion else {
        return Ok(());
    };
    if !matches!(conclusion.boundary, crate::RunBoundary::Suspended { .. }) {
        return Ok(());
    }
    let proof = SuspensionProof {
        conclusion: conclusion.clone(),
        receipt: receipt.clone(),
    };
    crate::snapshot::encoded_bytes(&proof, MAX_SUSPENSION_PROOF_BYTES)
        .ok_or(Error::UnknownStoreOutcome)?;
    proof.conclusion.verify_replay(receipt).map_err(unknown)?;
    sqlx::query("INSERT INTO run_suspension_proofs(run_id,data_json) VALUES($1,$2) ON CONFLICT(run_id) DO UPDATE SET data_json=excluded.data_json")
        .bind(&run.id.0).bind(serde_json::to_string(&proof).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
pub(super) async fn cancel(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &RunControlRequest,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let calls = verify(tx, run).await?.0;
    apply_verified(tx, run, request, calls, run_control::source_turn(run)?, now).await
}
pub(in super::super) async fn apply_verified(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &RunControlRequest,
    calls: Vec<ToolCallId>,
    source: Option<crabber_core::TurnId>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let turn = super::super::attempt::host_turn(tx, run, source)
        .await
        .map_err(unknown)?;
    for id in calls {
        let call = super::super::phase::load_call(tx, run, &id)
            .await
            .map_err(unknown)?;
        let mut records = run_control::cancel_call(request, run, &call, now)?;
        records.event.turn_id.clone_from(&turn.source);
        super::super::tool_result::apply(tx, run, call, records)
            .await
            .map_err(unknown)?;
    }
    let terminal = run_control::cancelled_run(run, now);
    if let Some(turn) = turn.close {
        super::super::records::insert_event(
            tx,
            run,
            &crate::attempt_control::close_turn(run, turn, now),
        )
        .await
        .map_err(unknown)?;
    }
    let mut event = run_control::terminal_event(&terminal, now)?;
    event.turn_id = turn.source;
    event.cursor = Some(
        super::super::records::insert_event_captured(tx, run, &event)
            .await
            .map_err(unknown)?,
    );
    super::super::records::save_run(tx, &terminal)
        .await
        .map_err(unknown)?;
    super::finalize_extra(tx, &terminal, now, Some((request, &event))).await
}
pub(in super::super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(Vec<ToolCallId>, SuspensionProof), Error> {
    let data: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(data_json AS BLOB))<=4227072 THEN data_json END FROM run_suspension_proofs WHERE run_id=$1")
        .bind(&run.id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::Busy)?;
    let proof: SuspensionProof =
        serde_json::from_str(&data.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    let (latest, revision) = super::super::attempt::records::load(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::Busy)?;
    let ids = proof.verify(run, &latest.snapshot)?;
    let original =
        super::super::attempt::records::receipt(tx, &run.session_id, &proof.receipt.operation_id)
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
    if original != proof.receipt {
        return Err(Error::UnknownStoreOutcome);
    }
    super::super::approval::validate_terminal_phases(tx, run, revision)
        .await
        .map_err(|error| match error {
            crate::ApprovalError::UnknownStoreOutcome => Error::UnknownStoreOutcome,
            _ => Error::Busy,
        })?;
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running','waiting')",
    )
    .bind(&run.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if usize::try_from(count).map_err(unknown)? != ids.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    for id in &ids {
        let call = super::super::phase::load_call(tx, run, id)
            .await
            .map_err(unknown)?;
        let phase: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND call_id=$2)",
        )
        .bind(&run.id.0)
        .bind(&id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
        if phase || call.status != ToolCallStatus::Pending || call.result.is_some() {
            return Err(Error::Busy);
        }
    }
    Ok((ids, proof))
}
