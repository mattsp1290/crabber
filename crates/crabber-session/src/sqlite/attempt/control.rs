use super::super::{SqliteStore, db};
use super::{records, sql};
use crate::{
    AttemptControlOutcome as Outcome, AttemptError as Error,
    attempt_control::{self, Control},
};
use crabber_core::{PhaseId, Run, ToolCallId};
use sqlx::{Sqlite, Transaction};
impl SqliteStore {
    pub(in super::super) async fn host_attempt(&self, control: Control) -> Result<Outcome, Error> {
        let digest = control.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(sql::unknown)?;
        let run_result = super::super::records::load_run(&mut tx, control.scope().1)
            .await
            .map_err(sql::unknown);
        let mut run = match run_result {
            Ok(run) => run,
            Err(error) => return super::finish(tx, Err(error)).await,
        };
        let result = async {
            let (session, id, key, expected, target) = control.scope();
            if run.session_id != *session || run.id != *id {
                return Err(Error::NotFound);
            }
            if let Some(receipt) = records::receipt(&mut tx, session, key).await? {
                if receipt.attempt.run_id != *id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(Outcome::Replayed(receipt));
            }
            let (mut record, current) = records::load(&mut tx, id, None, true)
                .await?
                .ok_or(Error::NotFound)?;
            if current != expected {
                return Err(Error::RevisionConflict);
            }
            if record.snapshot.target != *target {
                return Err(Error::GenerationConflict);
            }
            super::super::approval::guard_attempt(&mut tx, &run, &control.mutation()).await?;
            super::super::tool_continuation::guard_attempt(&mut tx, &run, &control.mutation())
                .await?;
            let now = self.clock.now();
            let turn = reconcile_children(&mut tx, &run, &control, now).await?;
            control.apply(&mut run, &mut record, current, now)?;
            if matches!(control, Control::Retire(_)) {
                if let Some(turn) = &turn.close {
                    let event = attempt_control::close_turn(&run, turn.clone(), now);
                    super::super::records::insert_event(&mut tx, &run, &event)
                        .await
                        .map_err(sql::unknown)?;
                }
                let event = attempt_control::terminal_event(&run, now, turn.source);
                super::super::records::insert_event(&mut tx, &run, &event)
                    .await
                    .map_err(sql::unknown)?;
                super::super::approval::release_terminal(&mut tx, &run, &control.mutation())
                    .await?;
                attempt_control::finish_record(&record)?;
                super::super::run_control::finalize(&mut tx, &run, now)
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            super::super::records::save_run(&mut tx, &run)
                .await
                .map_err(sql::unknown)?;
            records::save(&mut tx, &record, false).await?;
            let revision = records::revision(&mut tx, session)
                .await?
                .next()
                .map_err(|_| Error::RevisionExhausted)?;
            let event = control.event(&run, &record.snapshot, now)?;
            let receipt = records::publish(
                &mut tx,
                &run,
                &control.mutation(),
                &record.snapshot,
                digest,
                revision,
                event,
            )
            .await?;
            #[cfg(test)]
            if self.attempt_fault.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(Outcome::Applied(receipt))
        }
        .await;
        let result = super::finish(tx, result).await;
        #[cfg(test)]
        if self.attempt_fault.load(std::sync::atomic::Ordering::SeqCst) == 2
            && matches!(&result, Ok(Outcome::Applied(_)))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}
async fn reconcile_children(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    control: &Control,
    now: time::OffsetDateTime,
) -> Result<attempt_control::TurnContext, Error> {
    let Control::Retire(request) = control else {
        return Ok(attempt_control::TurnContext::default());
    };
    let context = super::super::approval::retirement_context(tx, run).await?;
    let turn = turns::read(tx, run, context.source.clone()).await?;
    retire_calls(
        tx,
        run,
        control,
        request,
        now,
        context.source.or(turn.source.clone()),
        context.order,
    )
    .await?;
    Ok(turn)
}
async fn retire_calls(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    control: &Control,
    request: &crate::AttemptRetirement,
    now: time::OffsetDateTime,
    turn: Option<crabber_core::TurnId>,
    order: Option<Vec<crabber_core::ToolCallId>>,
) -> Result<(), Error> {
    let mut pending = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let ids: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id>$2) ORDER BY id LIMIT 16",
        )
        .bind(&run.id.0)
        .bind(&after)
        .fetch_all(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call(tx, run, &ToolCallId::from(id.clone()))
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let is_pending = attempt_control::validate_logical(&call)?;
            let has_phase = validate_phases(tx, run, &call).await?;
            if is_pending {
                if has_phase || pending.len() == 64 {
                    return Err(Error::CleanupBlocked);
                }
                pending.push(call);
            }
            after = Some(id);
        }
    }
    let orphan:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_phases p LEFT JOIN tool_calls c ON c.run_id=p.run_id AND c.id=p.call_id WHERE p.run_id=$1 AND c.id IS NULL)").bind(&run.id.0).fetch_one(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
    if orphan {
        return Err(Error::UnknownStoreOutcome);
    }
    attempt_control::check_pending(request, &pending)?;
    for call in attempt_control::order_pending(run, pending, order)? {
        let records = attempt_control::cancel_records(control, run, &call, now, turn.clone());
        super::super::phase::apply_host_result(tx, run, call, records)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if !records::quiet(tx, &run.id).await? {
        return Err(Error::CleanupBlocked);
    }
    Ok(())
}
async fn validate_phases(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    call: &crabber_core::ToolCallRecord,
) -> Result<bool, Error> {
    let mut after: Option<String> = None;
    let mut found = false;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(id AS BLOB))<=128 THEN id END FROM tool_phases WHERE run_id=$1 AND call_id=$2 AND (CAST($3 AS TEXT) IS NULL OR id>$3) ORDER BY id LIMIT 16").bind(&run.id.0).bind(&call.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(db).map_err(sql::unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let phase =
                super::super::phase::load_host_record(tx, &run.id, &PhaseId::from(id.clone()))
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?
                    .ok_or(Error::UnknownStoreOutcome)?;
            attempt_control::validate_phase(run, call, &phase)?;
            found = true;
            after = Some(id);
        }
    }
    Ok(found)
}

#[cfg(test)]
#[path = "control/tests.rs"]
mod tests;
mod turns;
pub(in super::super) use turns::read as host_turn;
