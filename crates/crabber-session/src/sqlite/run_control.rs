//! Native exact-target acceptance and fenced worker boundaries.
use crate::public_session::v2::proof::{Proof, ReadError};
pub(super) mod public;
mod reads;
pub(super) mod suspension;
mod waiting;
use super::{SqliteStore, execution::SqliteExecution};
use crate::transaction::Transaction;
use crate::{
    RunControlError as Error,
    run_control::{
        MAX_CONTROLS, MAX_RUN_CONTROL_BYTES, Record, RunControlAction, RunControlBoundary,
        RunControlKind, RunControlOutcome, RunControlPage, RunControlReceipt, RunControlRequest,
        RunControlStatus, RunControlTarget, accepted, event, kind, reserve_ids, terminal_status,
        unknown, valid_scope,
    },
};
use crabber_core::{ControlId, EventCursor, Run, SessionId, TurnId};
use sqlx::Sqlite;
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, unknown).await
}
async fn publish(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &mut Record,
    fresh: bool,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    publish_extra(tx, run, record, fresh, now, None).await
}
async fn publish_extra(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &mut Record,
    fresh: bool,
    now: time::OffsetDateTime,
    source: Option<(&RunControlRequest, &crabber_core::EventRecord)>,
) -> Result<(), Error> {
    super::records::bump_public_revision(tx, &run.session_id)
        .await
        .map_err(unknown)?;
    let revision = reads::revision(tx, &run.session_id).await?;
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind("{}")
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    let cursor = EventCursor(u64::try_from(seq).map_err(unknown)?);
    if fresh {
        record.summary.receipt.event_cursor = cursor;
        record.summary.receipt.revision = revision;
    }
    record.summary.event_cursor = cursor;
    record.summary.revision = revision;
    let mut event = event(run, &record.summary, now)?;
    crate::run_control::terminal::attach(&mut event, &record.summary, source)?;
    sqlx::query("UPDATE events SET data=$2 WHERE seq=$1")
        .bind(seq)
        .bind(serde_json::to_string(&event).map_err(unknown)?)
        .execute(&mut **tx)
        .await
        .map_err(unknown)?;
    if fresh {
        let receipt = &record.summary.receipt;
        sqlx::query("INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'run_control',$3,$4,$5)")
            .bind(&run.session_id.0).bind(&record.request.id.0).bind(receipt.semantic_digest.as_str()).bind(seq).bind(serde_json::to_string(receipt).map_err(unknown)?)
            .execute(&mut **tx).await.map_err(unknown)?;
        sqlx::query("INSERT INTO run_controls(session_id,run_id,operation_id,sequence,request_json,summary_json) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(&run.session_id.0).bind(&run.id.0).bind(&record.request.id.0).bind(i64::try_from(receipt.sequence).map_err(unknown)?)
            .bind(serde_json::to_string(&record.request).map_err(unknown)?).bind(serde_json::to_string(&record.summary).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    } else if sqlx::query(
        "UPDATE run_controls SET summary_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&run.session_id.0)
    .bind(&record.request.id.0)
    .bind(serde_json::to_string(&record.summary).map_err(unknown)?)
    .execute(&mut **tx)
    .await
    .map_err(unknown)?
    .rows_affected()
        != 1
    {
        return Err(Error::UnknownStoreOutcome);
    }
    sqlx::query("INSERT INTO run_control_progress(session_id,operation_id,event_seq,summary_json) VALUES($1,$2,$3,$4)")
        .bind(&run.session_id.0).bind(&record.request.id.0).bind(seq).bind(serde_json::to_string(&record.summary).map_err(unknown)?).execute(&mut **tx).await.map_err(unknown)?;
    Ok(())
}
async fn guard_ids(
    tx: &mut Transaction<'_, Sqlite>,
    request: &RunControlRequest,
    previous: &[Record],
) -> Result<(), Error> {
    let (message_ids, part_ids) = reserve_ids(request, previous)?;
    for (table, ids) in [("messages", message_ids), ("parts", part_ids)] {
        let query = format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE id=$1)");
        for id in ids {
            if sqlx::query_scalar::<_, bool>(&query)
                .bind(id)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?
            {
                return Err(Error::SemanticConflict);
            }
        }
    }
    Ok(())
}
impl SqliteStore {
    pub(super) async fn control_binding(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
    ) -> Result<crate::RunControlBinding, Error> {
        valid_scope(session, &ControlId("read".into()))?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;
        let result = reads::binding(&mut tx, session, run).await;
        finish(tx, result).await
    }

    pub(super) async fn control_lookup(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<RunControlReceipt>, Error> {
        valid_scope(session, id)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;
        let result = reads::receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
    pub(super) async fn control_page(
        &self,
        target: &RunControlTarget,
    ) -> Result<RunControlPage, Error> {
        crate::run_control::validate_target(target)?;
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(unknown)?;
        let result = async {
            reads::verify_public_target(&mut tx, target).await?;
            Ok(RunControlPage {
                revision: reads::revision(&mut tx, &target.session_id).await?,
                controls: {
                    let controls = reads::summaries(&mut tx, target).await?;
                    let status = reads::status(&mut tx, target).await?;
                    for summary in &controls {
                        crate::run_control::verify_terminal(summary, status)?;
                    }
                    controls
                },
            })
        }
        .await;
        finish(tx, result).await
    }
    pub(super) async fn control_accept(
        &self,
        request: RunControlRequest,
    ) -> Result<RunControlOutcome, Error> {
        let digest = request.fingerprint()?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        let result = async {
            if let Some(saved) =
                reads::receipt(&mut tx, &request.target.session_id, &request.id).await?
            {
                if saved.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(RunControlOutcome::Replayed(saved));
            }
            let run = reads::load_run(&mut tx, &request.target).await?;
            if reads::revision(&mut tx, &run.session_id).await? != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            if run.status.is_terminal() {
                return Err(Error::Terminal);
            }
            let previous = reads::records(&mut tx, &request.target).await?;
            guard_ids(&mut tx, &request, &previous).await?;
            if previous.len() >= MAX_CONTROLS {
                return Err(Error::Capacity);
            }
            let bytes = previous.iter().try_fold(
                crate::run_control::reserved_bytes(&request)?,
                |n, record| {
                    Ok::<_, Error>(n + crate::run_control::reserved_bytes(&record.request)?)
                },
            )?;
            if bytes > MAX_RUN_CONTROL_BYTES {
                return Err(Error::Capacity);
            }
            let receipt = RunControlReceipt {
                operation_id: request.id.clone(),
                target: request.target.clone(),
                semantic_digest: digest.clone(),
                revision: request.expected_revision.next().map_err(unknown)?,
                event_cursor: EventCursor(0),
                sequence: previous.len() as u64 + 1,
                kind: kind(&request),
            };
            let mut record = Record {
                request,
                summary: accepted(&receipt),
            };
            publish(&mut tx, &run, &mut record, true, self.clock.now()).await?;
            waiting::apply(
                &mut tx,
                &run,
                &record.request,
                record.summary.receipt.clone(),
                self.clock.now(),
            )
            .await?;
            Ok(RunControlOutcome::Applied(record.summary.receipt))
        }
        .await;
        #[cfg(test)]
        let result = result.and_then(|outcome| {
            if matches!(outcome, RunControlOutcome::Applied(_)) {
                crate::run_control_contract::process::pause("accept_before");
                crate::binding_fault::trip(&self.control_fault, 1).map_err(unknown)?;
            }
            Ok(outcome)
        });
        let result = finish(tx, result).await;
        #[cfg(test)]
        if matches!(&result, Ok(RunControlOutcome::Applied(_))) {
            crate::run_control_contract::process::pause("accept_after");
            crate::binding_fault::trip(&self.control_fault, 2).map_err(unknown)?;
        }
        result
    }
}
impl SqliteExecution {
    pub(super) async fn control_boundary(
        &self,
        turn: Option<&TurnId>,
    ) -> Result<RunControlBoundary, Error> {
        crate::run_control::validate_turn(turn)?;
        let mut tx = self.store.begin_write().await.map_err(unknown)?;
        let run = self
            .store
            .fenced(&mut tx, &self.fence)
            .await
            .map_err(|error| crate::run_control::fenced_error(&error))?;
        let result = async {
            let mut pending = reads::records(&mut tx, &RunControlTarget::of(&run)).await?;
            let cancel = pending.iter().any(|r| {
                r.summary.receipt.kind == RunControlKind::Cancel
                    && matches!(
                        r.summary.status,
                        RunControlStatus::Accepted | RunControlStatus::Observed
                    )
            });
            let mut result = RunControlBoundary {
                cancel_observed: cancel,
                incorporated: 0,
            };
            for record in &mut pending {
                if record.summary.status != RunControlStatus::Accepted {
                    continue;
                }
                match &record.request.action {
                    RunControlAction::Cancel { .. } => {
                        record.summary.status = RunControlStatus::Observed;
                    }
                    RunControlAction::Steer { input } if !cancel && turn.is_some() => {
                        let messages = input
                            .materialize_messages(&run.session_id, self.store.clock.now())
                            .map_err(unknown)?;
                        for mut message in messages {
                            message.run_id = Some(run.id.clone());
                            super::records::insert_message(&mut tx, &run, &message)
                                .await
                                .map_err(unknown)?;
                        }
                        record.summary.status = RunControlStatus::Incorporated {
                            turn_id: turn.unwrap().clone(),
                        };
                        result.incorporated += 1;
                    }
                    RunControlAction::Steer { .. } => continue,
                }
                publish(&mut tx, &run, record, false, self.store.clock.now()).await?;
            }
            Ok(result)
        }
        .await;
        #[cfg(test)]
        let result = result.and_then(|boundary| {
            if boundary.cancel_observed || boundary.incorporated > 0 {
                crate::run_control_contract::process::pause("boundary_before");
                crate::binding_fault::trip(&self.store.control_fault, 1).map_err(unknown)?;
            }
            Ok(boundary)
        });
        let result = finish(tx, result).await;
        #[cfg(test)]
        if result
            .as_ref()
            .is_ok_and(|boundary| boundary.cancel_observed || boundary.incorporated > 0)
        {
            crate::run_control_contract::process::pause("boundary_after");
            crate::binding_fault::trip(&self.store.control_fault, 2).map_err(unknown)?;
        }
        result
    }
}
/// Invoked inside the same transaction that commits native run terminal truth.
pub(super) async fn finalize(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    finalize_extra(tx, run, now, None).await
}
pub(super) async fn finalize_extra(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    now: time::OffsetDateTime,
    source: Option<(&RunControlRequest, &crabber_core::EventRecord)>,
) -> Result<(), Error> {
    if !run.status.is_terminal() {
        return Ok(());
    }
    let mut matched = source.is_none();
    for mut record in reads::records(tx, &RunControlTarget::of(run)).await? {
        if let Some(status) = terminal_status(&record, run.status) {
            record.summary.status = status;
            let selected = source.filter(|(request, _)| **request == record.request);
            matched |= selected.is_some();
            publish_extra(tx, run, &mut record, false, now, selected).await?;
        }
    }
    if !matched {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(super) async fn pending(tx: &mut Transaction<'_, Sqlite>, run: &Run) -> Result<bool, Error> {
    Ok(reads::records(tx, &RunControlTarget::of(run))
        .await?
        .iter()
        .any(|r| {
            matches!(
                r.summary.status,
                RunControlStatus::Accepted | RunControlStatus::Observed
            )
        }))
}

#[cfg(test)]
#[path = "run_control/tests.rs"]
mod tests;

pub(super) async fn verify_cancelled_target(
    tx: &mut Transaction<'_, Sqlite>,
    target: &RunControlTarget,
) -> Result<(), Error> {
    reads::verify_public_target(tx, target).await?;
    if reads::status(tx, target).await? != crabber_core::RunStatus::Interrupted {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(super) async fn verify_original_cancel(
    tx: &mut Transaction<'_, Sqlite>,
    accepted: &RunControlReceipt,
) -> Result<(), Error> {
    if reads::receipt(tx, &accepted.target.session_id, &accepted.operation_id)
        .await
        .map_err(unknown)?
        .as_ref()
        != Some(accepted)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(super) async fn verify_original_cancel_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    accepted: &RunControlReceipt,
    proof: &mut Proof,
) -> Result<(), ReadError<Error>> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(CANCEL_PROOF_COST)
            .bind(&accepted.target.session_id.0)
            .bind(&accepted.operation_id.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    Ok(verify_original_cancel(tx, accepted).await?)
}
pub(super) async fn verify_cancelled_target_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    target: &RunControlTarget,
    proof: &mut Proof,
) -> Result<(), ReadError<Error>> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(TARGET_PROOF_COST).bind(&target.run_id.0),
        proof,
    )
    .await
    .map_err(ReadError::Budget)?;
    Ok(verify_cancelled_target(tx, target).await?)
}
const CANCEL_PROOF_COST: &str = r"SELECT CAST(COALESCE(length(CAST(r.data_json AS BLOB)),0)+COALESCE(length(CAST(r.semantic_digest AS BLOB)),0)+11+COALESCE(length(CAST(e.data AS BLOB)),0) AS BIGINT) FROM operation_receipts r LEFT JOIN events e ON e.session_id=r.session_id AND e.seq=r.event_seq WHERE r.session_id=$1 AND r.operation_id=$2";
const TARGET_PROOF_COST: &str = r"SELECT CAST(COALESCE(length(CAST(session_id AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.config_hash') AS BLOB)),0)+COALESCE(length(CAST(json_extract(data,'$.plan_fingerprint') AS BLOB)),0)+COALESCE(length(CAST(status AS BLOB)),0) AS BIGINT) FROM runs WHERE id=$1";
