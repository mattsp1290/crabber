//! Coherent bounded source authentication; copied history never obtains authority.
use super::{Audit, Error, ForkBoundary, ForkSession, ImportedHistory, unknown};
use crate::fork::history::{AdmittedCall, AdmittedResult, Decision, Selection, SourceCall};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, Message, MessageId, Role, RunId, ToolCallRecord,
};
use sqlx::{Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};
pub(super) enum Failure {
    Limited,
    RawLimited(crate::SnapshotLimit),
    Error(Error),
}
impl From<Error> for Failure {
    fn from(value: Error) -> Self {
        Self::Error(value)
    }
}
struct Proof<'a>(&'a mut crate::public_session::budget::Budget);
impl Proof<'_> {
    async fn read(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        query: &str,
        scope: &str,
        key: &str,
        cap: usize,
    ) -> Result<Option<String>, Failure> {
        let row = sqlx::query(query)
            .bind(scope)
            .bind(key)
            .bind(i64::try_from(self.0.remaining().min(cap)).map_err(unknown)?)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?;
        let Some(row) = row else { return Ok(None) };
        let size = i64::from(row.try_get::<i32, _>("bytes").map_err(unknown)?);
        let size = usize::try_from(size).map_err(unknown)?;
        if size > cap {
            return Err(Error::UnknownStoreOutcome.into());
        }
        self.0.charge(size).map_err(|_| Failure::Limited)?;
        let text = row
            .try_get::<Option<String>, _>("record")
            .map_err(unknown)?
            .ok_or(Failure::Error(Error::UnknownStoreOutcome))?;
        if text.len() != size {
            return Err(Error::UnknownStoreOutcome.into());
        }
        Ok(Some(text))
    }
    async fn identities(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        query: &str,
        scope: &str,
        key: &str,
        count: usize,
    ) -> Result<Vec<String>, Failure> {
        let rows = sqlx::query(query)
            .bind(scope)
            .bind(key)
            .bind(i64::try_from(count).map_err(unknown)?)
            .bind(i64::try_from(self.0.remaining()).map_err(unknown)?)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        let mut ids = Vec::new();
        for row in rows {
            let size = i64::from(row.try_get::<i32, _>("bytes").map_err(unknown)?);
            let size = usize::try_from(size).map_err(unknown)?;
            if size > 512 {
                return Err(Error::UnknownStoreOutcome.into());
            }
            self.0.charge(size).map_err(|_| Failure::Limited)?;
            let id = row
                .try_get::<Option<String>, _>("id")
                .map_err(unknown)?
                .ok_or(Error::UnknownStoreOutcome)?;
            crate::fork::history::validate_id(&id)?;
            ids.push(id);
        }
        Ok(ids)
    }
}
pub(super) async fn prepare(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
) -> Result<Decision, Failure> {
    let (messages, mut raw_usage) = messages(tx, request).await?;
    let mut budget = crate::public_session::budget::Budget::new(
        request.max_proof_bytes,
        crate::PublicSessionLimit::ProofBytes,
    );
    let mut proof = Proof(&mut budget);
    let mut committed = BTreeSet::new();
    let mut admitted = BTreeSet::new();
    let imported = if messages.iter().any(|m| m.run_id.is_none()) {
        Some(imported(tx, &request.source_session_id, &mut proof, &mut committed).await?)
    } else {
        None
    };
    let mut runs = BTreeSet::new();
    let mut native = BTreeMap::new();
    let mut imported_calls = BTreeMap::new();
    for message in &messages {
        crate::fork::history::validate_id(&message.id.0)?;
        if let Some(run) = &message.run_id {
            crate::attempt::validate_identifier(&run.0).map_err(unknown)?;
            if runs.insert(run.clone()) {
                original_input(tx, request, run, &mut proof, &mut committed, &mut admitted).await?;
            }
            if !committed.contains(&message.id)
                && committed_event(tx, request, message, &mut proof).await?
            {
                committed.insert(message.id.clone());
            }
        }
        for part in &message.parts {
            if let ContentBlock::ToolCall { call_id, .. } = &part.content {
                if admitted.contains(&message.id) {
                    continue;
                }
                if native.len() + imported_calls.len() >= request.limits.tool_calls {
                    return Ok(Decision::Limited(crate::SnapshotLimit::ToolCalls));
                }
                if let Some(call) = imported
                    .as_ref()
                    .and_then(|h| h.calls.iter().find(|c| c.id == *call_id))
                {
                    imported_calls.insert(call_id.clone(), call);
                } else if let Some(run) = &message.run_id {
                    let call =
                        calls::read(tx, request, run, call_id, &mut proof, &mut raw_usage).await?;
                    native.insert(call_id.clone(), call);
                } else {
                    return Err(Error::Unsupported.into());
                }
            }
        }
    }
    let mut calls = native
        .values()
        .map(SourceCall::Native)
        .chain(
            imported_calls
                .values()
                .map(|call| SourceCall::Imported(call)),
        )
        .collect::<Vec<_>>();
    for message in &messages {
        if admitted.contains(&message.id) {
            for part in &message.parts {
                if matches!(part.content, ContentBlock::ToolCall { .. }) {
                    calls.push(admitted_call(message, &part.content, &messages, &admitted)?);
                }
            }
        }
    }
    if let Selection::Limited(limit) =
        crate::fork::history::select(request, &messages, &calls, &committed)?
    {
        return Ok(Decision::Limited(limit));
    }
    authenticate_calls(tx, request, &mut calls, &mut proof).await?;
    match crate::fork::history::select(request, &messages, &calls, &committed)? {
        Selection::Ready(plan) => plan.copy().map(Decision::Ready).map_err(Failure::from),
        Selection::Unsafe(boundary) => Ok(Decision::Unsafe(boundary)),
        Selection::Limited(limit) => Ok(Decision::Limited(limit)),
    }
}
async fn authenticate_calls(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
    calls: &mut [SourceCall<'_>],
    proof: &mut Proof<'_>,
) -> Result<(), Failure> {
    for source in calls {
        if let SourceCall::Native(call) = source
            && matches!(
                call.status,
                crabber_core::ToolCallStatus::Completed | crabber_core::ToolCallStatus::Failed
            )
            && terminal(tx, request, call, proof).await? != crate::PhaseQuiescence::Complete
        {
            *source = SourceCall::UnquiescentNative(call);
        }
    }
    Ok(())
}

async fn messages(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
) -> Result<(Vec<Message>, crate::SnapshotUsage), Failure> {
    let end = match &request.boundary {
        ForkBoundary::Empty => return Ok((Vec::new(), crate::SnapshotUsage::default())),
        ForkBoundary::AfterMessage { message_id } => {
            sqlx::query_scalar::<_, i64>("SELECT seq FROM messages WHERE session_id=$1 AND id=$2")
                .bind(&request.source_session_id.0)
                .bind(&message_id.0)
                .fetch_optional(&mut **tx)
                .await
                .map_err(unknown)?
                .ok_or(Error::NotFound)?
        }
    };
    let rows = sqlx::query(MESSAGES)
        .bind(&request.source_session_id.0)
        .bind(end)
        .bind(i64::try_from(request.limits.messages.saturating_add(1)).map_err(unknown)?)
        .bind(i64::try_from(request.limits.encoded_bytes).map_err(unknown)?)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() > request.limits.messages {
        return Err(Failure::RawLimited(crate::SnapshotLimit::Messages));
    }
    let mut result = Vec::new();
    let mut usage = crate::SnapshotUsage::default();
    for row in rows {
        if !row.try_get::<bool, _>("identities_ok").map_err(unknown)? {
            return Err(Error::UnknownStoreOutcome.into());
        }
        let text = row
            .try_get::<Option<String>, _>("record")
            .map_err(unknown)?
            .ok_or(Failure::RawLimited(crate::SnapshotLimit::EncodedBytes))?;
        let message: Message = serde_json::from_str(&text).map_err(unknown)?;
        if message.id.0 != row.try_get::<String, _>("id").map_err(unknown)?
            || message.session_id != request.source_session_id
            || message.run_id.as_ref().map(|id| id.0.as_str())
                != row
                    .try_get::<Option<String>, _>("run_id")
                    .map_err(unknown)?
                    .as_deref()
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
        usage.messages += 1;
        usage.parts += message.parts.len();
        usage.encoded_bytes += text.len();
        usage.text_bytes = message.parts.iter().fold(usage.text_bytes, |n, p| {
            n.saturating_add(crate::snapshot::text_bytes(&p.content))
        });
        if let Some(limit) = usage.exceeded(request.limits) {
            return Err(Failure::RawLimited(limit));
        }
        result.push(message);
    }
    Ok((result, usage))
}

async fn committed_event(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
    message: &Message,
    proof: &mut Proof<'_>,
) -> Result<bool, Failure> {
    let kind = match message.role {
        Role::Assistant => EventKind::MessageCommitted,
        Role::Tool => EventKind::ToolCallSettled,
        _ => return Ok(false),
    };
    let text = proof
        .read(
            tx,
            COMMITTED_EVENT,
            &request.source_session_id.0,
            &message.id.0,
            crate::phase::terminal_history::MAX_SETTLED_EVENT_BYTES,
        )
        .await?;
    let Some(text) = text else { return Ok(false) };
    let event = decode_event(&text)?;
    if event.session_id != request.source_session_id
        || Some(&event.run_id) != message.run_id.as_ref()
        || event.kind != kind
        || event.live_only
        || (kind == EventKind::MessageCommitted
            && event.payload.get("role") != Some(&serde_json::json!(Role::Assistant)))
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(true)
}

fn decode_event(text: &str) -> Result<EventRecord, Error> {
    let value: serde_json::Value = serde_json::from_str(text).map_err(unknown)?;
    crate::public_state::decode_event(sqlx::types::Json(value)).map_err(unknown)
}

const MESSAGE: &str = r"SELECT octet_length(snapshot_record) AS bytes,CASE WHEN octet_length(snapshot_record)<=$3 THEN snapshot_record END AS record FROM messages WHERE session_id=$1 AND id=$2";

const MESSAGES: &str = r"WITH prefix AS (SELECT id,run_id,snapshot_record AS record,seq FROM messages WHERE session_id=$1 AND seq<=$2 ORDER BY seq LIMIT $3) SELECT CASE WHEN octet_length(id)<=512 THEN id END AS id,CASE WHEN octet_length(run_id)<=512 THEN run_id END AS run_id,(octet_length(id)<=512 AND (run_id IS NULL OR octet_length(run_id)<=512)) AS identities_ok,CASE WHEN sum(octet_length(record)) OVER ()<=$4 THEN record END AS record FROM prefix ORDER BY seq";

const COMMITTED_EVENT: &str = r#"SELECT octet_length(record) AS bytes,CASE WHEN octet_length(record)<=$3 THEN record END AS record FROM (SELECT data::text AS record,seq,CASE WHEN data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(data->>'record', ',"payload":',1) ~ ',"kind":"(message_committed|tool_call_settled)"$' THEN (data->>'record')::json ELSE '{}'::json END ELSE data::json END AS d FROM events WHERE session_id=$1) e WHERE d->>'kind' IN ('message_committed','tool_call_settled') AND d #>> '{payload,message_id}'=$2 ORDER BY seq DESC LIMIT 1"#;

#[path = "source/imported.rs"]
mod imported;
#[path = "source/input.rs"]
mod input;
#[path = "source/terminal.rs"]
mod terminal;
use imported::imported;
use input::{admitted_call, original_input};
use terminal::terminal;

#[path = "source/calls.rs"]
mod calls;

mod ownership;
pub(in crate::postgres) use ownership::verify_imported_history;
