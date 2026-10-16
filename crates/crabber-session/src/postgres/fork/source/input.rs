use super::{
    AdmittedCall, AdmittedResult, ContentBlock, Error, Failure, ForkSession, MESSAGE, Message,
    MessageId, Proof, Role, RunId, SourceCall, unknown,
};
use sqlx::{Postgres, Transaction};
use std::collections::BTreeSet;
pub(super) async fn original_input(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
    run: &RunId,
    proof: &mut Proof,
    committed: &mut BTreeSet<MessageId>,
    admitted: &mut BTreeSet<MessageId>,
) -> Result<(), Failure> {
    let Some(encoded) = proof
        .read(
            tx,
            BINDING,
            &request.source_session_id.0,
            &run.0,
            16 * 1024 * 1024,
        )
        .await?
    else {
        return Ok(());
    };
    let binding = crate::RetainedExecutionBinding::decode(&encoded).map_err(unknown)?;
    let facts = proof
        .read(tx, RUN_FACTS, &request.source_session_id.0, &run.0, 16384)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let (id, session, config, plan): (RunId, crabber_core::SessionId, String, String) =
        serde_json::from_str(&facts).map_err(unknown)?;
    if id != *run || session != request.source_session_id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let count = binding
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |i| i.messages().len());
    if count == 0 || count > crabber_core::input::MAX_INPUT_MESSAGES {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let ids = proof
        .identities(tx, INPUT_IDS, &request.source_session_id.0, &run.0, count)
        .await?;
    if ids.len() != count {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let mut messages = Vec::new();
    for id in ids {
        crate::fork::history::validate_id(&id)?;
        let text = proof
            .read(
                tx,
                MESSAGE,
                &request.source_session_id.0,
                &id,
                16 * 1024 * 1024,
            )
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        messages.push(serde_json::from_str::<Message>(&text).map_err(unknown)?);
    }
    binding
        .validate_messages(run, &request.source_session_id, &config, &plan, &messages)
        .map_err(unknown)?;
    if binding.capsule.request.workspace_id != request.workspace_id
        || binding.capsule.request.directory != request.directory
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    admitted.extend(messages.iter().map(|m| m.id.clone()));
    committed.extend(messages.into_iter().map(|m| m.id));
    Ok(())
}
pub(super) fn admitted_call<'a>(
    message: &'a Message,
    block: &'a ContentBlock,
    messages: &'a [Message],
    admitted: &BTreeSet<MessageId>,
) -> Result<SourceCall<'a>, Failure> {
    let ContentBlock::ToolCall {
        call_id,
        name,
        arguments,
    } = block
    else {
        return Err(unknown("admitted source kind").into());
    };
    let run_id = message
        .run_id
        .as_ref()
        .ok_or_else(|| unknown("admitted run"))?;
    let mut results = messages
        .iter()
        .filter(|candidate| {
            candidate.role == Role::Tool
                && candidate.run_id.as_ref() == Some(run_id)
                && admitted.contains(&candidate.id)
        })
        .flat_map(|message| &message.parts)
        .filter_map(|part| match &part.content {
            ContentBlock::ToolResult {
                call_id: id,
                content,
                is_error,
            } if id == call_id => Some(AdmittedResult {
                content,
                is_error: *is_error,
            }),
            _ => None,
        });
    let result = results.next();
    if results.next().is_some() {
        return Err(unknown("duplicate admitted result").into());
    }
    Ok(SourceCall::AdmittedInput(AdmittedCall {
        id: call_id,
        name,
        arguments,
        run_id,
        result,
    }))
}
const BINDING: &str = r"SELECT octet_length(b.data_json) AS bytes,CASE WHEN octet_length(b.data_json)<=$3 THEN b.data_json END AS record FROM run_execution_bindings b JOIN runs r ON r.id=b.run_id WHERE r.session_id=$1 AND r.id=$2";

const RUN_FACTS: &str = r"SELECT octet_length(json_build_array(r.data->>'id',r.data->>'session_id',r.data->>'config_hash',r.data->>'plan_fingerprint')::text) AS bytes,CASE WHEN octet_length(json_build_array(r.data->>'id',r.data->>'session_id',r.data->>'config_hash',r.data->>'plan_fingerprint')::text)<=$3 THEN json_build_array(r.data->>'id',r.data->>'session_id',r.data->>'config_hash',r.data->>'plan_fingerprint')::text END AS record FROM runs r WHERE r.session_id=$1 AND r.id=$2";

const INPUT_IDS: &str = r"WITH ids AS (SELECT id,seq AS position FROM messages WHERE session_id=$1 AND run_id=$2 ORDER BY seq LIMIT $3) SELECT octet_length(id) AS bytes,CASE WHEN octet_length(id)<=512 AND sum(octet_length(id)) OVER ()<=$4 THEN id END AS id FROM ids ORDER BY position";
