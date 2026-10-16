use super::{
    Audit, Error, Failure, ForkSession, ImportedHistory, MESSAGE, MessageId, Proof, unknown,
};
use crabber_core::{ImportedMessageProvenance, ImportedToolCall};
use sqlx::{Sqlite, Transaction};
use std::collections::BTreeSet;
pub(super) async fn imported(
    tx: &mut Transaction<'_, Sqlite>,
    request: &ForkSession,
    proof: &mut Proof,
    committed: &mut BTreeSet<MessageId>,
) -> Result<ImportedHistory, Failure> {
    let audit = import_origin(tx, request, proof).await?;
    let origins: i64 =
        sqlx::query_scalar("SELECT count(*) FROM imported_message_provenance WHERE session_id=$1")
            .bind(&request.source_session_id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    if usize::try_from(origins).ok() != Some(audit.receipt.messages) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let mut history = ImportedHistory::default();
    let ids = proof
        .identities(
            tx,
            IMPORTED_MESSAGE_IDS,
            &request.source_session_id.0,
            "",
            audit.receipt.messages,
        )
        .await?;
    if ids.len() != audit.receipt.messages {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for id in ids {
        crate::fork::history::validate_id(&id)?;
        let raw = proof
            .read(
                tx,
                MESSAGE,
                &request.source_session_id.0,
                &id,
                32 * 1024 * 1024,
            )
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        history
            .messages
            .push(serde_json::from_str(&raw).map_err(unknown)?);
        let raw = proof
            .read(
                tx,
                IMPORT_PROVENANCE,
                &request.source_session_id.0,
                &id,
                8 * 1024 * 1024,
            )
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        history
            .provenance
            .push(serde_json::from_str::<ImportedMessageProvenance>(&raw).map_err(unknown)?);
    }
    let ids = proof
        .identities(
            tx,
            IMPORTED_CALL_IDS,
            &request.source_session_id.0,
            "",
            audit.receipt.imported_calls + 1,
        )
        .await?;
    if ids.len() != audit.receipt.imported_calls {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for id in ids {
        crate::fork::history::validate_id(&id)?;
        let raw = proof
            .read(
                tx,
                IMPORT_CALL,
                &request.source_session_id.0,
                &id,
                8 * 1024 * 1024,
            )
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let call: ImportedToolCall = serde_json::from_str(&raw).map_err(unknown)?;
        if call.id.0 != id || call.session_id != request.source_session_id {
            return Err(Error::UnknownStoreOutcome.into());
        }
        history.calls.push(call);
    }
    if history
        .messages
        .iter()
        .any(|m| m.run_id.is_some() || m.session_id != request.source_session_id)
        || history.digest()? != audit.receipt.history_digest
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    committed.extend(history.messages.iter().map(|m| m.id.clone()));
    Ok(history)
}
pub(super) async fn import_origin(
    tx: &mut Transaction<'_, Sqlite>,
    request: &ForkSession,
    proof: &mut Proof,
) -> Result<Audit, Failure> {
    let raw = proof
        .read(tx, IMPORT_AUDIT, &request.source_session_id.0, "", 32768)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let audit: Audit = serde_json::from_str(&raw).map_err(unknown)?;
    audit.validate()?;
    let raw = proof
        .read(
            tx,
            IMPORT_RECEIPT,
            &request.source_session_id.0,
            &audit.receipt.operation_id.0,
            65536,
        )
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let (kind, digest, event_seq, encoded): (String, String, Option<i64>, String) =
        serde_json::from_str(&raw).map_err(unknown)?;
    let saved: crate::ForkReceipt = serde_json::from_str(&encoded).map_err(unknown)?;
    if kind != "session_fork"
        || event_seq.is_some()
        || digest != saved.semantic_digest.as_str()
        || saved != audit.receipt
        || saved.target.session_id != request.source_session_id
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(audit)
}
const IMPORT_AUDIT: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM session_fork_audit WHERE session_id=$1 AND $2=''";

const IMPORT_RECEIPT: &str = r"SELECT length(CAST(json_array(kind,semantic_digest,event_seq,data_json) AS BLOB)) AS bytes,CASE WHEN length(CAST(json_array(kind,semantic_digest,event_seq,data_json) AS BLOB))<=$3 THEN json_array(kind,semantic_digest,event_seq,data_json) END AS record FROM operation_receipts WHERE session_id=$1 AND operation_id=$2";

const IMPORT_PROVENANCE: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM imported_message_provenance WHERE session_id=$1 AND message_id=$2";

const IMPORT_CALL: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM imported_tool_calls WHERE session_id=$1 AND id=$2";

const IMPORTED_MESSAGE_IDS: &str = r"WITH ids AS (SELECT id,seq AS position FROM messages WHERE session_id=$1 AND $2='' ORDER BY seq LIMIT $3) SELECT length(CAST(id AS BLOB)) AS bytes,CASE WHEN length(CAST(id AS BLOB))<=512 AND sum(length(CAST(id AS BLOB))) OVER ()<=$4 THEN id END AS id FROM ids ORDER BY position";

const IMPORTED_CALL_IDS: &str = r"WITH ids AS (SELECT id,ordinal AS position FROM imported_tool_calls WHERE session_id=$1 AND $2='' ORDER BY ordinal LIMIT $3) SELECT length(CAST(id AS BLOB)) AS bytes,CASE WHEN length(CAST(id AS BLOB))<=512 AND sum(length(CAST(id AS BLOB))) OVER ()<=$4 THEN id END AS id FROM ids ORDER BY position";
