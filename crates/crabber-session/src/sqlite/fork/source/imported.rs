use super::{Audit, Error, Failure, ImportedHistory, MESSAGE, MessageId, Proof, unknown};
use crate::transaction::Transaction;
use crabber_core::{ImportedMessageProvenance, ImportedToolCall};
use sqlx::Sqlite;
use std::collections::BTreeSet;
pub(super) async fn imported(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    proof: &mut Proof<'_>,
    committed: &mut BTreeSet<MessageId>,
) -> Result<ImportedHistory, Failure> {
    let audit = import_origin(tx, session, proof).await?;
    let (messages, provenance) = message_prefix(tx, session, proof, audit.receipt.messages).await?;
    let mut history = ImportedHistory {
        messages,
        provenance,
        calls: Vec::new(),
    };
    let ids = proof
        .identities(
            tx,
            IMPORTED_CALL_IDS,
            &session.0,
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
            .read(tx, IMPORT_CALL, &session.0, &id, 8 * 1024 * 1024)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let call: ImportedToolCall = serde_json::from_str(&raw).map_err(unknown)?;
        if call.id.0 != id || call.session_id != *session {
            return Err(Error::UnknownStoreOutcome.into());
        }
        history.calls.push(call);
    }
    if history
        .messages
        .iter()
        .any(|m| m.run_id.is_some() || m.session_id != *session)
        || history.digest()? != audit.receipt.history_digest
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    committed.extend(history.messages.iter().map(|m| m.id.clone()));
    Ok(history)
}

async fn message_prefix(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    proof: &mut Proof<'_>,
    count: usize,
) -> Result<(Vec<crabber_core::Message>, Vec<ImportedMessageProvenance>), Failure> {
    let origins: i64 =
        sqlx::query_scalar("SELECT count(*) FROM imported_message_provenance WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    if usize::try_from(origins).ok() != Some(count) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let messages: i64 =
        sqlx::query_scalar("SELECT count(*) FROM messages WHERE session_id=$1 AND run_id IS NULL")
            .bind(&session.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    if usize::try_from(messages).ok() != Some(count) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let mut messages = Vec::new();
    let mut provenance_rows = Vec::new();
    let ids = proof
        .identities(tx, IMPORTED_MESSAGE_IDS, &session.0, "", count)
        .await?;
    if ids.len() != count {
        return Err(Error::UnknownStoreOutcome.into());
    }
    for id in ids {
        crate::fork::history::validate_id(&id)?;
        let raw = proof
            .read(tx, MESSAGE, &session.0, &id, 32 * 1024 * 1024)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let message: crabber_core::Message = serde_json::from_str(&raw).map_err(unknown)?;
        if message.id.0 != id || message.session_id != *session || message.run_id.is_some() {
            return Err(Error::UnknownStoreOutcome.into());
        }
        messages.push(message);
        let raw = proof
            .read(tx, IMPORT_PROVENANCE, &session.0, &id, 8 * 1024 * 1024)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let provenance: ImportedMessageProvenance = serde_json::from_str(&raw).map_err(unknown)?;
        if provenance.message_id.0 != id || provenance.session_id != *session {
            return Err(Error::UnknownStoreOutcome.into());
        }
        provenance_rows.push(provenance);
    }
    Ok((messages, provenance_rows))
}

pub(super) async fn import_origin(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    proof: &mut Proof<'_>,
) -> Result<Audit, Failure> {
    let raw = proof
        .read(tx, IMPORT_AUDIT, &session.0, "", 32768)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let audit: Audit = serde_json::from_str(&raw).map_err(unknown)?;
    audit.validate()?;
    let raw = proof
        .read(
            tx,
            IMPORT_RECEIPT,
            &session.0,
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
        || saved.target.session_id != *session
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(audit)
}
const IMPORT_AUDIT: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM session_fork_audit WHERE session_id=$1 AND $2=''";

const IMPORT_RECEIPT: &str = r"SELECT length(CAST(json_array(kind,semantic_digest,event_seq,data_json) AS BLOB)) AS bytes,CASE WHEN length(CAST(json_array(kind,semantic_digest,event_seq,data_json) AS BLOB))<=$3 THEN json_array(kind,semantic_digest,event_seq,data_json) END AS record FROM operation_receipts WHERE session_id=$1 AND operation_id=$2";

const IMPORT_PROVENANCE: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM imported_message_provenance WHERE session_id=$1 AND message_id=$2";

const IMPORT_CALL: &str = r"SELECT length(CAST(data_json AS BLOB)) AS bytes,CASE WHEN length(CAST(data_json AS BLOB))<=$3 THEN data_json END AS record FROM imported_tool_calls WHERE session_id=$1 AND id=$2";

const IMPORTED_MESSAGE_IDS: &str = r"WITH ids AS (SELECT id,seq AS position FROM messages WHERE session_id=$1 AND run_id IS NULL AND $2='' ORDER BY seq LIMIT $3) SELECT length(CAST(id AS BLOB)) AS bytes,CASE WHEN length(CAST(id AS BLOB))<=512 AND sum(length(CAST(id AS BLOB))) OVER ()<=$4 THEN id END AS id FROM ids ORDER BY position";

const IMPORTED_CALL_IDS: &str = r"WITH ids AS (SELECT id,ordinal AS position FROM imported_tool_calls WHERE session_id=$1 AND $2='' ORDER BY ordinal LIMIT $3) SELECT length(CAST(id AS BLOB)) AS bytes,CASE WHEN length(CAST(id AS BLOB))<=512 AND sum(length(CAST(id AS BLOB))) OVER ()<=$4 THEN id END AS id FROM ids ORDER BY position";
