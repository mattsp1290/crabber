//! Exact original generic claim reconciliation; no current Run or grant.
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunOutcome, JoinedPreparation, PreparationCommitError,
    PreparationCompletion, PreparedOperation,
    expired::{history, replay::Native},
};
use crate::transaction::Transaction;
use crabber_core::{RunId, SessionId};
use sqlx::Sqlite;
impl crate::sqlite::SqliteStore {
    pub(super) async fn replay_expired(
        tx: &mut Transaction<'_, Sqlite>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<PreparationCompletion>, PreparationCommitError> {
        let PreparedOperation::ExpiredRunResume(prepared) = joined.observed_operation() else {
            return Ok(None);
        };
        let source = &prepared.source;
        let receipt = Self::lookup_expired_metadata(
            tx,
            &source.request().session_id,
            &source.request().expected_fence.run_id,
            source.expected_issuance(),
            proof,
        )
        .await?;
        if let Some(receipt) = receipt {
            super::super::verify_replay(tx, joined.observed_grant(), proof).await?;
            return Ok(Some(PreparationCompletion::ExpiredRun(
                ExpiredRunOutcome::Replayed(receipt),
            )));
        }
        Ok(None)
    }
    pub(in crate::sqlite) async fn lookup_expired_source(
        &self,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        history::identities(session, run, issuance)?;
        let mut tx = self.begin_write().await.map_err(unknown)?;
        crate::sqlite::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        let result =
            Self::lookup_expired_metadata(&mut tx, session, run, issuance, &mut proof).await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
    async fn lookup_expired_metadata(
        tx: &mut Transaction<'_, Sqlite>,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        let ids = history::identities(session, run, issuance)?;
        let Some(next) = issuer(tx, session, run, issuance + 1, proof).await? else {
            return Ok(None);
        };
        if next.completion_source.as_ref().is_none_or(|source| {
            source.native_operation != crate::native_parent::CompletionOperation::ExpiredRunResume
                || source.root_join_id != ids.join
        }) {
            return Ok(None);
        }
        let previous = issuer(tx, session, run, issuance, proof)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let mut audits = Vec::with_capacity(5);
        for id in [&ids.reserve, &ids.begin, &ids.join] {
            audits.push(
                crate::sqlite::session_owner::source_audit_with_proof(tx, session, id, proof)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
        }
        for id in history::audit_ids(&audits[0], &audits[1], &audits[2])? {
            audits.push(
                crate::sqlite::session_owner::source_audit_with_proof(tx, session, &id, proof)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
        }
        Self::original_expired_receipt(tx, session, run, &previous, &next, &audits, proof).await
    }
    async fn original_expired_receipt(
        tx: &mut Transaction<'_, Sqlite>,
        session: &SessionId,
        run: &RunId,
        previous: &crate::native_parent::RunIssuance,
        next: &crate::native_parent::RunIssuance,
        audits: &[crate::session_owner::audit::Audit],
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        let target = history::target(audits)?;
        let parent = if let ExpiredRunOrigin::InitialUnstarted { reservation, .. } = &target.origin
        {
            Some(
                crate::sqlite::expired::unstarted::read_with_proof(tx, session, reservation, proof)
                    .await?,
            )
        } else {
            None
        };
        let origin = if let ExpiredRunOrigin::InitialUnstarted { admission, .. } = &target.origin {
            crate::session_owner::preparation::expired::unstarted::admission(admission)
        } else {
            target.origin.clone()
        };
        let verify = |native| {
            history::verify(
                previous,
                next,
                audits,
                if let Some(original) = &parent {
                    Native::Unstarted {
                        initial: Box::new(native),
                        original,
                    }
                } else {
                    native
                },
            )
        };
        let receipt = match &origin {
            ExpiredRunOrigin::InitialUnstarted { .. } => return Err(Error::UnknownStoreOutcome),
            ExpiredRunOrigin::UnkeyedAdmission(_) => {
                let original =
                    crate::sqlite::expired::unkeyed::read_with_proof(tx, session, run, proof)
                        .await?
                        .ok_or(Error::UnknownStoreOutcome)?;
                verify(Native::Unkeyed(&original))?
            }

            ExpiredRunOrigin::Suspended(id) => {
                let (original, event) = suspended_source(tx, session, id, proof).await?;
                verify(Native::Suspended(&original, &event))?
            }
            ExpiredRunOrigin::KeyedInitial(key) => {
                let protocol: Option<i32> = sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2")
                    .bind(&session.0).bind(&run.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
                if protocol != Some(1) {
                    return Err(Error::UnknownStoreOutcome);
                }
                let init = source_record(tx, session, run, "init", proof).await?;
                let begin = source_record(tx, session, run, "begin", proof).await?;
                let (
                    crate::admission_journal::Source::Init(init),
                    crate::admission_journal::Source::Begin(begin),
                ) = (init, begin)
                else {
                    return Err(Error::UnknownStoreOutcome);
                };
                let receipt = keyed_receipt(tx, session, key, proof).await?;
                let initial = issuer(tx, session, run, 1, proof)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let activated_issuer = issuer(tx, session, run, begin.issuance.sequence, proof)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?;
                verify(Native::Initial {
                    init: &init,
                    begin: &begin,
                    receipt: &receipt,
                    initial: &initial,
                    begun: &activated_issuer,
                })?
            }
        };
        Ok(receipt)
    }
}
// Load the independently authenticated suspension receipt and its actual bounded Event.
async fn suspended_source(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &crabber_core::ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(crate::AttemptReceipt, crabber_core::EventRecord), Error> {
    let original = crate::sqlite::attempt::records::receipt_with_proof(tx, session, id, proof)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let query =
        format!("SELECT CAST(length(CAST(record AS BLOB)) AS BIGINT) FROM ({EVENT}) selected");
    crate::sqlite::read_proof::charge(
        tx,
        sqlx::query_scalar(&query).bind(i64::try_from(original.event_cursor.0).map_err(unknown)?),
        proof,
    )
    .await
    .map_err(|_| Error::Capacity)?;
    let text: Option<String> = sqlx::query_scalar(EVENT)
        .bind(i64::try_from(original.event_cursor.0).map_err(unknown)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let event: crabber_core::EventRecord =
        serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    Ok((original, event))
}

async fn keyed_receipt(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    key: &crabber_core::AdmissionKey,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crabber_core::AdmissionReceipt, Error> {
    let query =
        format!("SELECT CAST(length(CAST(record AS BLOB)) AS BIGINT) FROM ({RECEIPT}) selected");
    crate::sqlite::read_proof::charge(
        tx,
        sqlx::query_scalar(&query)
            .bind(&session.0)
            .bind(key.as_str()),
        proof,
    )
    .await
    .map_err(|_| Error::Capacity)?;
    let text: Option<String> = sqlx::query_scalar(RECEIPT)
        .bind(&session.0)
        .bind(key.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let receipt: crabber_core::AdmissionReceipt =
        serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    Ok(receipt)
}

async fn issuer(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<crate::native_parent::RunIssuance>, Error> {
    super::super::super::super::native_parent::source_protocol::original(
        tx, session, run, sequence, proof,
    )
    .await
    .map_err(unknown)
}

async fn source_record(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    kind: &str,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<crate::admission_journal::Source, Error> {
    let query =
        format!("SELECT CAST(length(CAST(record AS BLOB)) AS BIGINT) FROM ({SOURCE}) selected");
    crate::sqlite::read_proof::charge(
        tx,
        sqlx::query_scalar(&query)
            .bind(&session.0)
            .bind(&run.0)
            .bind(kind),
        proof,
    )
    .await
    .map_err(|_| Error::Capacity)?;
    let row: Option<String> = sqlx::query_scalar(SOURCE)
        .bind(&session.0)
        .bind(&run.0)
        .bind(kind)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::admission_journal::Source::decode(&row.ok_or(Error::UnknownStoreOutcome)?)
        .map_err(unknown)
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}
const EVENT: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=65536 THEN data END AS record FROM events WHERE seq=$1";
const RECEIPT: &str = "SELECT CASE WHEN length(CAST(data AS BLOB))<=8192 THEN data END AS record FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const SOURCE: &str = "SELECT CASE WHEN length(CAST(data_json AS BLOB))<=8192 THEN data_json END AS record FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind=$3";
