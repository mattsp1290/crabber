//! Exact original generic claim reconciliation; no current Run or grant.
use crate::session_owner::SessionOwnerError as Error;
use crate::session_owner::preparation::{
    ExpiredRunOrigin, ExpiredRunOutcome, JoinedPreparation, PreparationCommitError,
    PreparationCompletion, PreparedOperation,
    expired::{history, replay::Native},
};
use crabber_core::{RunId, SessionId};
use sqlx::{Postgres, Transaction};
impl crate::postgres::PostgresStore {
    pub(super) async fn replay_expired(
        tx: &mut Transaction<'_, Postgres>,
        joined: &JoinedPreparation,
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
        )
        .await?;
        if let Some(receipt) = receipt {
            super::super::verify_replay(tx, joined.observed_grant()).await?;
            return Ok(Some(PreparationCompletion::ExpiredRun(
                ExpiredRunOutcome::Replayed(receipt),
            )));
        }
        Ok(None)
    }
    pub(in crate::postgres) async fn lookup_expired_source(
        &self,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        history::identities(session, run, issuance)?;
        let mut tx = self.session_transaction(session).await.map_err(unknown)?;
        crate::postgres::deletion::ensure_live(&mut tx, session)
            .await
            .map_err(crate::deletion::owner_error)?;
        let result = Self::lookup_expired_metadata(&mut tx, session, run, issuance).await;
        tx.rollback().await.map_err(unknown)?;
        result
    }
    async fn lookup_expired_metadata(
        tx: &mut Transaction<'_, Postgres>,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        let ids = history::identities(session, run, issuance)?;
        let Some(next) = issuer(tx, session, run, issuance + 1).await? else {
            return Ok(None);
        };
        if next.completion_source.as_ref().is_none_or(|source| {
            source.native_operation != crate::native_parent::CompletionOperation::ExpiredRunResume
                || source.root_join_id != ids.join
        }) {
            return Ok(None);
        }
        let previous = issuer(tx, session, run, issuance)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let mut audits = Vec::with_capacity(5);
        for id in [&ids.reserve, &ids.begin, &ids.join] {
            audits.push(
                crate::postgres::session_owner::source_audit(tx, session, id)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
        }
        for id in history::audit_ids(&audits[0], &audits[1], &audits[2])? {
            audits.push(
                crate::postgres::session_owner::source_audit(tx, session, &id)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
        }
        Self::original_expired_receipt(tx, session, run, &previous, &next, &audits).await
    }
    async fn original_expired_receipt(
        tx: &mut Transaction<'_, Postgres>,
        session: &SessionId,
        run: &RunId,
        previous: &crate::native_parent::RunIssuance,
        next: &crate::native_parent::RunIssuance,
        audits: &[crate::session_owner::audit::Audit],
    ) -> Result<Option<crate::session_owner::preparation::ExpiredRunReceipt>, Error> {
        let target = history::target(audits)?;
        let parent = if let ExpiredRunOrigin::InitialUnstarted { reservation, .. } = &target.origin
        {
            Some(crate::postgres::expired::unstarted::read(tx, session, reservation).await?)
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
                let original = crate::postgres::expired::unkeyed::read(tx, session, run)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?;
                verify(Native::Unkeyed(&original))?
            }

            ExpiredRunOrigin::Suspended(id) => {
                let original = crate::postgres::attempt::records::receipt(tx, session, id)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let text: Option<String> = sqlx::query_scalar(EVENT)
                    .bind(i64::try_from(original.event_cursor.0).map_err(unknown)?)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let encoded: serde_json::Value =
                    serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?)
                        .map_err(unknown)?;
                let event = crate::public_state::decode_event(sqlx::types::Json(encoded))
                    .map_err(unknown)?;
                verify(Native::Suspended(&original, &event))?
            }
            ExpiredRunOrigin::KeyedInitial(key) => {
                let protocol: Option<i32> = sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2")
                    .bind(&session.0).bind(&run.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
                if protocol != Some(1) {
                    return Err(Error::UnknownStoreOutcome);
                }
                let init = source_record(tx, session, run, "init").await?;
                let begin = source_record(tx, session, run, "begin").await?;
                let (
                    crate::admission_journal::Source::Init(init),
                    crate::admission_journal::Source::Begin(begin),
                ) = (init, begin)
                else {
                    return Err(Error::UnknownStoreOutcome);
                };
                let text: Option<String> = sqlx::query_scalar(RECEIPT)
                    .bind(&session.0)
                    .bind(key.as_str())
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let receipt: crabber_core::AdmissionReceipt =
                    serde_json::from_str(&text.ok_or(Error::UnknownStoreOutcome)?)
                        .map_err(unknown)?;
                let initial = issuer(tx, session, run, 1)
                    .await?
                    .ok_or(Error::UnknownStoreOutcome)?;
                let activated_issuer = issuer(tx, session, run, begin.issuance.sequence)
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
async fn issuer(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
) -> Result<Option<crate::native_parent::RunIssuance>, Error> {
    let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
    super::super::super::super::native_parent::source_protocol::original(
        tx, session, run, sequence, &mut proof,
    )
    .await
    .map_err(unknown)
}

async fn source_record(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    kind: &str,
) -> Result<crate::admission_journal::Source, Error> {
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
const EVENT: &str =
    "SELECT CASE WHEN octet_length(data::text)<=65536 THEN data::text END FROM events WHERE seq=$1";
const RECEIPT: &str = "SELECT CASE WHEN octet_length(data::text)<=8192 THEN data::text END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const SOURCE: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind=$3";
