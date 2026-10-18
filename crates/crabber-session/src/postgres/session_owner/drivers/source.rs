//! Exact independent initial sources, usable after later native work for metadata replay.
use super::super::{Audit, Error, unknown};
use super::proof;
use crate::postgres::transaction::Transaction;
use crate::session_owner::preparation::admission::{SourceAudits, Witness};
use crate::{
    admission_journal::{Init, Source},
    native_parent::RunIssuance,
    public_session::v2::proof::{Proof, ReadError},
};
use crabber_core::{RunId, Session, SessionId};
use sqlx::{Postgres, Row};
pub(in crate::postgres) struct Original {
    pub session: Session,
    pub initial: RunIssuance,
    pub witness: Witness,
    pub audits: [Audit; 5],
    pub init: Option<Init>,
}
impl Original {
    pub fn audits(&self) -> SourceAudits<'_> {
        SourceAudits::from_array(&self.audits)
    }
    pub fn verify_link(
        &self,
        link: &crate::session_owner::driver::DriverLink,
    ) -> Result<(), Error> {
        if link.is_unkeyed() {
            if self.init.is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
            link.verify_original_unkeyed(
                &self.session,
                &self.initial,
                &self.witness,
                &self.audits(),
            )
        } else {
            link.verify_original(
                &self.session,
                &self.initial,
                &self.witness,
                &self.audits(),
                self.init.as_ref().ok_or(Error::UnknownStoreOutcome)?,
            )
        }
    }
}
pub(super) async fn original(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    budget: &mut Proof,
) -> Result<Original, Error> {
    Box::pin(original_read(tx, session, run, budget))
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in crate::postgres) async fn original_read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    budget: &mut Proof,
) -> Result<Original, ReadError<Error>> {
    proof::read_charge(tx, SESSION_COST, &session.0, &run.0, budget).await?;
    let raw: Option<String> = sqlx::query_scalar(SESSION)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    let current: Session =
        serde_json::from_str(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    if current.id != *session {
        return Err(Error::UnknownStoreOutcome.into());
    }
    proof::read_charge(tx, WITNESS_COST, &session.0, &run.0, budget).await?;
    let row = sqlx::query(WITNESS)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let witness: Witness = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(unknown)?;
    witness.validate()?;
    if witness.session_id() != session
        || witness.run_id() != run
        || row
            .try_get::<Option<String>, _>("root_join")
            .map_err(unknown)?
            .as_deref()
            != Some(&witness.join_id()?.0)
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let initial = super::super::super::admission_journal::issuance_with_proof(
        tx,
        session,
        run,
        Some(1),
        budget,
    )
    .await
    .map_err(|error| error.map_domain(unknown))?;
    let mut audits = Vec::with_capacity(5);
    for id in witness.audit_ids()? {
        audits.push(
            proof::read_audit(tx, session, &id, budget)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    let audits = audits.try_into().map_err(unknown)?;
    let init = if witness.is_unkeyed() {
        witness.unkeyed_binding_digest()?;
        None
    } else {
        Some(init(tx, session, run, &initial, budget).await?)
    };
    let result = Original {
        session: current,
        initial,
        witness,
        audits,
        init,
    };
    result
        .witness
        .verify_sources(&result.session, &result.initial, &result.audits())?;
    Ok(result)
}
async fn init(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    initial: &RunIssuance,
    budget: &mut Proof,
) -> Result<Init, ReadError<Error>> {
    budget.charge(4).map_err(ReadError::Budget)?;
    let protocol: Option<i32> = sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2").bind(&session.0).bind(&run.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if protocol != Some(1) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    proof::read_charge(tx, INIT_COST, &session.0, &run.0, budget).await?;
    let raw: Option<String> = sqlx::query_scalar(INIT)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    let Source::Init(init) =
        Source::decode(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?
    else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    proof::read_charge(
        tx,
        RECEIPT_COST,
        &session.0,
        init.driver_key().as_str(),
        budget,
    )
    .await?;
    let raw: Option<String> = sqlx::query_scalar(RECEIPT)
        .bind(&session.0)
        .bind(init.driver_key().as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    let receipt = serde_json::from_str(&raw.ok_or(Error::UnknownStoreOutcome)?).map_err(unknown)?;
    init.verify_driver_origin(&receipt, initial)
        .map_err(unknown)?;
    Ok(init)
}
const SESSION_COST: &str = "SELECT CAST(octet_length(data::text) AS BIGINT) FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const SESSION: &str = "SELECT CASE WHEN octet_length(data::text)<=16384 THEN data::text END FROM sessions WHERE id=$1 AND CAST($2 AS TEXT) IS NOT NULL";
const WITNESS_COST: &str = "SELECT CAST(octet_length(data_json)+octet_length(root_join) AS BIGINT) FROM preparation_admission_witnesses WHERE session_id=$1 AND run_id=$2";
const WITNESS: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END AS data,CASE WHEN octet_length(root_join)<=128 THEN root_join END AS root_join FROM preparation_admission_witnesses WHERE session_id=$1 AND run_id=$2";
const INIT_COST: &str = "SELECT CAST(octet_length(data_json) AS BIGINT) FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='init'";
const INIT: &str = "SELECT CASE WHEN octet_length(data_json)<=8192 THEN data_json END FROM admission_execution_sources WHERE session_id=$1 AND run_id=$2 AND kind='init'";
const RECEIPT_COST: &str = "SELECT CAST(octet_length(data::text) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const RECEIPT: &str = "SELECT CASE WHEN octet_length(data::text)<=8192 THEN data::text END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
