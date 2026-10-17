use super::{Error, Proof, unknown};
use crate::native_parent::RunIssuance;
pub(super) use crate::session_owner::driver::successor::source::scope::Scope;
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
use sqlx::{Sqlite, Transaction};
pub(super) struct Pair {
    pub previous: RunIssuance,
    pub next: RunIssuance,
    pub audits: Vec<Audit>,
}
pub(super) async fn issuer(
    tx: &mut Transaction<'_, Sqlite>,
    scope: Scope<'_>,
    sequence: u64,
    proof: &mut Proof,
) -> Result<RunIssuance, Error> {
    crate::sqlite::native_parent::source_protocol::original(
        tx,
        scope.session,
        scope.run,
        sequence,
        proof,
    )
    .await
    .map_err(unknown)?
    .ok_or(Error::UnknownStoreOutcome)
}
pub(super) async fn pair(
    tx: &mut Transaction<'_, Sqlite>,
    scope: Scope<'_>,
    key: PreparationKey,
    proof: &mut Proof,
) -> Result<Pair, Error> {
    let join = crate::session_owner::preparation::identity::ids(&key, scope.session)?.join;
    proof.charge(16).map_err(unknown)?;
    let candidates: Vec<i64> = sqlx::query_scalar(SLOT)
        .bind(&scope.session.0)
        .bind(&scope.run.0)
        .bind(&join.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    let sequence = match candidates.as_slice() {
        [sequence] => *sequence,
        [] => return Err(Error::Unsupported),
        _ => return Err(Error::UnknownStoreOutcome),
    };
    let sequence = u64::try_from(sequence).map_err(unknown)?;
    let next = issuer(tx, scope, sequence, proof).await?;
    let previous = issuer(
        tx,
        scope,
        sequence
            .checked_sub(1)
            .filter(|v| *v > 0)
            .ok_or(Error::UnknownStoreOutcome)?,
        proof,
    )
    .await?;
    let joined = super::super::super::proof::audit(tx, scope.session, &join, proof)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut audits = Vec::with_capacity(5);
    for id in crate::session_owner::driver::successor::source::claim::audit_ids(&joined)? {
        audits.push(
            super::super::super::proof::audit(tx, scope.session, &id, proof)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?,
        );
    }
    Ok(Pair {
        previous,
        next,
        audits,
    })
}
pub(in crate::sqlite::session_owner::drivers::successor) async fn ancestry(
    tx: &mut Transaction<'_, Sqlite>,
    scope: Scope<'_>,
    facts: &crate::session_owner::driver::successor::source::ContinuationFacts,
    current: &RunIssuance,
    proof: &mut Proof,
) -> Result<(), Error> {
    let origin = issuer(tx, scope, facts.head, proof).await?;
    if crate::native_parent::issuance_digest(&origin).map_err(unknown)? != facts.head_digest
        || origin.state.status != crabber_core::RunStatus::Running
        || current.sequence < origin.sequence
    {
        return Err(Error::StaleOwner);
    }
    for sequence in origin.sequence + 1..=current.sequence {
        crate::session_owner::driver::successor::source::scope::status_only(
            &origin,
            &issuer(tx, scope, sequence, proof).await?,
        )?;
    }
    if current.state.token != origin.state.token {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
const SLOT: &str = "SELECT sequence FROM native_run_issuances WHERE session_id=?1 AND run_id=?2 AND json_extract(data_json,'$.completion_source.root_join_id')=?3 ORDER BY sequence LIMIT 2";
