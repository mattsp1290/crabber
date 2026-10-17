use super::PostgresStore;
use crate::postgres::transaction::Transaction;
use crate::public_session::budget::{self, Budget, Failure, unknown};
use crate::{
    AttemptSnapshot, PublicControlHeader, PublicHeaderOutcome, PublicHeaderRequest,
    PublicHeaderUsage, PublicSessionError, PublicSessionLimit,
};
use crabber_core::{RunStatus, SessionRevision};
use sqlx::{Postgres, Row};
impl PostgresStore {
    pub(super) async fn read_public_header(
        &self,
        request: PublicHeaderRequest,
    ) -> Result<PublicHeaderOutcome, PublicSessionError> {
        request.validate()?;
        let result = async {
        let mut tx = crate::postgres::transaction::begin(&self.pool).await.map_err(unknown)?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").execute(&mut *tx).await.map_err(unknown)?;
        let row = sqlx::query("SELECT s.public_revision,CAST(octet_length(s.data::text)+COALESCE(octet_length(m.data_json),0)+COALESCE(octet_length(m.workspace_id),0) AS BIGINT) AS bytes FROM sessions s LEFT JOIN session_management m ON m.session_id=s.id WHERE s.id=$1").bind(&request.session_id.0).fetch_optional(&mut *tx).await.map_err(unknown)?.ok_or(Failure::Error(PublicSessionError::NotFound))?;
        let revision = SessionRevision::new(u64::try_from(row.try_get::<i64,_>("public_revision").map_err(unknown)?).map_err(unknown)?).map_err(unknown)?;
        if revision != request.expected_revision { tx.commit().await.map_err(unknown)?; return Ok(None); }
        let mut safe = Budget::new(request.max_bytes, PublicSessionLimit::HeaderBytes);
        safe.charge(usize::try_from(row.try_get::<i64,_>("bytes").map_err(unknown)?).map_err(unknown)?)?;
        let header = read(&mut tx, &request, revision, safe).await?;
        tx.commit().await.map_err(unknown)?;
        Ok(Some(header))
        }.await;
        match result {
            Ok(Some(header)) => Ok(PublicHeaderOutcome::Header(Box::new(header))),
            Ok(None) => Ok(PublicHeaderOutcome::Invalidated),
            Err(Failure::Limited(limit)) => Ok(PublicHeaderOutcome::Limited(limit)),
            Err(Failure::Error(error)) => Err(error),
        }
    }
}
async fn read(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
    mut safe: Budget,
) -> Result<PublicControlHeader, Failure> {
    let mut proof = Budget::new(request.max_proof_bytes, PublicSessionLimit::ProofBytes);
    let header = base(tx, request, revision, &mut safe, &mut proof).await?;
    budget::finish_header(header, safe, &proof)
}
pub(super) async fn base(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
    safe: &mut Budget,
    proof: &mut Budget,
) -> Result<PublicControlHeader, Failure> {
    let (mut header, status) = common(tx, request, revision, safe).await?;
    let (approval, barrier) =
        super::approval::public_header(tx, request, status, header.attempt.as_ref(), safe, proof)
            .await?;
    header.approval = approval;
    header.barrier = barrier;
    Ok(header)
}
pub(super) async fn common(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
    safe: &mut Budget,
) -> Result<(PublicControlHeader, Option<RunStatus>), Failure> {
    let (session, metadata, saved_revision) = super::management::current(tx, &request.session_id)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("session"))?;
    if saved_revision != revision {
        return Err(unknown("revision"));
    }
    let session = crate::management::summary(&session, metadata, revision);
    let row = sqlx::query("SELECT CAST(octet_length(id)+octet_length(status) AS BIGINT) AS bytes,CASE WHEN octet_length(id)<=128 AND octet_length(id)+octet_length(status)<=$2 THEN id END AS id,CASE WHEN octet_length(status)<=32 AND octet_length(id)+octet_length(status)<=$2 THEN status END AS status FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1").bind(&request.session_id.0).bind(i64::try_from(safe.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let status = if let Some(row) = row {
        safe.charge(
            usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?,
        )?;
        let id = row
            .try_get::<Option<String>, _>("id")
            .map_err(unknown)?
            .ok_or_else(|| unknown("run id"))?;
        if request.run_id.as_ref().map(|id| id.0.as_str()) != Some(id.as_str()) {
            return Err(unknown("latest run"));
        }
        Some(
            serde_json::from_value::<RunStatus>(serde_json::Value::String(
                row.try_get::<Option<String>, _>("status")
                    .map_err(unknown)?
                    .ok_or_else(|| unknown("run status"))?,
            ))
            .map_err(unknown)?,
        )
    } else {
        if request.run_id.is_some() {
            return Err(unknown("missing run"));
        }
        None
    };
    let attempt = attempt(tx, request, safe).await?;
    let contents = contents(tx, request, revision, safe).await?;

    Ok((
        PublicControlHeader {
            version: 1,
            revision,
            session,
            attempt,
            approval: None,
            barrier: None,
            contents,
            read_usage: PublicHeaderUsage::default(),
        },
        status,
    ))
}
async fn attempt(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequest,
    budget: &mut Budget,
) -> Result<Option<AttemptSnapshot>, Failure> {
    let Some(run) = &request.run_id else {
        return Ok(None);
    };
    let row = sqlx::query("WITH latest AS (SELECT json_build_object('session_id',$1,'run_id',a.run_id,'target',json_build_object('id',a.id,'generation',a.generation),'state',a.state,'cleanup',a.cleanup_state)::text AS data FROM execution_attempts a WHERE a.run_id=$2 ORDER BY a.generation DESC LIMIT 1) SELECT CASE WHEN octet_length(data)<=$3 THEN data END AS data FROM latest").bind(&request.session_id.0).bind(&run.0).bind(i64::try_from(budget.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let data = row
        .try_get::<Option<String>, _>("data")
        .map_err(unknown)?
        .ok_or(Failure::Limited(PublicSessionLimit::HeaderBytes))?;
    budget.charge(data.len())?;
    let attempt: AttemptSnapshot = serde_json::from_str(&data).map_err(unknown)?;
    if attempt.session_id != request.session_id
        || attempt.run_id != *run
        || attempt.target.id.0.is_empty()
        || attempt.target.id.0.len() > 128
        || attempt.target.id.0.chars().any(char::is_control)
    {
        return Err(unknown("attempt scope"));
    }
    Ok(Some(attempt))
}
async fn contents(
    tx: &mut Transaction<'_, Postgres>,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
    budget: &mut Budget,
) -> Result<crabber_core::PublicStateContents, Failure> {
    let row = sqlx::query("SELECT format_version::bigint AS format_version,CASE WHEN octet_length(state_json)+octet_length(activities_json)<=$2 THEN state_json END AS state_json,CASE WHEN octet_length(state_json)+octet_length(activities_json)<=$2 THEN activities_json END AS activities_json FROM session_public_state WHERE session_id=$1").bind(&request.session_id.0).bind(i64::try_from(budget.remaining()).map_err(unknown)?).fetch_optional(&mut **tx).await.map_err(unknown)?;
    let Some(row) = row else {
        return Ok(crabber_core::PublicStateContents::default());
    };
    let state = row
        .try_get::<Option<String>, _>("state_json")
        .map_err(unknown)?
        .ok_or(Failure::Limited(PublicSessionLimit::HeaderBytes))?;
    let activities = row
        .try_get::<Option<String>, _>("activities_json")
        .map_err(unknown)?
        .ok_or(Failure::Limited(PublicSessionLimit::HeaderBytes))?;
    budget.charge(
        state
            .len()
            .checked_add(activities.len())
            .ok_or_else(|| unknown("state bytes"))?,
    )?;
    Ok(crate::public_state::snapshot(
        &request.session_id,
        i64::try_from(revision.get()).map_err(unknown)?,
        Some(row.try_get::<i64, _>("format_version").map_err(unknown)?),
        Some(state),
        Some(activities),
    )
    .map_err(unknown)?
    .contents)
}

#[cfg(test)]
#[tokio::test]
async fn public_session_contract() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::public_session::tests::run(&store, &clock).await;
    let reopened = crate::PostgresStore::connect(&url).await.unwrap();
    crate::public_session::tests::cross_connection(&store, &reopened).await;
}

#[cfg(test)]
#[test]
fn oversized_approval_proof_is_not_transferred() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = runtime.block_on(crate::postgres::tests::TEST_LOCK.lock());
    runtime
        .block_on(crate::PostgresStore::migrate(&url))
        .unwrap();
    let store = runtime
        .block_on(crate::PostgresStore::connect(&url))
        .unwrap()
        .with_clock(clock.clone());
    let f = runtime.block_on(crate::approval_contract::fixture::staged(&store, &clock));
    runtime
        .block_on(f.execution.suspend_for_approval(f.batch.clone(), f.owner))
        .unwrap();
    let saved: String = runtime
        .block_on(
            sqlx::query_scalar("SELECT data_json FROM continuations WHERE id=$1")
                .bind(&f.batch.continuation.id.0)
                .fetch_one(&store.pool),
        )
        .unwrap();
    runtime
        .block_on(
            sqlx::query("UPDATE continuations SET data_json=$1 WHERE id=$2")
                .bind("private-carrier".repeat(100_000))
                .bind(&f.batch.continuation.id.0)
                .execute(&store.pool),
        )
        .unwrap();
    crate::public_session::allocation::proof_allocation(
        &runtime,
        &store,
        f.run.session_id,
        f.run.id,
    );
    runtime
        .block_on(
            sqlx::query("UPDATE continuations SET data_json=$1 WHERE id=$2")
                .bind(saved)
                .bind(&f.batch.continuation.id.0)
                .execute(&store.pool),
        )
        .unwrap();
}

mod v2;
