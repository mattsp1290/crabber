//! One native gate/transaction selects current authority and safe presentation.
use super::PostgresStore;
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::{AttemptAuthority, PublicStateError as Error};
use crabber_core::continuation_source::{AttemptIdentity, FrozenCall, ResumeSnapshot};
use crabber_core::{Part, RunFence, SessionId, ToolCallRecord};
use sqlx::{Postgres, Row};

impl PostgresStore {
    pub(super) async fn read_continuation_source(
        &self,
        fence: &RunFence,
        authority: &AttemptAuthority,
    ) -> Result<ResumeSnapshot, Error> {
        let mut tx = self
            .session_transaction(&authority.snapshot.session_id)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let result = read(self, &mut tx, fence, authority).await;
        match result {
            Ok(result) => {
                tx.commit().await.map_err(|_| Error::UnknownStoreOutcome)?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?;
                Err(error)
            }
        }
    }
}
async fn read(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    fence: &RunFence,
    authority: &AttemptAuthority,
) -> Result<ResumeSnapshot, Error> {
    let mut proof = Proof::bounded(32 << 20);
    proof.measure(fence).map_err(|_| Error::InvalidRequest)?;
    let run_bytes: i64 = sqlx::query_scalar("SELECT CAST(octet_length((data-'checkpoint'-'error')::text) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2")
        .bind(&authority.snapshot.session_id.0).bind(&fence.run_id.0).fetch_one(&mut **tx).await.map_err(|_| Error::NotFound)?;
    if run_bytes > 16384 {
        return Err(Error::InvalidRequest);
    }
    charge(&mut proof, run_bytes)?;
    let run = super::session_owner::run::load(tx, &authority.snapshot.session_id, &fence.run_id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::deletion::ensure_live(tx, &run.session_id)
        .await
        .map_err(|_| Error::NotFound)?;
    super::credential_access::check(tx, &run, &mut proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::credential_access::check_current(tx, &run, &mut proof)
        .await
        .map_err(|_| Error::StaleOwner)?;
    let (attempt, _) = super::attempt::records::load_with_proof(tx, &run.id, None, &mut proof)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::StaleOwner)?;
    crate::continuation_source::verify_owner(&run, fence, &attempt, authority, store.clock.now())?;
    let calls = calls(tx, &run.session_id, &run.id, &mut proof).await?;
    let state = snapshot(tx, &run.session_id, &mut proof).await?;
    proof
        .measure(&(&run.session_id, &run.id, &attempt.snapshot.target))
        .map_err(|_| Error::InvalidRequest)?;
    let result = ResumeSnapshot {
        session_id: run.session_id,
        run_id: run.id,
        attempt: AttemptIdentity {
            id: attempt.snapshot.target.id,
            generation: attempt.snapshot.target.generation,
        },
        revision: state.revision,
        contents: state.contents,
        calls,
    };
    proof.measure(&result).map_err(|_| Error::InvalidRequest)?;
    Ok(result)
}
async fn calls(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<Vec<FrozenCall>, Error> {
    let cost = sqlx::query("SELECT count(*) AS count, COALESCE(sum(octet_length(data::text)+octet_length(id)+octet_length(run_id)+octet_length(status)),0) AS bytes FROM tool_calls WHERE (run_id=$1 OR data->>'run_id'=$1) AND (status IN ('pending','running','waiting') OR data->>'status' IN ('pending','running','waiting'))")
        .bind(&run.0).fetch_one(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    let count: i64 = cost
        .try_get("count")
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if !(0..=64).contains(&count) {
        return Err(Error::InvalidRequest);
    }
    charge(
        proof,
        cost.try_get("bytes")
            .map_err(|_| Error::UnknownStoreOutcome)?,
    )?;
    let rows = sqlx::query("SELECT id,run_id,status,data::text AS data FROM tool_calls WHERE (run_id=$1 OR data->>'run_id'=$1) AND (status IN ('pending','running','waiting') OR data->>'status' IN ('pending','running','waiting')) ORDER BY id LIMIT 65")
        .bind(&run.0).fetch_all(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    if i64::try_from(rows.len()).ok() != Some(count) {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut result = Vec::new();
    for row in rows {
        let text: String = row
            .try_get("data")
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let call: ToolCallRecord =
            serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?;
        if row
            .try_get::<String, _>("id")
            .map_err(|_| Error::UnknownStoreOutcome)?
            != call.id.0
            || row
                .try_get::<String, _>("run_id")
                .map_err(|_| Error::UnknownStoreOutcome)?
                != run.0
            || serde_json::to_value(call.status)
                .map_err(|_| Error::UnknownStoreOutcome)?
                .as_str()
                != Some(
                    row.try_get::<String, _>("status")
                        .map_err(|_| Error::UnknownStoreOutcome)?
                        .as_str(),
                )
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result.push(parent(tx, session, run, &call, proof).await?);
    }
    Ok(result)
}
async fn parent(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &crabber_core::RunId,
    call: &ToolCallRecord,
    proof: &mut Proof,
) -> Result<FrozenCall, Error> {
    let cost = sqlx::query("SELECT count(*) AS count, COALESCE(sum(octet_length(p.data::text)+octet_length(m.id)+octet_length(m.session_id)+octet_length(m.run_id)+octet_length(p.id)),0) AS bytes FROM parts p JOIN messages m ON m.id=p.message_id WHERE p.data#>>'{content,call_id}'=$2 AND m.session_id=$1")
        .bind(&session.0).bind(&call.id.0).fetch_one(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    if cost
        .try_get::<i64, _>("count")
        .map_err(|_| Error::UnknownStoreOutcome)?
        != 1
    {
        return Err(Error::UnknownStoreOutcome);
    }
    charge(
        proof,
        cost.try_get("bytes")
            .map_err(|_| Error::UnknownStoreOutcome)?,
    )?;
    let row = sqlx::query("SELECT p.data::text AS data,p.id,p.ordinal::bigint AS ordinal,m.id AS message_id,m.run_id,m.data->>'role' AS role,(m.data->>'id'=m.id AND m.data->>'session_id'=m.session_id AND m.data->>'run_id'=m.run_id) AS valid FROM parts p JOIN messages m ON m.id=p.message_id WHERE p.data#>>'{content,call_id}'=$2 AND m.session_id=$1 LIMIT 2")
        .bind(&session.0).bind(&call.id.0).fetch_one(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    let text: String = row
        .try_get("data")
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let part: Part = serde_json::from_str(&text).map_err(|_| Error::UnknownStoreOutcome)?;
    let message_id = crabber_core::MessageId(
        row.try_get("message_id")
            .map_err(|_| Error::UnknownStoreOutcome)?,
    );
    if row
        .try_get::<Option<bool>, _>("valid")
        .map_err(|_| Error::UnknownStoreOutcome)?
        != Some(true)
        || row
            .try_get::<Option<String>, _>("run_id")
            .map_err(|_| Error::UnknownStoreOutcome)?
            .as_deref()
            != Some(run.0.as_str())
        || row
            .try_get::<String, _>("role")
            .map_err(|_| Error::UnknownStoreOutcome)?
            != "assistant"
        || row
            .try_get::<String, _>("id")
            .map_err(|_| Error::UnknownStoreOutcome)?
            != part.id.0
        || row
            .try_get::<i64, _>("ordinal")
            .map_err(|_| Error::UnknownStoreOutcome)?
            != i64::from(part.ordinal)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::continuation_source::frozen_call(session, run, call, &message_id, &part, proof)
}
async fn snapshot(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<crate::PublicStateSnapshot, Error> {
    let bytes: i64 = sqlx::query_scalar("SELECT CAST(COALESCE(octet_length(p.state_json)+octet_length(p.activities_json),0) AS BIGINT) FROM sessions s LEFT JOIN session_public_state p ON p.session_id=s.id WHERE s.id=$1")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    charge(proof, bytes)?;
    let row = sqlx::query("SELECT s.public_revision,p.format_version::bigint AS format_version,p.state_json,p.activities_json FROM sessions s LEFT JOIN session_public_state p ON p.session_id=s.id WHERE s.id=$1")
        .bind(&session.0).fetch_one(&mut **tx).await.map_err(|_| Error::UnknownStoreOutcome)?;
    crate::public_state::snapshot(
        session,
        row.try_get("public_revision")
            .map_err(|_| Error::UnknownStoreOutcome)?,
        row.try_get("format_version")
            .map_err(|_| Error::UnknownStoreOutcome)?,
        row.try_get("state_json")
            .map_err(|_| Error::UnknownStoreOutcome)?,
        row.try_get("activities_json")
            .map_err(|_| Error::UnknownStoreOutcome)?,
    )
}
fn charge(proof: &mut Proof, bytes: i64) -> Result<(), Error> {
    let bytes = usize::try_from(bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(3))
        .ok_or(Error::InvalidRequest)?;
    proof.charge(bytes).map_err(|_| Error::InvalidRequest)
}
#[cfg(test)]
#[tokio::test]
async fn continuation_source_exact_native_parent_and_foreign_authority() {
    let Some(url) = super::tests::test_url() else {
        return;
    };
    let _guard = super::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::continuation_source::tests::run(&store).await;
    crate::continuation_source::tests::waiting_atomic(&store, &clock, &store.approval_fault).await;
    crate::continuation_source::tests::tool_waiting_atomic(&store, &store.tool_continuation_fault)
        .await;
}

// Called only inside the suspension compound; its receipt revision covers this
// extra safe event as well as the existing RunPaused/audit writes.
pub(in crate::postgres) async fn append_waiting(
    tx: &mut Transaction<'_, Postgres>,
    event: &crabber_core::EventRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    proof.measure(event).map_err(|_| Error::InvalidRequest)?;
    let text = serde_json::to_string(event).map_err(|_| Error::InvalidRequest)?;
    proof
        .charge(text.len())
        .map_err(|_| Error::InvalidRequest)?;
    sqlx::query("INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3)")
        .bind(&event.session_id.0)
        .bind(&event.run_id.0)
        .bind(sqlx::types::Json(event))
        .execute(&mut **tx)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(())
}
