mod source;
use super::{PostgresExecution, PostgresStore, admission_execution, bump_public_revision, db};
use crate::postgres::transaction::Transaction;
use crate::{
    PublicStateError as Error, PublicStateMutation, PublicStateReceipt, PublicStateSnapshot,
    public_state,
};
use crabber_core::{ControlId, EventCursor, InputFingerprint, Run, SessionId, SessionRevision};
use sqlx::{Postgres, Row};
use time::OffsetDateTime;

const UPSERT_STATE: &str = "INSERT INTO session_public_state(session_id,format_version,state_json,activities_json) VALUES($1,1,$2,$3) ON CONFLICT(session_id) DO UPDATE SET format_version=1,state_json=excluded.state_json,activities_json=excluded.activities_json";
const INSERT_RECEIPT: &str = "INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'public_state',$3,$4,$5)";
const SNAPSHOT: &str = "SELECT s.public_revision,p.format_version::bigint AS format_version, CASE WHEN octet_length(p.state_json)+octet_length(p.activities_json)<=1048576 THEN p.state_json END AS state_json, CASE WHEN octet_length(p.state_json)+octet_length(p.activities_json)<=1048576 THEN p.activities_json END AS activities_json FROM sessions s LEFT JOIN session_public_state p ON p.session_id=s.id WHERE s.id=$1";
const RECEIPT: &str = "SELECT r.kind,r.semantic_digest,r.event_seq,e.run_id AS event_run_id,CASE WHEN octet_length(r.data_json)<=4096 THEN r.data_json END AS data_json, CASE WHEN r.kind='public_state' AND octet_length(e.data::text)<=4194304 THEN e.data END AS event_data FROM operation_receipts r LEFT JOIN events e ON e.seq=r.event_seq AND e.session_id=r.session_id WHERE r.session_id=$1 AND r.operation_id=$2";
async fn snapshot(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
) -> Result<PublicStateSnapshot, Error> {
    let row = sqlx::query(SNAPSHOT)
        .bind(&session.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?
        .ok_or(Error::NotFound)?;
    public_state::snapshot(
        session,
        row.get("public_revision"),
        row.get("format_version"),
        row.get("state_json"),
        row.get("activities_json"),
    )
}
async fn receipt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<PublicStateReceipt>, Error> {
    receipt_with_proof(
        tx,
        session,
        id,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await
}
async fn receipt_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    id: &ControlId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<PublicStateReceipt>, Error> {
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(r.data_json)+octet_length(r.semantic_digest)+COALESCE(octet_length(e.data::text),0)+64 AS BIGINT) FROM operation_receipts r LEFT JOIN events e ON e.seq=r.event_seq AND e.session_id=r.session_id WHERE r.session_id=$1 AND r.operation_id=$2").bind(&session.0).bind(&id.0),proof).await.map_err(|_|Error::UnknownStoreOutcome)?;
    let row = sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?;
    let Some(row) = row else {
        return Ok(None);
    };
    {
        let kind: String = row.get("kind");
        if kind != "public_state" {
            return Err(Error::SemanticConflict);
        }
        let receipt = public_state::receipt(
            session,
            id,
            &kind,
            &row.get::<String, _>("semantic_digest"),
            row.get("event_seq"),
            row.get("data_json"),
            proof,
        )?;
        let event = crate::public_state::decode_source_event(
            row.get::<Option<sqlx::types::Json<serde_json::Value>>, _>("event_data")
                .ok_or(Error::UnknownStoreOutcome)?,
            proof,
        )?;
        if row.get::<Option<String>, _>("event_run_id").as_deref() != Some(&receipt.run_id.0) {
            return Err(Error::UnknownStoreOutcome);
        }
        if event
            .payload
            .get("version")
            .and_then(serde_json::Value::as_u64)
            == Some(2)
        {
            public_state::verify_source_event_with_proof(&receipt, &event, proof)?;
        } else {
            public_state::verify_receipt_event(&receipt, &event)?;
        }
        source::verify_before(tx, &event, proof).await?;
        Ok(Some(receipt))
    }
}
async fn finish<T>(tx: Transaction<'_, Postgres>, result: Result<T, Error>) -> Result<T, Error> {
    match result {
        Ok(value) => {
            tx.commit()
                .await
                .map_err(db)
                .map_err(public_state::unknown)?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback()
                .await
                .map_err(db)
                .map_err(public_state::unknown)?;
            Err(error)
        }
    }
}
impl PostgresStore {
    pub(super) async fn read_public_state(
        &self,
        session: &SessionId,
    ) -> Result<PublicStateSnapshot, Error> {
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)
            .map_err(public_state::unknown)?;
        let result = snapshot(&mut tx, session).await;
        finish(tx, result).await
    }
    pub(super) async fn public_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PublicStateReceipt>, Error> {
        public_state::validate_id(id)?;
        let mut tx = crate::postgres::transaction::begin(&self.pool)
            .await
            .map_err(db)
            .map_err(public_state::unknown)?;
        let result = receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl PostgresExecution {
    pub(super) async fn change_public_state(
        &self,
        mutation: PublicStateMutation,
    ) -> Result<PublicStateReceipt, Error> {
        public_state::validate_id(&mutation.id)?;
        let (mut tx, run) = self
            .store
            .run_transaction(&self.fence.run_id)
            .await
            .map_err(|error| {
                if error == crate::StoreError::NotFound {
                    Error::NotFound
                } else {
                    public_state::unknown(error)
                }
            })?;
        let result = async {
            super::credential_access::check(
                &mut tx,
                &run,
                &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
            )
            .await
            .map_err(public_state::unknown)?;
            let digest = mutation.fingerprint(&run.session_id, &run.id)?;
            if let Some(receipt) = receipt(&mut tx, &run.session_id, &mutation.id).await? {
                if receipt.run_id != run.id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(receipt);
            }
            public_state::verify_owner(&run, &self.fence, self.store.clock.now())?;
            if admission_execution::load_record(&mut tx, &run.id)
                .await
                .map_err(public_state::unknown)?
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(Error::StaleOwner);
            }
            let current = snapshot(&mut tx, &run.session_id).await?;
            if current.revision != mutation.expected_revision {
                return Err(Error::RevisionConflict);
            }
            public_state::validate_fresh_activities(&mutation.contents)?;
            let revision = current
                .revision
                .next()
                .map_err(|_| Error::RevisionExhausted)?;
            let receipt = persist(
                &mut tx,
                &run,
                &mutation,
                revision,
                digest,
                self.store.clock.now(),
            )
            .await?;
            #[cfg(test)]
            if self
                .store
                .public_state_fault
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                return Err(Error::UnknownStoreOutcome);
            }
            record_receipt(&mut tx, &receipt).await?;
            Ok(receipt)
        }
        .await;
        let result = finish(tx, result).await;
        #[cfg(test)]
        if self
            .store
            .public_state_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && result.is_ok()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}

async fn persist(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    mutation: &PublicStateMutation,
    revision: SessionRevision,
    digest: InputFingerprint,
    now: OffsetDateTime,
) -> Result<PublicStateReceipt, Error> {
    let state =
        serde_json::to_string(mutation.contents.state()).map_err(|_| Error::InvalidRequest)?;
    let activities =
        serde_json::to_string(mutation.contents.activities()).map_err(|_| Error::InvalidRequest)?;
    sqlx::query(UPSERT_STATE)
        .bind(&run.session_id.0)
        .bind(state)
        .bind(activities)
        .execute(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?;
    let event = public_state::event(run, mutation, revision, now);
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO events(session_id,run_id,data) VALUES($1,$2,$3) RETURNING seq",
    )
    .bind(&run.session_id.0)
    .bind(&run.id.0)
    .bind(public_state::encode_event(&event).map_err(public_state::unknown)?)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(public_state::unknown)?;
    bump_public_revision(tx, &run.session_id)
        .await
        .map_err(public_state::unknown)?;
    let receipt = PublicStateReceipt {
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        operation_id: mutation.id.clone(),
        semantic_digest: digest,
        revision,
        event_cursor: EventCursor(u64::try_from(seq).map_err(|_| Error::UnknownStoreOutcome)?),
    };
    Ok(receipt)
}
async fn record_receipt(
    tx: &mut Transaction<'_, Postgres>,
    receipt: &PublicStateReceipt,
) -> Result<(), Error> {
    sqlx::query(INSERT_RECEIPT)
        .bind(&receipt.session_id.0)
        .bind(&receipt.operation_id.0)
        .bind(receipt.semantic_digest.as_str())
        .bind(i64::try_from(receipt.event_cursor.0).map_err(|_| Error::UnknownStoreOutcome)?)
        .bind(serde_json::to_string(receipt).map_err(|_| Error::InvalidRequest)?)
        .execute(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::public_state_contract::process;
    #[tokio::test]
    async fn exhausted_revision_leaves_no_partial_write() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url).await.unwrap();
        let admitted =
            crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
        sqlx::query("UPDATE sessions SET public_revision=$1 WHERE id=$2")
            .bind(i64::MAX)
            .bind(&admitted.session.id.0)
            .execute(&store.pool)
            .await
            .unwrap();
        crate::public_state_contract::exhausted(&store, admitted).await;
    }
    #[tokio::test]
    async fn process_helper() {
        let Some(fixture) = process::fixture() else {
            return;
        };
        let url = crate::postgres::tests::test_url().expect("child database required");
        let store = super::PostgresStore::connect(&url).await.unwrap();
        if std::env::var_os("CRABBER_PUBLIC_STATE_WRITE").is_some() {
            process::write(&store, &fixture).await;
            return;
        }
        process::verify(&store, fixture).await;
    }
    #[tokio::test]
    async fn killed_writer_before_commit_rolls_back_state_event_revision_and_receipt() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url).await.unwrap();
        let fixture = process::pending(&store).await;
        let function = format!("state_barrier_{}", uuid::Uuid::new_v4().simple());
        // Identities are generated UUIDs; this test-only function has no caller SQL.
        sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id='{}' THEN PERFORM pg_advisory_xact_lock(751302922); END IF; RETURN NEW; END $$", fixture.session.0))
            .execute(&store.pool).await.unwrap();
        sqlx::query(&format!("CREATE TRIGGER {function} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {function}()"))
            .execute(&store.pool).await.unwrap();
        let mut held = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(751302922)")
            .execute(&mut *held)
            .await
            .unwrap();
        let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *held)
            .await
            .unwrap();
        let mut child = process::spawn(
            "postgres::public_state::tests::process_helper",
            &fixture,
            None,
            &[("CRABBER_PUBLIC_STATE_WRITE", "1".into())],
        );
        let waiter = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'")
                    .bind(blocker).fetch_optional(&store.pool).await.unwrap();
                if let Some(pid) = pid { break pid; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.expect("writer must reach receipt precommit barrier");
        process::uncommitted(&store, &fixture).await;
        child.kill();
        // Release server-side work too; a disconnected blocked backend can
        // otherwise remain alive until its statement wakes up.
        sqlx::query("SELECT pg_terminate_backend($1)")
            .bind(waiter)
            .execute(&store.pool)
            .await
            .unwrap();
        held.rollback().await.unwrap();
        sqlx::query(&format!("DROP TRIGGER {function} ON operation_receipts"))
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query(&format!("DROP FUNCTION {function}()"))
            .execute(&store.pool)
            .await
            .unwrap();
        process::uncommitted(&store, &fixture).await;
        process::write(&store, &fixture).await;
        process::run(
            "postgres::public_state::tests::process_helper",
            &fixture,
            None,
        )
        .await;
    }
    #[tokio::test]
    async fn committed_response_loss_survives_fresh_process() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url).await.unwrap();
        let fixture = process::prepare(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        process::run(
            "postgres::public_state::tests::process_helper",
            &fixture,
            None,
        )
        .await;
    }
    #[tokio::test]
    async fn public_state_fault_contract() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url).await.unwrap();
        crate::public_state_contract::run_fault_contract(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
    #[tokio::test]
    async fn native_public_source_contract() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock);
        crate::public_state_contract::source::run(&store).await;
    }
    #[tokio::test]
    async fn native_public_source_fault_contract() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        super::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock);
        crate::public_state_contract::source::faults(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
