use super::records::{bump_public_revision, decode_row};
use super::{SqliteExecution, SqliteStore, admission_execution, db};
use crate::transaction::Transaction;
use crate::{
    PublicStateError as Error, PublicStateMutation, PublicStateReceipt, PublicStateSnapshot,
    public_state,
};
use crabber_core::{ControlId, EventCursor, InputFingerprint, Run, SessionId, SessionRevision};
use sqlx::{Row, Sqlite};
use time::OffsetDateTime;

const UPSERT_STATE: &str = "INSERT INTO session_public_state(session_id,format_version,state_json,activities_json) VALUES($1,1,$2,$3) ON CONFLICT(session_id) DO UPDATE SET format_version=1,state_json=excluded.state_json,activities_json=excluded.activities_json";
const INSERT_RECEIPT: &str = "INSERT INTO operation_receipts(session_id,operation_id,kind,semantic_digest,event_seq,data_json) VALUES($1,$2,'public_state',$3,$4,$5)";
const SNAPSHOT: &str = "SELECT s.public_revision,p.format_version AS format_version, CASE WHEN length(CAST(p.state_json AS BLOB))+length(CAST(p.activities_json AS BLOB))<=1048576 THEN p.state_json END AS state_json, CASE WHEN length(CAST(p.state_json AS BLOB))+length(CAST(p.activities_json AS BLOB))<=1048576 THEN p.activities_json END AS activities_json FROM sessions s LEFT JOIN session_public_state p ON p.session_id=s.id WHERE s.id=$1";
const RECEIPT: &str = "SELECT r.kind,r.semantic_digest,r.event_seq,e.run_id AS event_run_id,CASE WHEN length(CAST(r.data_json AS BLOB))<=4096 THEN r.data_json END AS data_json, CASE WHEN r.kind='public_state' AND length(CAST(e.data AS BLOB))<=4194304 THEN e.data END AS event_data FROM operation_receipts r LEFT JOIN events e ON e.seq=r.event_seq AND e.session_id=r.session_id WHERE r.session_id=$1 AND r.operation_id=$2";
async fn snapshot(
    tx: &mut Transaction<'_, Sqlite>,
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
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    id: &ControlId,
) -> Result<Option<PublicStateReceipt>, Error> {
    sqlx::query(RECEIPT)
        .bind(&session.0)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)
        .map_err(public_state::unknown)?
        .map(|row| {
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
            )?;
            let event = serde_json::from_str::<crabber_core::EventRecord>(
                &row.get::<Option<String>, _>("event_data")
                    .ok_or(Error::UnknownStoreOutcome)?,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
            if row.get::<Option<String>, _>("event_run_id").as_deref() != Some(&receipt.run_id.0) {
                return Err(Error::UnknownStoreOutcome);
            }
            public_state::verify_receipt_event(&receipt, &event)?;
            Ok(receipt)
        })
        .transpose()
}
async fn finish<T>(tx: Transaction<'_, Sqlite>, result: Result<T, Error>) -> Result<T, Error> {
    super::transactions::finish(tx, result, public_state::unknown).await
}
impl SqliteStore {
    pub(super) async fn read_public_state(
        &self,
        session: &SessionId,
    ) -> Result<PublicStateSnapshot, Error> {
        let mut tx = crate::transaction::begin(&self.readers)
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
        let mut tx = crate::transaction::begin(&self.readers)
            .await
            .map_err(db)
            .map_err(public_state::unknown)?;
        let result = receipt(&mut tx, session, id).await;
        finish(tx, result).await
    }
}
impl SqliteExecution {
    pub(super) async fn change_public_state(
        &self,
        mutation: PublicStateMutation,
    ) -> Result<PublicStateReceipt, Error> {
        public_state::validate_id(&mutation.id)?;
        let mut tx = self
            .store
            .begin_write()
            .await
            .map_err(public_state::unknown)?;
        let result = async {
            let row = sqlx::query("SELECT session_id,data FROM runs WHERE id=$1")
                .bind(&self.fence.run_id.0)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)
                .map_err(public_state::unknown)?
                .ok_or(Error::NotFound)?;
            let run: crabber_core::Run = decode_row(&row, "data").map_err(public_state::unknown)?;
            if run.id != self.fence.run_id || run.session_id.0 != row.get::<String, _>("session_id")
            {
                return Err(Error::UnknownStoreOutcome);
            }

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
    tx: &mut Transaction<'_, Sqlite>,
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
    .bind(serde_json::to_string(&event).map_err(|_| Error::InvalidRequest)?)
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
    tx: &mut Transaction<'_, Sqlite>,
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
        let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
        let admitted =
            crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
        sqlx::query("UPDATE sessions SET public_revision=$1 WHERE id=$2")
            .bind(i64::MAX)
            .bind(&admitted.session.id.0)
            .execute(&store.writer)
            .await
            .unwrap();
        crate::public_state_contract::exhausted(&store, admitted).await;
    }
    #[tokio::test]
    async fn process_helper() {
        let Some(fixture) = process::fixture() else {
            return;
        };
        let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
        if let Ok(marker) = std::env::var("CRABBER_PUBLIC_STATE_BARRIER") {
            let store = crate::sqlite::test_support::connect_with_test_options(
                std::path::Path::new(&path),
                crate::sqlite::test_support::TestOptions {
                    barrier: Some(marker.into()),
                    ..Default::default()
                },
            )
            .await;
            process::write(&store, &fixture).await;
            return;
        }
        let store = super::SqliteStore::connect(std::path::Path::new(&path))
            .await
            .unwrap();
        process::verify(&store, fixture).await;
    }
    #[tokio::test]
    async fn killed_writer_before_commit_rolls_back_state_event_revision_and_receipt() {
        use crate::sqlite::test_support;
        let _guard = test_support::BARRIER_LOCK.lock().await;
        let (store, path, directory) = crate::sqlite::tests::temp_store().await;
        let fixture = process::pending(&store).await;
        let marker = directory.path().join("state.marker");
        sqlx::query("CREATE TRIGGER state_barrier AFTER INSERT ON operation_receipts BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END")
            .execute(&store.writer).await.unwrap();
        let mut child = test_support::child(
            &path,
            "sqlite::public_state::tests::process_helper",
            &[
                (
                    "CRABBER_PUBLIC_STATE_CHILD",
                    serde_json::to_string(&fixture).unwrap(),
                ),
                (
                    "CRABBER_PUBLIC_STATE_BARRIER",
                    marker.to_str().unwrap().into(),
                ),
            ],
        );
        test_support::marker(&marker, &mut child).await;
        process::uncommitted(&store, &fixture).await;
        child.kill();
        sqlx::query("DROP TRIGGER state_barrier")
            .execute(&store.writer)
            .await
            .unwrap();
        process::uncommitted(&store, &fixture).await;
        process::write(&store, &fixture).await;
        process::run(
            "sqlite::public_state::tests::process_helper",
            &fixture,
            Some(&path),
        )
        .await;
    }
    #[tokio::test]
    async fn committed_response_loss_survives_fresh_process() {
        let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
        let fixture = process::prepare(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        process::run(
            "sqlite::public_state::tests::process_helper",
            &fixture,
            Some(&path),
        )
        .await;
    }
    #[tokio::test]
    async fn public_state_fault_contract() {
        let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
        crate::public_state_contract::run_fault_contract(&store, |point| {
            store
                .public_state_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
