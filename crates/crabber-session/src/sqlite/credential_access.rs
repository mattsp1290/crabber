//! Raw credential gates; lists return all selected credentials or none.
//! One read snapshot preserves lock-free reads. Immutable Raw/Legacy cohorts
//! are never adopted; every unsupported Pending origin refuses before return.
use super::{Sqlite, SqliteStore, Transaction, db, native_enrollment, records};
use crate::StoreError;
use crate::native_enrollment::access::{allow, failure};
use crate::public_session::v2::proof::Proof;
use crabber_core::{Run, RunFence, RunId};
use sqlx::Row;

#[cfg(test)]
#[path = "credential_access/regression_tests.rs"]
mod regression_tests;

pub(super) async fn check(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    proof.measure(run).map_err(failure)?;
    check_origin(tx, &run.session_id, &run.id, proof).await
}

/// Credential acquisition and cached writes authenticate the current issuer
/// for native Runs. Genuine pre-protocol legacy rows keep their old behavior.
pub(super) async fn check_current(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    proof.measure(run).map_err(failure)?;
    super::native_parent::source_protocol::check_raw_current(tx, run, proof).await
}

/// Legacy writer guard; its API has no enclosing operation Proof.
pub(super) async fn check_bounded(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
) -> Result<(), StoreError> {
    check(tx, run, &mut Proof::bounded(32 << 20)).await
}

pub(super) async fn check_origin(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    allow(
        native_enrollment::original(tx, session, run, proof)
            .await
            .map_err(failure)?,
    )
}

impl SqliteStore {
    pub(super) async fn raw_execution_access(&self, fence: &RunFence) -> Result<(), StoreError> {
        let mut tx = crate::transaction::begin(&self.readers).await.map_err(db)?;
        let result = async {
            let mut proof = Proof::bounded(32 << 20);
            proof.measure(fence).map_err(failure)?;
            let run = records::load_run_with_proof(&mut tx, &fence.run_id, &mut proof).await?;
            if run.id != fence.run_id {
                return Err(StoreError::Validation(
                    "stored run identity is invalid".into(),
                ));
            }
            if run.claim_token != fence.claim_token
                || run.status.is_terminal()
                || run.lease_until <= self.clock.now()
            {
                return Err(StoreError::Conflict);
            }
            check_current(&mut tx, &run, &mut proof).await
        }
        .await;
        super::transactions::finish(tx, result, std::convert::identity).await
    }

    pub(super) async fn raw_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        let mut tx = crate::transaction::begin(&self.readers).await.map_err(db)?;
        let result = async {
            let mut proof = Proof::bounded(32 << 20);
            let run = match records::load_run_with_proof(&mut tx, id, &mut proof).await {
                Ok(run) => run,
                Err(StoreError::NotFound) => return Ok(None),
                Err(error) => return Err(error),
            };
            if run.id != *id {
                return Err(StoreError::Validation(
                    "stored run identity is invalid".into(),
                ));
            }
            check(&mut tx, &run, &mut proof).await?;
            Ok(Some(run))
        }
        .await;
        super::transactions::finish(tx, result, std::convert::identity).await
    }

    pub(super) async fn raw_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        let mut tx = crate::transaction::begin(&self.readers).await.map_err(db)?;
        let result = async {
            let mut proof = Proof::bounded(32 << 20);
            super::read_proof::charge(&mut tx, sqlx::query_scalar(
                "SELECT SUM(length(CAST(data AS BLOB))+length(CAST(id AS BLOB))+length(CAST(session_id AS BLOB))) FROM runs WHERE status IN ('pending','running','paused')"
            ), &mut proof).await.map_err(failure)?;
            let rows = sqlx::query("SELECT id,session_id,data FROM runs WHERE status IN ('pending','running','paused') ORDER BY seq")
                .fetch_all(&mut *tx).await.map_err(db)?;
            let mut runs = Vec::with_capacity(rows.len());
            for row in rows {
                let run: Run = records::decode_row(&row, "data")?;
                if run.id.0 != row.try_get::<String, _>("id").map_err(db)?
                    || run.session_id.0 != row.try_get::<String, _>("session_id").map_err(db)?
                    || run.status.is_terminal()
                {
                    return Err(StoreError::Validation("stored run identity is invalid".into()));
                }
                check(&mut tx, &run, &mut proof).await?;
                runs.push(run);
            }
            Ok(runs)
        }.await;
        super::transactions::finish(tx, result, std::convert::identity).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use crate::native_enrollment::access::contract;

    async fn replace(store: &SqliteStore, run: &RunId, value: &str) {
        let mut tx = store.begin_write().await.unwrap();
        let trigger: String = sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='execution_unclassified_update'").fetch_one(&mut *tx).await.unwrap();
        sqlx::query("DROP TRIGGER execution_unclassified_update")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE native_execution_unclassified SET data=$2 WHERE run_id=$1")
            .bind(&run.0)
            .bind(value)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(&trigger).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }

    #[async_trait::async_trait]
    impl crate::driver_raw_access_contract::Damage for crate::SqliteStore {
        type Saved = String;
        async fn damage(&self, _: &crabber_core::SessionId, run: &RunId) -> Self::Saved {
            let saved = sqlx::query_scalar(
                "SELECT data FROM native_execution_unclassified WHERE run_id=$1",
            )
            .bind(&run.0)
            .fetch_one(&self.readers)
            .await
            .unwrap();
            replace(self, run, "{}").await;
            saved
        }
        async fn restore(&self, _: &crabber_core::SessionId, run: &RunId, saved: Self::Saved) {
            replace(self, run, &saved).await;
        }
    }
    #[tokio::test]
    async fn raw_driver_registration_and_activation_authenticate_birth() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("raw-driver.sqlite");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = crate::SqliteStore::connect(&path)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::driver_raw_access_contract::run(&store, &clock).await;
    }
    #[tokio::test]
    async fn public_raw_credentials_require_original_enrollment_before_return() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credential-access.sqlite");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = crate::SqliteStore::connect(&path).await.unwrap();
        let admitted = contract::admit(&store).await;
        let _other_scope = contract::admit(&store).await;
        contract::succeeds(&store, &admitted).await;
        let read = store.get_run(&admitted.run.id);
        let list = store.list_unfinished_runs();
        let acquire = store.execution(admitted.fence.clone());
        let original: String =
            sqlx::query_scalar("SELECT data FROM native_execution_unclassified WHERE run_id=$1")
                .bind(&admitted.run.id.0)
                .fetch_one(&store.readers)
                .await
                .unwrap();
        replace(&store, &admitted.run.id, "{}").await;
        // These futures were constructed before corruption and first polled after.
        // No individual credential or partial multi-scope list may escape.
        let refused = (
            read.await.is_err(),
            list.await.is_err(),
            acquire.await.is_err(),
        );
        replace(&store, &admitted.run.id, &original).await;
        contract::succeeds(&store, &admitted).await;
        assert_eq!(refused, (true, true, true));
    }
    #[tokio::test]
    async fn wrong_row_cannot_borrow_other_runs_raw_origin() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("credential-routing.sqlite");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = crate::SqliteStore::connect(&path).await.unwrap();
        let first = contract::admit(&store).await;
        let other = contract::admit(&store).await;
        let forged_route = RunFence {
            run_id: first.run.id.clone(),
            claim_token: other.run.claim_token.clone(),
        };
        sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
            .bind(&first.run.id.0)
            .bind(serde_json::to_string(&other.run).unwrap())
            .execute(&store.writer)
            .await
            .unwrap();
        let refused = store.execution(forged_route).await.is_err();
        sqlx::query("UPDATE runs SET data=$2 WHERE id=$1")
            .bind(&first.run.id.0)
            .bind(serde_json::to_string(&first.run).unwrap())
            .execute(&store.writer)
            .await
            .unwrap();
        contract::succeeds(&store, &first).await;
        assert!(refused, "a row cannot borrow a distinct genuine Run origin");
    }
    #[tokio::test]
    async fn genuine_pending_birth_refuses_copied_cached_raw_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pending-cache.sqlite");
        crate::SqliteStore::migrate(&path).await.unwrap();
        let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
        let session = contract::pending(store.clone()).await;
        let data: String = sqlx::query_scalar("SELECT r.data FROM runs r JOIN native_execution_origins o ON o.run_id=r.id WHERE o.birth_kind='pending' AND r.session_id=$1")
            .bind(&session.0).fetch_one(&store.readers).await.unwrap();
        let run: Run = serde_json::from_str(&data).unwrap();
        let cached = crate::sqlite::SqliteExecution {
            store: store.as_ref().clone(),
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
        };
        contract::cached_mutations(store.as_ref(), &run, &cached).await;
        let after: String = sqlx::query_scalar("SELECT r.data FROM runs r JOIN native_execution_origins o ON o.run_id=r.id WHERE o.birth_kind='pending' AND r.session_id=$1")
            .bind(&session.0).fetch_one(&store.readers).await.unwrap();
        assert_eq!(after, data);
    }
}
