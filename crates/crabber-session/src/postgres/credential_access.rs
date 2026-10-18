//! Public Raw credentials; no ambient Driver promotion or enrolled transport.
use super::{Postgres, PostgresStore, Transaction, db, native_enrollment};
use crate::StoreError;
use crate::native_enrollment::access::{allow, failure};
use crate::public_session::v2::proof::Proof;
use crabber_core::{Run, RunFence, RunId, SessionId};
use sqlx::Row;

#[cfg(test)]
#[path = "credential_access/regression_tests.rs"]
mod regression_tests;

pub(super) async fn check(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    proof.measure(run).map_err(failure)?;
    check_origin(tx, &run.session_id, &run.id, proof).await
}

/// Credential acquisition and cached writes authenticate the current issuer
/// for native Runs. Genuine pre-protocol legacy rows keep their old behavior.
pub(super) async fn check_current(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    proof.measure(run).map_err(failure)?;
    match native_enrollment::original(tx, &run.session_id, &run.id, proof)
        .await
        .map_err(failure)?
    {
        crate::native_enrollment::Status::Legacy => Ok(()),
        crate::native_enrollment::Status::Unclassified => {
            super::native_parent::current_issuance_with_proof(tx, run, proof)
                .await
                .map(|_| ())
                .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()))
        }
        crate::native_enrollment::Status::Pending => Err(StoreError::HostUnsupported),
    }
}

/// Legacy writer guard; its API has no enclosing operation Proof.
pub(super) async fn check_bounded(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<(), StoreError> {
    check(tx, run, &mut Proof::bounded(32 << 20)).await
}

pub(super) async fn check_origin(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<(), StoreError> {
    allow(
        native_enrollment::original(tx, session, run, proof)
            .await
            .map_err(failure)?,
    )
}

fn decode_run(row: &sqlx::postgres::PgRow) -> Result<Run, StoreError> {
    let run: Run = super::decode_record(row.try_get("data").map_err(db)?)?;
    if run.id.0 != row.try_get::<String, _>("id").map_err(db)?
        || run.session_id != SessionId(row.try_get("session_id").map_err(db)?)
    {
        return Err(StoreError::Validation(
            "stored run identity is invalid".into(),
        ));
    }
    Ok(run)
}

impl PostgresStore {
    pub(super) async fn raw_execution_access(&self, fence: &RunFence) -> Result<(), StoreError> {
        let mut proof = Proof::bounded(32 << 20);
        proof.measure(fence).map_err(failure)?;
        let (mut tx, run) = self
            .run_transaction_with_proof(&fence.run_id, &mut proof)
            .await?;
        if run.claim_token != fence.claim_token
            || run.status.is_terminal()
            || run.lease_until <= self.clock.now()
        {
            return Err(StoreError::Conflict);
        }
        check_current(&mut tx, &run, &mut proof).await?;
        tx.rollback().await.map_err(db)
    }

    pub(super) async fn raw_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        let mut tx = super::transaction::begin(&self.pool).await.map_err(db)?;
        // Raw/Legacy birth is immutable. Every unsupported Pending birth refuses;
        // this read never admits a Raw holder into a Pending closure.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let mut proof = Proof::bounded(32 << 20);
        super::read_proof::charge(&mut tx, sqlx::query_scalar(
            "SELECT CAST(octet_length(data::text)+octet_length(id)+octet_length(session_id) AS BIGINT) FROM runs WHERE id=$1"
        ).bind(&id.0), &mut proof).await.map_err(failure)?;
        let row = sqlx::query("SELECT id,session_id,data::text AS data FROM runs WHERE id=$1")
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some(row) = row else {
            tx.rollback().await.map_err(db)?;
            return Ok(None);
        };
        let run = decode_run(&row)?;
        if run.id != *id {
            return Err(StoreError::Validation(
                "stored run identity is invalid".into(),
            ));
        }
        check(&mut tx, &run, &mut proof).await?;
        tx.rollback().await.map_err(db)?;
        Ok(Some(run))
    }

    pub(super) async fn raw_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        let mut tx = super::transaction::begin(&self.pool).await.map_err(db)?;
        // One captured set: a newly born Run cannot appear after the cost read.
        // Any unsupported birth in this snapshot refuses the entire list.
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        let mut proof = Proof::bounded(32 << 20);
        super::read_proof::charge(&mut tx, sqlx::query_scalar(
            "SELECT CAST(SUM(octet_length(data::text)+octet_length(id)+octet_length(session_id)) AS BIGINT) FROM runs WHERE status IN ('pending','running','paused')"
        ), &mut proof).await.map_err(failure)?;
        let rows = sqlx::query("SELECT id,session_id,data::text AS data FROM runs WHERE status IN ('pending','running','paused') ORDER BY seq")
            .fetch_all(&mut *tx).await.map_err(db)?;
        let mut runs = Vec::with_capacity(rows.len());
        for row in rows {
            let run = decode_run(&row)?;
            if run.status.is_terminal() {
                return Err(StoreError::Validation(
                    "stored run status is invalid".into(),
                ));
            }
            check(&mut tx, &run, &mut proof).await?;
            runs.push(run);
        }
        tx.rollback().await.map_err(db)?;
        Ok(runs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use crate::native_enrollment::access::contract;

    async fn replace(store: &PostgresStore, run: &RunId, value: &str) {
        let mut tx = super::super::transaction::begin(&store.pool).await.unwrap();
        sqlx::query("ALTER TABLE native_execution_unclassified DISABLE TRIGGER USER")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE native_execution_unclassified SET data=$2 WHERE run_id=$1")
            .bind(&run.0)
            .bind(value)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE native_execution_unclassified ENABLE TRIGGER USER")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }

    #[async_trait::async_trait]
    impl crate::driver_raw_access_contract::Damage for crate::PostgresStore {
        type Saved = String;
        async fn damage(&self, _: &crabber_core::SessionId, run: &RunId) -> Self::Saved {
            let saved = sqlx::query_scalar(
                "SELECT data FROM native_execution_unclassified WHERE run_id=$1",
            )
            .bind(&run.0)
            .fetch_one(&self.pool)
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
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::driver_raw_access_contract::run(&store, &clock).await;
    }
    #[tokio::test]
    async fn public_raw_credentials_require_original_enrollment_before_return() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let admitted = contract::admit(&store).await;
        let _other_scope = contract::admit(&store).await;
        contract::succeeds(&store, &admitted).await;
        let read = store.get_run(&admitted.run.id);
        let list = store.list_unfinished_runs();
        let acquire = store.execution(admitted.fence.clone());
        let original: String =
            sqlx::query_scalar("SELECT data FROM native_execution_unclassified WHERE run_id=$1")
                .bind(&admitted.run.id.0)
                .fetch_one(&store.pool)
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
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let first = contract::admit(&store).await;
        let other = contract::admit(&store).await;
        let forged_route = RunFence {
            run_id: first.run.id.clone(),
            claim_token: other.run.claim_token.clone(),
        };
        sqlx::query("UPDATE runs SET data=$2::jsonb WHERE id=$1")
            .bind(&first.run.id.0)
            .bind(serde_json::to_string(&other.run).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
        let refused = store.execution(forged_route).await.is_err();
        sqlx::query("UPDATE runs SET data=$2::jsonb WHERE id=$1")
            .bind(&first.run.id.0)
            .bind(serde_json::to_string(&first.run).unwrap())
            .execute(&store.pool)
            .await
            .unwrap();
        contract::succeeds(&store, &first).await;
        assert!(refused, "a row cannot borrow a distinct genuine Run origin");
    }
    #[tokio::test]
    async fn genuine_pending_birth_refuses_copied_cached_raw_mutations() {
        let Some(url) = crate::postgres::tests::test_url() else {
            return;
        };
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
        let session = contract::pending(store.clone()).await;
        let data: String = sqlx::query_scalar("SELECT r.data::text FROM runs r JOIN native_execution_origins o ON o.run_id=r.id WHERE o.birth_kind='pending' AND r.session_id=$1")
            .bind(&session.0).fetch_one(&store.pool).await.unwrap();
        let run: Run = super::super::decode_record(&data).unwrap();
        let cached = crate::postgres::PostgresExecution {
            store: store.as_ref().clone(),
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
        };
        contract::cached_mutations(store.as_ref(), &run, &cached).await;
        let after: String = sqlx::query_scalar("SELECT r.data::text FROM runs r JOIN native_execution_origins o ON o.run_id=r.id WHERE o.birth_kind='pending' AND r.session_id=$1")
            .bind(&session.0).fetch_one(&store.pool).await.unwrap();
        assert_eq!(after, data);
        store.purge_pending_initial_fixture(&session).await.unwrap();
    }
}
