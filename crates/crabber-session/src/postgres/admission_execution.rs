#[path = "admission_execution/prepared.rs"]
mod prepared;
pub(super) use prepared::claim_prepared;
#[path = "admission_execution/inspection.rs"]
mod inspection;
use super::{PostgresExecution, PostgresStore, db, decode, json, lookup_receipt};
use crate::StoreError;
use crate::{
    AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState,
    ClaimUnstartedAdmissionRequest, ClaimedAdmission,
};
use crabber_core::{Run, RunFence, RunId};
pub(super) use inspection::{inspect, validate_source};
use sqlx::{Postgres, Row, Transaction};

pub(super) async fn load_record(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
) -> Result<Option<AdmissionExecutionRecord>, StoreError> {
    let record = load_unchecked(tx, run).await?;
    if let Some(record) = &record {
        super::admission_journal::read(tx, record).await?;
    }
    Ok(record)
}
pub(super) async fn load_record_with_proof(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Option<AdmissionExecutionRecord>, crate::public_session::v2::proof::ReadError<StoreError>>
{
    use crate::public_session::v2::proof::ReadError;
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(CASE WHEN capsule_version=1 THEN octet_length(data::text) ELSE octet_length(data_json) END AS BIGINT) FROM admission_executions WHERE run_id=$1").bind(&run.0),proof).await.map_err(ReadError::Budget)?;
    let record = load_unchecked(tx, run).await?;
    if let Some(record) = &record {
        super::admission_journal::read_with_proof(tx, record, proof).await?;
    }
    Ok(record)
}
async fn load_unchecked(
    tx: &mut Transaction<'_, Postgres>,
    run: &RunId,
) -> Result<Option<AdmissionExecutionRecord>, StoreError> {
    let record = sqlx::query("SELECT capsule_version,CASE WHEN capsule_version=1 AND octet_length(data::text)<=16777216 THEN data ELSE NULL END AS data,CASE WHEN octet_length(data_json)<=16777216 THEN data_json END AS data_json,start_state FROM admission_executions WHERE run_id=$1")
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?
        .map(|row| {
            let version: i32 = row.try_get("capsule_version").map_err(db)?;
            let record: AdmissionExecutionRecord = match version {
                1 => decode(row.try_get::<Option<sqlx::types::Json<serde_json::Value>>, _>("data").map_err(db)?.ok_or(StoreError::AdmissionConflict)?)?,
                2 => AdmissionExecutionRecord::decode(&row.try_get::<Option<String>, _>("data_json").map_err(db)?.ok_or(StoreError::AdmissionConflict)?)?,
                _ => return Err(StoreError::AdmissionConflict),
            };
            if u32::try_from(version).ok() != Some(record.capsule.version) { return Err(StoreError::AdmissionConflict); }
            let state: String = row.get("start_state");
            if state != format!("{:?}", record.state) {
                return Err(StoreError::AdmissionConflict);
            }
            Ok(record)
        })
        .transpose()?;
    if let Some(record) = &record
        && record.receipt.run_id != *run
    {
        return Err(StoreError::AdmissionConflict);
    }
    if record.is_none() {
        let orphan: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM admission_execution_sources WHERE run_id=$1)",
        )
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)?;
        if orphan {
            return Err(StoreError::AdmissionConflict);
        }
    }
    Ok(record)
}

async fn validate_binding(
    tx: &mut Transaction<'_, Postgres>,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), Error> {
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages = super::execution_binding::original_messages(tx, &run.session_id, &run.id, count)
        .await
        .map_err(|_| Error::SemanticConflict)?;
    record.validate_messages(run, &messages)
}

pub(super) async fn claim(
    store: &PostgresStore,
    request: ClaimUnstartedAdmissionRequest,
) -> Result<ClaimedAdmission, Error> {
    let (mut tx, run) = store
        .run_transaction(&request.expected_fence.run_id)
        .await
        .map_err(unknown)?;
    super::session_owner::guard_native_claim(&mut tx, &request.session_id)
        .await
        .map_err(|error| match error {
            crate::session_owner::SessionOwnerError::Busy => Error::PreparationBusy,
            _ => Error::UnknownStoreFailure,
        })?;
    let value = claim_in_transaction(store, &mut tx, run, request, None).await?;
    tx.commit().await.map_err(|_| Error::UnknownStoreFailure)?;
    Ok(value)
}
pub(in crate::postgres) async fn claim_in_transaction(
    store: &PostgresStore,
    tx: &mut Transaction<'_, Postgres>,
    mut run: Run,
    request: ClaimUnstartedAdmissionRequest,
    completion: Option<crate::native_parent::CompletionSource>,
) -> Result<ClaimedAdmission, Error> {
    let record = load_record(tx, &run.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::MissingEvidence)?;
    let receipt = lookup_receipt(tx, &request.session_id, &request.key)
        .await
        .map_err(unknown)?;
    if receipt.as_ref() != Some(&record.receipt) {
        return Err(Error::SemanticConflict);
    }
    validate_binding(tx, &record, &run).await?;
    record.verify_claim(&request, &run)?;
    let now = store.clock.now();
    if run.lease_until > now {
        return Err(Error::LiveLease);
    }
    let lease = time::Duration::try_from(request.lease).map_err(|_| Error::SemanticConflict)?;
    if lease <= time::Duration::ZERO {
        return Err(Error::SemanticConflict);
    }
    run.owner = request.owner;
    run.claim_token = uuid::Uuid::new_v4().to_string();
    run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
    run.updated_at = now;
    super::save_run_with_completion(tx, &run, completion.as_ref())
        .await
        .map_err(unknown)?;
    Ok(ClaimedAdmission {
        record,
        fence: RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        },
        run,
    })
}

fn unknown(_: StoreError) -> Error {
    Error::UnknownStoreFailure
}

pub(super) async fn begin(execution: &PostgresExecution) -> Result<(), Error> {
    let store = &execution.store;
    let (mut tx, run) = store
        .run_transaction(&execution.fence.run_id)
        .await
        .map_err(unknown)?;
    if run.claim_token != execution.fence.claim_token {
        return Err(Error::StaleOwner);
    }
    if run.status.is_terminal() {
        return Err(Error::AlreadyTerminal);
    }
    if run.lease_until <= store.clock.now() {
        return Err(Error::StaleOwner);
    }
    let mut record = load_record(&mut tx, &run.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::MissingEvidence)?;
    validate_binding(&mut tx, &record, &run).await?;
    if lookup_receipt(&mut tx, &run.session_id, &record.key)
        .await
        .map_err(unknown)?
        .as_ref()
        != Some(&record.receipt)
    {
        return Err(Error::SemanticConflict);
    }
    if record.receipt.run_id != run.id || record.receipt.session_id != run.session_id {
        return Err(Error::SemanticConflict);
    }
    if record.state != AdmissionExecutionState::Unstarted {
        return Err(Error::AlreadyStarted);
    }
    super::admission_journal::begin(&mut tx, &record, &run)
        .await
        .map_err(unknown)?;
    record.state = AdmissionExecutionState::Started;
    if record.capsule.version == 2 {
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data_json=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(record.encode().map_err(unknown)?)
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    } else {
        sqlx::query(
            "UPDATE admission_executions SET start_state='Started',data=$2 WHERE run_id=$1",
        )
        .bind(&run.id.0)
        .bind(json(&record).map_err(unknown)?)
        .execute(&mut *tx)
        .await
        .map_err(|_| Error::UnknownStoreFailure)?;
    }
    tx.commit().await.map_err(|_| Error::UnknownStoreFailure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use crate::postgres::tests::{TEST_LOCK, test_url};
    use crabber_core::{
        AbandonAuthority, AbandonRequest, ManualClock, RunStatus, SessionId, Usage,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use time::OffsetDateTime;

    async fn admission(
        store: &PostgresStore,
        now: OffsetDateTime,
    ) -> (
        crate::KeyedAdmitRequest,
        crate::AdmitOutcome,
        ClaimUnstartedAdmissionRequest,
    ) {
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
        let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!()
        };
        let claim = ClaimUnstartedAdmissionRequest {
            session_id: receipt.session_id,
            key: keyed.options.key.clone(),
            expected_fence: admitted.fence.clone(),
            expected_owner: admitted.run.owner.clone(),
            fingerprint: receipt.fingerprint,
            behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
            semantic_digest: receipt.semantic_digest,
            capsule_digest: keyed.execution.as_ref().unwrap().digest().unwrap(),
            owner: "new owner".into(),
            lease: Duration::from_secs(30),
        };
        (keyed, *admitted, claim)
    }

    #[tokio::test]
    async fn claim_and_begin_never_wait_for_locked_session_row() {
        let Some(url) = test_url() else { return };
        let _guard = TEST_LOCK.lock().await;
        PostgresStore::migrate(&url).await.unwrap();
        let now = OffsetDateTime::now_utc();
        let clock = Arc::new(ManualClock::new(now));
        let store = PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        let (_, admitted, request) = admission(&store, now).await;
        clock.set(now + time::Duration::seconds(30));
        let mut session_lock = store.pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM sessions WHERE id=$1 FOR UPDATE")
            .bind(&admitted.session.id.0)
            .fetch_one(&mut *session_lock)
            .await
            .unwrap();
        let claimed = tokio::time::timeout(
            Duration::from_secs(2),
            store.claim_unstarted_admission(request),
        )
        .await
        .unwrap()
        .unwrap();
        let execution = store.execution(claimed.fence).await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(2),
            execution.begin_admission_execution(),
        )
        .await
        .unwrap()
        .unwrap();
        // Owner writes take the session gate, run, then session row. They may wait on the deliberately
        // held session, while claim/begin have already released their run lock.
        let event =
            crate::abandonment_contract::event(&claimed.run, crabber_core::EventKind::RunSettled);
        let write = tokio::spawn(async move {
            execution
                .settle_run(RunStatus::Completed, None, Usage::default(), event)
                .await
        });
        session_lock.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), write)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        store.pool.close().await;
    }

    #[tokio::test]
    async fn renew_claim_begin_and_abandon_serialize_without_duplicate_authority() {
        let Some(url) = test_url() else { return };
        let _guard = TEST_LOCK.lock().await;
        PostgresStore::migrate(&url).await.unwrap();
        let now = OffsetDateTime::now_utc();
        let clock = Arc::new(ManualClock::new(now));
        let store = PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        let (_, admitted, request) = admission(&store, now).await;
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        let (renewed, denied) = tokio::join!(
            execution.renew_lease(now + time::Duration::seconds(60)),
            store.claim_unstarted_admission(request.clone())
        );
        renewed.unwrap();
        assert_eq!(denied.unwrap_err(), Error::LiveLease);
        clock.set(now + time::Duration::seconds(60));
        let abandonment = AbandonRequest {
            expected: admitted.fence,
            expected_owner: admitted.run.owner,
            authority: AbandonAuthority::ExpiredLease,
        };
        let (claim_result, abandon_result) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                store.claim_unstarted_admission(request),
                store.abandon_run(abandonment)
            )
        })
        .await
        .unwrap();
        match (claim_result, abandon_result) {
            (Ok(claimed), Err(crabber_core::AbandonError::StaleOwner)) => {
                store
                    .execution(claimed.fence)
                    .await
                    .unwrap()
                    .begin_admission_execution()
                    .await
                    .unwrap();
            }
            (Err(Error::StaleOwner | Error::AlreadyTerminal), Ok(_)) => (),
            other => panic!("unexpected race {other:?}"),
        }
        // Live administrative abandonment competes with one-shot begin. If begin
        // wins, abandonment still revokes it; if abandonment wins, begin is denied.
        clock.set(now);
        let (_, admitted, _) = admission(&store, now).await;
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        let abandonment = AbandonRequest {
            expected: admitted.fence,
            expected_owner: admitted.run.owner,
            authority: AbandonAuthority::HostStoppedOwner,
        };
        let (begin, abandoned) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                execution.begin_admission_execution(),
                store.abandon_run(abandonment)
            )
        })
        .await
        .unwrap();
        abandoned.unwrap();
        assert!(matches!(
            begin,
            Ok(()) | Err(Error::StaleOwner | Error::AlreadyTerminal)
        ));
        assert!(
            store
                .get_run(&admitted.run.id)
                .await
                .unwrap()
                .unwrap()
                .status
                .is_terminal()
        );
        assert!(execution.begin_admission_execution().await.is_err());
        store.pool.close().await;
    }
}

#[cfg(test)]
#[tokio::test]
async fn failed_begin_update_rolls_back_started_and_can_be_retried() {
    use crate::Store;
    use crate::postgres::tests::{TEST_LOCK, test_url};
    use time::OffsetDateTime;
    let Some(url) = test_url() else { return };
    let _guard = TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let now = OffsetDateTime::now_utc();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(now));
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let keyed = crate::admission_execution_contract::keyed(&crabber_core::SessionId::new(), now);
    let crate::KeyedAdmitOutcome::Started { admitted, receipt } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let function = format!("begin_rollback_{suffix}");
    sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.run_id = '{}' THEN RAISE EXCEPTION 'injected begin rollback'; END IF; RETURN NEW; END $$", receipt.run_id.0))
        .execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {function} AFTER UPDATE ON admission_executions FOR EACH ROW EXECUTE FUNCTION {function}()"))
        .execute(&store.pool).await.unwrap();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        Error::UnknownStoreFailure
    );
    assert_eq!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .unwrap()
            .unwrap()
            .state,
        AdmissionExecutionState::Unstarted
    );
    sqlx::query(&format!("DROP TRIGGER {function} ON admission_executions"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {function}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    // The failed caller never retries begin for authority. After expiry a fresh
    // claim can start the retained Unstarted turn with a different fence.
    clock.set(now + time::Duration::seconds(30));
    let claimed = store
        .claim_unstarted_admission(ClaimUnstartedAdmissionRequest {
            session_id: receipt.session_id.clone(),
            key: keyed.options.key,
            expected_fence: admitted.fence,
            expected_owner: admitted.run.owner,
            fingerprint: receipt.fingerprint,
            behavior_fingerprint: keyed.options.behavior_fingerprint,
            semantic_digest: receipt.semantic_digest,
            capsule_digest: keyed.execution.unwrap().digest().unwrap(),
            owner: "after rollback".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let execution = store.execution(claimed.fence).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        Error::AlreadyStarted
    );
    store.pool.close().await;
}

#[cfg(test)]
#[path = "admission_execution/source_tests.rs"]
mod source_tests;
