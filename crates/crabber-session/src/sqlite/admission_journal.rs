//! Bounded immutable Init/Begin sources, independent of mutable execution state.
use super::db;
use crate::admission_journal::{self, Begin, Init, Kind, Source};
use crate::native_parent::{RunIssuance, RunState};
use crate::{AdmissionExecutionRecord, StoreError};
use crabber_core::{Run, RunId, SessionId};
use sqlx::{Row, Sqlite, Transaction};
fn invalid() -> StoreError {
    StoreError::AdmissionConflict
}
async fn issuer(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    sequence: Option<u64>,
) -> Result<RunIssuance, StoreError> {
    let row=sqlx::query("SELECT sequence,CASE WHEN length(CAST(data_json AS BLOB))<=2048 THEN data_json END AS data FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND ($3 IS NULL OR sequence=$3) ORDER BY sequence DESC LIMIT 1")
        .bind(&session.0).bind(&run.0).bind(sequence.map(i64::try_from).transpose().map_err(|_|invalid())?)
        .fetch_one(&mut **tx).await.map_err(db)?;
    let value: RunIssuance = serde_json::from_str(
        &row.try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    value.validate().map_err(|_| invalid())?;
    if value.state.session_id != *session
        || value.state.run_id != *run
        || i64::try_from(value.sequence).map_err(|_| invalid())?
            != row.try_get::<i64, _>("sequence").map_err(db)?
    {
        return Err(invalid());
    }
    Ok(value)
}
async fn insert(tx: &mut Transaction<'_, Sqlite>, source: Source) -> Result<(), StoreError> {
    let state = &source.issuance().state;
    sqlx::query("INSERT INTO admission_execution_sources(session_id,run_id,kind,data_json) VALUES($1,$2,$3,$4)")
        .bind(&state.session_id.0).bind(&state.run_id.0).bind(source.kind().name()).bind(source.encode()?)
        .execute(&mut **tx).await.map_err(db)?;
    Ok(())
}
pub(super) async fn init(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), StoreError> {
    let issuance = issuer(tx, &run.session_id, &run.id, Some(1)).await?;
    issuance.verify(run).map_err(|_| invalid())?;
    insert(tx, Source::Init(Init::new(record, issuance)?)).await
}
/// None denotes an authenticated legacy cohort, never missing new evidence.
pub(super) async fn read(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
) -> Result<Option<Init>, StoreError> {
    let protocol:i32=sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM admission_executions WHERE session_id=$1 AND run_id=$2")
        .bind(&record.receipt.session_id.0).bind(&record.receipt.run_id.0).fetch_one(&mut **tx).await.map_err(db)?;
    if !matches!(protocol, 0 | 1) {
        return Err(invalid());
    }
    let rows=sqlx::query("SELECT kind,session_id,CASE WHEN length(CAST(data_json AS BLOB))<=8192 THEN data_json END AS data FROM admission_execution_sources WHERE run_id=$1 ORDER BY kind LIMIT 3")
        .bind(&record.receipt.run_id.0).fetch_all(&mut **tx).await.map_err(db)?;
    let mut init = None;
    let mut begin = None;
    for row in rows {
        if row.try_get::<String, _>("session_id").map_err(db)? != record.receipt.session_id.0 {
            return Err(invalid());
        }
        let source = Source::decode(
            &row.try_get::<Option<String>, _>("data")
                .map_err(db)?
                .ok_or_else(invalid)?,
        )?;
        if row.try_get::<String, _>("kind").map_err(db)? != source.kind().name() {
            return Err(invalid());
        }
        let issuance = source.issuance();
        if *issuance
            != issuer(
                tx,
                &record.receipt.session_id,
                &record.receipt.run_id,
                Some(issuance.sequence),
            )
            .await?
        {
            return Err(invalid());
        }
        match source.kind() {
            Kind::Init => {
                if init.replace(source).is_some() {
                    return Err(invalid());
                }
            }
            Kind::Begin => {
                if begin.replace(source).is_some() {
                    return Err(invalid());
                }
            }
        }
    }
    admission_journal::verify(record, protocol == 1, init.as_ref(), begin.as_ref())?;
    Ok(match init {
        Some(Source::Init(init)) => Some(init),
        _ => None,
    })
}
pub(super) async fn begin(
    tx: &mut Transaction<'_, Sqlite>,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), StoreError> {
    if let Some(init) = read(tx, record).await? {
        let issuance = issuer(tx, &run.session_id, &run.id, None).await?;
        if issuance.state != RunState::from_run(run).map_err(|_| invalid())? {
            return Err(invalid());
        }
        insert(tx, Source::Begin(Begin::new(&init, issuance)?)).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::Store;
    use crabber_core::SessionId;
    #[tokio::test]
    async fn execution_sources_are_atomic_immutable_and_reject_state_rollback() {
        let (store, _path, _directory) = super::super::tests::temp_store().await;
        let now = time::OffsetDateTime::now_utc();
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
        let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!()
        };
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM admission_execution_sources WHERE run_id=$1")
                .bind(&receipt.run_id.0)
                .fetch_one(&store.writer)
                .await
                .unwrap();
        assert_eq!(count, 1);
        for query in [
            "UPDATE admission_execution_sources SET data_json='{}' WHERE run_id=$1",
            "DELETE FROM admission_execution_sources WHERE run_id=$1",
            "UPDATE admission_executions SET source_protocol=0 WHERE run_id=$1",
        ] {
            assert!(
                sqlx::query(query)
                    .bind(&receipt.run_id.0)
                    .execute(&store.writer)
                    .await
                    .is_err(),
                "{query}"
            );
        }
        for algorithm in ["REPLACE", "IGNORE"] {
            let source = format!(
                "INSERT OR {algorithm} INTO admission_execution_sources SELECT * FROM admission_execution_sources WHERE run_id=$1"
            );
            assert!(
                sqlx::query(&source)
                    .bind(&receipt.run_id.0)
                    .execute(&store.writer)
                    .await
                    .is_err()
            );
            let cohort = format!(
                "INSERT OR {algorithm} INTO admission_executions(run_id,session_id,admission_key,capsule_version,start_state,data,source_protocol) SELECT run_id,session_id,admission_key,capsule_version,start_state,data,0 FROM admission_executions WHERE run_id=$1"
            );
            assert!(
                sqlx::query(&cohort)
                    .bind(&receipt.run_id.0)
                    .execute(&store.writer)
                    .await
                    .is_err()
            );
        }
        let execution = store.execution(admitted.fence).await.unwrap();
        execution.begin_admission_execution().await.unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM admission_execution_sources WHERE run_id=$1")
                .bind(&receipt.run_id.0)
                .fetch_one(&store.writer)
                .await
                .unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            execution.begin_admission_execution().await.unwrap_err(),
            crate::AdmissionExecutionError::AlreadyStarted
        );
        sqlx::query("UPDATE admission_executions SET start_state='Unstarted',data=json_set(data,'$.state','Unstarted') WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.writer).await.unwrap();
        assert!(
            store
                .load_admission_execution(&receipt.session_id, &keyed.options.key)
                .await
                .is_err()
        );
        assert!(execution.begin_admission_execution().await.is_err());
        store.writer.close().await;
    }
}
