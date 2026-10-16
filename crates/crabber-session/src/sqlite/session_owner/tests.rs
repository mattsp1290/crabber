use crate::session_owner_contract;
#[tokio::test]
async fn session_owner_contract() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    session_owner_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    session_owner_contract::faults::run(&store, &clock, |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    session_owner_contract::parent::run(&store, &clock).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    super::corruption::run(&store).await;
    session_owner_contract::process::run(
        &store,
        "sqlite::session_owner::tests::session_owner_process_helper",
        Some(&path),
    )
    .await;
}

#[tokio::test]
async fn session_owner_process_helper() {
    let Some(command) = session_owner_contract::process::command() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = crate::SqliteStore::connect(path).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10),
    )));
    session_owner_contract::process::execute(&store, command).await;
}

#[tokio::test]
async fn preparation_protocol_contract() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    Box::pin(crate::session_owner::preparation_contract::run(&store)).await;
}

#[tokio::test]
async fn preparation_protocol_commit_faults() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::session_owner::preparation_contract::faults::run(
        &store,
        |point| {
            store
                .session_owner_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
        |point| {
            store
                .binding_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_deleted_history_plus_foreign_publication() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let (grant, bound, old) =
        crate::session_owner::preparation_contract::absent::fixture_for_order(&store).await;
    assert_eq!(
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=? AND operation_id=?")
            .bind(&bound.binding.request.session_id.0)
            .bind(&old.0)
            .execute(&store.writer)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    crate::session_owner::preparation_contract::absent::foreign_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn preparation_absent_order_rejects_foreign_join_relabeled_renew() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let (grant, bound, join) =
        crate::session_owner::preparation_contract::absent::fixture_for_relabel(&store).await;
    assert_eq!(sqlx::query("UPDATE session_owner_audits SET operation='Renew' WHERE session_id=$1 AND operation_id=$2")
        .bind(&bound.binding.request.session_id.0).bind(&join.0).execute(&store.writer).await.unwrap().rows_affected(), 1);
    let mut tx = store.writer.begin().await.unwrap();
    assert_eq!(
        super::reads::publication_count(&mut tx, &bound.binding.request.session_id)
            .await
            .unwrap_err(),
        super::Error::UnknownStoreOutcome
    );
    tx.rollback().await.unwrap();
    crate::session_owner::preparation_contract::absent::relabel_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn owner_cancellation_contract() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    crate::session_owner::cancellation_contract::run(&store, &clock).await;
    super::cancellation_tests::corruption(&store).await;
    crate::session_owner::cancellation_contract::faults::run(&store, |point| {
        store
            .session_owner_fault
            .store(point, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    crate::session_owner_contract::process::run_cancellation(
        &store,
        "sqlite::session_owner::tests::session_owner_process_helper",
        Some(&path),
    )
    .await;
}

#[tokio::test]
async fn preparation_resource_contract() {
    use crate::session_owner::preparation_contract::resources::guards;
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    crate::session_owner::preparation_contract::resources::run(&store).await;
    crate::session_owner::preparation_contract::resources::faults(
        &store,
        |point| {
            store
                .session_owner_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
        |point| {
            store
                .binding_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
    guards::run(&store, &clock).await;
    let (grant, bound, original_begin) = guards::source_fixture(&store).await;
    assert_eq!(
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=? AND operation_id=?")
            .bind(&bound.binding.request.session_id.0)
            .bind(&original_begin.0)
            .execute(&store.writer)
            .await
            .unwrap()
            .rows_affected(),
        1
    );
    guards::source_refuses(&store, grant, bound).await;
}

#[tokio::test]
async fn preparation_handoff_marker_is_bound_to_original_join_digest() {
    use crate::session_owner::preparation_contract::resources::guards;
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    for native in [false, true] {
        let (grant, bound) = guards::handoff_fixture(&store, native).await;
        let metadata = grant.reconciliation().unwrap();
        let target = if native {
            serde_json::Value::Null
        } else {
            serde_json::to_value(grant.retained_resources_authority().unwrap().target()).unwrap()
        };
        for (table, field, id) in [
            ("session_owner_current", "owner_id", &metadata.owner.id.0),
            ("session_owner_audits", "operation_id", &metadata.join_id.0),
        ] {
            let query = format!("SELECT data_json FROM {table} WHERE session_id=? AND {field}=?");
            let data: String = sqlx::query_scalar(&query)
                .bind(&metadata.session_id.0)
                .bind(id)
                .fetch_one(&store.writer)
                .await
                .unwrap();
            let mut data: serde_json::Value = serde_json::from_str(&data).unwrap();
            let record = if table == "session_owner_current" {
                &mut data
            } else {
                &mut data["record"]
            };
            if native {
                record.as_object_mut().unwrap().remove("retained_resources");
            } else {
                record["retained_resources"] = target.clone();
            }
            let query = format!("UPDATE {table} SET data_json=? WHERE session_id=? AND {field}=?");
            sqlx::query(&query)
                .bind(serde_json::to_string(&data).unwrap())
                .bind(&metadata.session_id.0)
                .bind(id)
                .execute(&store.writer)
                .await
                .unwrap();
        }
        guards::handoff_refuses(&store, grant, bound).await;
    }
}

#[tokio::test]
async fn owned_scope_sources_and_join_hash_are_independent() {
    use crate::session_owner::preparation_contract::resources::scope_integrity as check;
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    for original_begin in [false, true] {
        let fixture = check::fixture(&store).await;
        let key = if original_begin {
            &fixture.activation.source.begin
        } else {
            fixture.activation.operation_id()
        };
        sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
            .bind(&fixture.child.session_id().0)
            .bind(&key.0)
            .execute(&store.writer)
            .await
            .unwrap();
        check::source_refuses(&store, fixture, original_begin).await;
    }
    for owned in [false, true] {
        let fixture = check::fixture(&store).await;
        check::join_source(&store, &fixture, owned).await;
        let join = fixture.proof.request().unwrap();
        for (table, column, identity) in [
            (
                "session_owner_current",
                "owner_id",
                fixture.child.target().id.0.clone(),
            ),
            ("session_owner_audits", "operation_id", join.id.0),
        ] {
            let text: String = sqlx::query_scalar(&format!(
                "SELECT data_json FROM {table} WHERE session_id=$1 AND {column}=$2"
            ))
            .bind(&fixture.child.session_id().0)
            .bind(&identity)
            .fetch_one(&store.writer)
            .await
            .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&text).unwrap();
            let record = if table == "session_owner_audits" {
                &mut value["record"]
            } else {
                &mut value
            };
            if owned {
                record.as_object_mut().unwrap().remove("owned_join");
            } else {
                record["owned_join"] = serde_json::to_value(fixture.proof.inspection()).unwrap();
            }
            sqlx::query(&format!(
                "UPDATE {table} SET data_json=$1 WHERE session_id=$2 AND {column}=$3"
            ))
            .bind(value.to_string())
            .bind(&fixture.child.session_id().0)
            .bind(&identity)
            .execute(&store.writer)
            .await
            .unwrap();
        }
        check::marker_refuses(&store, fixture).await;
    }
}

#[tokio::test]
async fn active_preparation_cannot_be_hidden_by_joined_index_route() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let (grant, _, _) =
        crate::session_owner::preparation_contract::absent::fixture_for_order(&store).await;
    let request = grant.original_reservation();
    let session = &request.owner.binding.session_id;
    let begin = request.reconciliation().unwrap().begin_id;
    assert_eq!(
        sqlx::query(
            "UPDATE session_owner_audits SET state='Joined' WHERE session_id=$1 AND operation_id=$2"
        )
        .bind(&session.0)
        .bind(&begin.0)
        .execute(&store.writer)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let mut tx = store.writer.begin().await.unwrap();
    assert!(
        super::reads::has_preparation(&mut tx, session)
            .await
            .unwrap()
    );
    tx.rollback().await.unwrap();
}
