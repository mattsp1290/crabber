#[tokio::test]
async fn frontend_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::frontend_contract::run_contract(|clock| store.with_clock(clock)).await;
}

#[tokio::test]
async fn frontend_fault_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    crate::frontend_contract::run_fault_contract(&store, &clock, |fault| {
        store
            .tool_continuation_fault
            .store(fault, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn frontend_process_helper() {
    let Some(directory) = crate::tool_continuation_contract::process::directory() else {
        return;
    };
    let url = crate::postgres::tests::test_url().unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    if std::env::var_os("CRABBER_TOOL_SQL_WRITE").is_some() {
        crate::frontend_contract::process::write(&store, &clock, &directory).await;
    } else {
        crate::frontend_contract::process::verify(&store, &directory).await;
    }
}
#[tokio::test]
async fn frontend_controls_survive_killed_writer() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    crate::tool_continuation_contract::process::journey(
        "postgres::frontend::tests::frontend_process_helper",
        None,
    )
    .await;
}

#[tokio::test]
async fn frontend_origin_absence_and_receipt_index_corruption_fail_closed() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for kind in 0..5 {
        let f = crate::frontend_contract::fixture::fixture(&store).await;
        let receipt = f
            .execution
            .stage_frontend_call(f.request.clone(), f.authority.clone())
            .await
            .unwrap();
        if kind == 0 || kind == 4 {
            sqlx::query("DELETE FROM frontend_tool_origins WHERE call_id=$1")
                .bind(&f.request.call.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        } else if kind == 1 {
            let encoded: String =
                sqlx::query_scalar("SELECT data_json FROM frontend_tool_origins WHERE call_id=$1")
                    .bind(&f.request.call.id.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut record: crate::frontend_continuation::FrontendOriginRecord =
                serde_json::from_str(&encoded).unwrap();
            record.request.descriptor.description = "coherent replacement".into();
            record.request.descriptor_fingerprint =
                crate::frontend_descriptor_fingerprint(&record.request.descriptor).unwrap();
            sqlx::query("UPDATE frontend_tool_origins SET data_json=$1 WHERE call_id=$2")
                .bind(serde_json::to_string(&record).unwrap())
                .bind(&f.request.call.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        }
        if kind == 2 || kind == 4 {
            sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
                .bind(&f.request.session_id.0)
                .bind(&f.request.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        } else if kind == 3 {
            sqlx::query("UPDATE operation_receipts SET kind='attempt' WHERE session_id=$1 AND operation_id=$2").bind(&f.request.session_id.0).bind(&f.request.id.0).execute(&store.pool).await.unwrap();
        }
        assert!(matches!(
            crate::Store::frontend_call_origin(
                &store,
                &f.request.session_id,
                &f.request.run_id,
                &f.request.call.id
            )
            .await,
            Err(crate::ToolContinuationError::UnknownStoreOutcome)
        ));
        if kind <= 1 {
            assert_eq!(
                crate::Store::lookup_frontend_staging(&store, &f.request.session_id, &f.request.id)
                    .await
                    .unwrap(),
                Some(receipt.clone())
            );
            assert_eq!(
                f.execution
                    .stage_frontend_call(f.request.clone(), f.authority.clone())
                    .await
                    .unwrap(),
                receipt
            );
        }
        assert_eq!(
            crate::Store::session_revision(&store, &f.request.session_id)
                .await
                .unwrap(),
            receipt.revision
        );
    }
}

#[tokio::test]
async fn frontend_removed_required_links_and_original_results_are_rejected() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for kind in 0..3 {
        let (f, batch, paused) = crate::frontend_contract::fixture::paused(&store).await;
        let claim =
            crate::frontend_contract::fixture::external_claim(&f.request, &batch, paused.revision);
        let crate::ToolContinuationClaimOutcome::Applied { receipt, .. } =
            crate::Store::claim_frontend_continuation(&store, claim.clone())
                .await
                .unwrap()
        else {
            panic!("fresh claim")
        };
        if kind == 0 {
            sqlx::query("DELETE FROM tool_continuation_frontend_calls WHERE continuation_id=$1")
                .bind(&batch.continuation.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        } else {
            let encoded: String =
                sqlx::query_scalar("SELECT data_json FROM tool_continuations WHERE id=$1")
                    .bind(&batch.continuation.id.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            let mut record: crate::tool_continuation::ToolContinuationRecord =
                serde_json::from_str(&encoded).unwrap();
            if kind == 1 {
                record.frontend_claim.as_mut().unwrap().results.clear();
            } else {
                record.frontend_claim = None;
                record.claim = None;
                record.state = crate::ToolContinuationState::Waiting;
            }
            sqlx::query("UPDATE tool_continuations SET data_json=$1 WHERE id=$2")
                .bind(serde_json::to_string(&record).unwrap())
                .bind(&batch.continuation.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
        }
        assert_eq!(
            crate::Store::tool_continuation_snapshot(
                &store,
                &f.request.session_id,
                &f.request.run_id
            )
            .await
            .unwrap_err(),
            crate::ToolContinuationError::UnknownStoreOutcome
        );
        assert!(
            matches!(crate::Store::claim_frontend_continuation(&store,claim).await.unwrap(),crate::ToolContinuationClaimOutcome::Replayed(saved) if saved==receipt)
        );
        assert_eq!(
            crate::Store::session_revision(&store, &f.request.session_id)
                .await
                .unwrap(),
            receipt.revision
        );
    }
}

#[tokio::test]
async fn frontend_near_limit_recovery() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    crate::frontend_contract::large::run(|clock| store.with_clock(clock)).await;
}
