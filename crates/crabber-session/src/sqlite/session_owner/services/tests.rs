use crate::Store;
#[tokio::test]
async fn service_generation_original_membership_and_bounds() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    for huge in [false, true] {
        let (request, receipt, next) =
            crate::session_owner_contract::services::cohort::source_fixture(&store).await;
        let session = &receipt.source.root.session_id;
        let owner = &receipt.began.owner.target.id;
        let text: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind(&owner.0)
        .fetch_one(&store.writer)
        .await
        .unwrap();
        let mut data: serde_json::Value = serde_json::from_str(&text).unwrap();
        if huge {
            data["owner"] = serde_json::Value::String("x".repeat(33 * 1024 * 1024));
        } else {
            data.as_object_mut().unwrap().remove("service");
        }
        sqlx::query(
            "UPDATE session_owner_current SET data_json=$1 WHERE session_id=$2 AND owner_id=$3",
        )
        .bind(serde_json::to_string(&data).unwrap())
        .bind(&session.0)
        .bind(&owner.0)
        .execute(&store.writer)
        .await
        .unwrap();
        if !huge {
            assert!(matches!(
                store.get_session_owner(session, owner).await,
                Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
            ));
        }
        let expected = if huge {
            crate::session_owner::SessionOwnerError::Capacity
        } else {
            crate::session_owner::SessionOwnerError::UnknownStoreOutcome
        };
        crate::session_owner_contract::services::cohort::source_refuses(
            &store, request, receipt, expected, next,
        )
        .await;
    }
}

#[tokio::test]
async fn later_service_access_refuses_erased_cancel_with_retained_revision() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (access, _service, cancel) =
        crate::session_owner_contract::services::cohort::cancelled_across_roots(&store).await;
    let session = &access.source.root.session_id;
    let owner = &access.authority.target().id;
    let revision = store.session_revision(session).await.unwrap();
    let activated: String = sqlx::query_scalar("SELECT data_json FROM session_owner_audits WHERE session_id=$1 AND owner_id=$2 AND operation='ActivateScope'")
        .bind(&session.0).bind(&owner.0).fetch_one(&store.writer).await.unwrap();
    let old: serde_json::Value = serde_json::from_str(&activated).unwrap();
    sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
        .bind(&session.0)
        .bind(&cancel.0)
        .execute(&store.writer)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$1 WHERE session_id=$2 AND owner_id=$3",
    )
    .bind(old["record"].to_string())
    .bind(&session.0)
    .bind(&owner.0)
    .execute(&store.writer)
    .await
    .unwrap();
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
    assert_eq!(
        store.get_session_owner(session, owner).await.unwrap_err(),
        crate::session_owner::SessionOwnerError::UnknownStoreOutcome,
        "snapshot must not report Active after the independent cancellation source",
    );
    let result = store.check_session_service_dependency_access(access).await;
    assert!(
        matches!(
            result,
            Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
        ),
        "missing cancellation source accepted: {result:?}"
    );
}
