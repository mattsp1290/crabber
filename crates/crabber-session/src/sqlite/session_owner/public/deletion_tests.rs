use super::*;
use crate::PublicSessionLimit;
use crate::session_owner::coverage::{
    CheckedOwners,
    contract::{Mode, fixture},
};
use crate::sqlite::SqliteStore;
async fn check(
    store: &SqliteStore,
    session: &SessionId,
    cap: usize,
) -> (Result<CheckedOwners, Failure>, usize) {
    let mut tx = crate::transaction::begin(&store.readers).await.unwrap();

    let mut proof = Proof::bounded(cap);
    let result = deletion_registered(
        &mut tx,
        session,
        &mut Budget::new(65536, PublicSessionLimit::HeaderBytes),
        &mut proof,
    )
    .await;
    tx.rollback().await.unwrap();
    (result, proof.used())
}
#[tokio::test]
async fn detached_owner_coverage_requires_original_declaration_activation_and_parent() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    for (mode, unknown_history) in [
        (Mode::Declared, false),
        (Mode::ActivatedNoDispatch, false),
        (Mode::Legacy, true),
        (Mode::Claim, true),
        (Mode::NoActivation, true),
    ] {
        let (session, _) = fixture(&store, mode).await;
        let (result, used) = check(&store, &session, 32 * 1024 * 1024).await;
        let owners = result.unwrap();
        assert_eq!(owners.live.len(), 0);
        assert_eq!(owners.unclassified, unknown_history);
        assert!(matches!(
            check(&store, &session, 1).await.0,
            Err(Failure::Limited(_))
        ));
        if !unknown_history {
            assert!(matches!(
                check(&store, &session, used - 1).await.0,
                Err(Failure::Limited(_))
            ));
            corrupt_issuer(&store, &session).await;
            corrupt_all_owner_parent_copies(&store, &session).await;
        }
    }
}
async fn corrupt_issuer(store: &SqliteStore, session: &SessionId) {
    let row = sqlx::query("SELECT run_id,sequence,data_json FROM native_run_issuances WHERE session_id=$1 ORDER BY sequence LIMIT 1")
        .bind(&session.0).fetch_one(&store.readers).await.unwrap();
    let run: String = row.get("run_id");
    let sequence: i64 = row.get("sequence");
    let original: String = row.get("data_json");
    let mut changed: serde_json::Value = serde_json::from_str(&original).unwrap();
    changed["state"]["token"] = serde_json::Value::String("f".repeat(64));
    for (raw, rejected) in [(changed.to_string(), true), (original, false)] {
        sqlx::query("UPDATE native_run_issuances SET data_json=$1 WHERE session_id=$2 AND run_id=$3 AND sequence=$4")
            .bind(raw).bind(&session.0).bind(&run).bind(sequence).execute(&store.writer).await.unwrap();
        let result = check(store, session, 32 * 1024 * 1024).await.0;
        if rejected {
            assert!(matches!(result, Err(Failure::Error(_))));
        } else {
            assert!(!result.unwrap().unclassified);
        }
    }
}

// A coherent rewrite of all owner-private copies must still fail against the
// independently persisted original native Begin witness.
async fn corrupt_all_owner_parent_copies(store: &SqliteStore, session: &SessionId) {
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let current: String =
        sqlx::query_scalar("SELECT data_json FROM session_owner_current WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let mut changed: serde_json::Value = serde_json::from_str(&current).unwrap();
    changed["parent"]["run_token"] = serde_json::Value::String("f".repeat(64));
    sqlx::query("UPDATE session_owner_current SET data_json=$1 WHERE session_id=$2")
        .bind(changed.to_string())
        .bind(&session.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    let audits: Vec<(String, String)> = sqlx::query_as(
        "SELECT operation_id,data_json FROM session_owner_audits WHERE session_id=$1",
    )
    .bind(&session.0)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    for (operation, raw) in audits {
        let mut changed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        changed["record"]["parent"]["run_token"] = serde_json::Value::String("f".repeat(64));
        sqlx::query(
            "UPDATE session_owner_audits SET data_json=$1 WHERE session_id=$2 AND operation_id=$3",
        )
        .bind(changed.to_string())
        .bind(&session.0)
        .bind(operation)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    // Independent publication bytes now reject this coherent rewrite before
    // the native-parent cross-check; both sources must remain authoritative.
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    assert!(
        registered(
            &mut tx,
            session,
            64,
            &mut Budget::new(65536, PublicSessionLimit::HeaderBytes),
            &mut Proof::bounded(32 * 1024 * 1024),
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
    assert!(matches!(
        check(store, session, 32 * 1024 * 1024).await.0,
        Err(Failure::Error(_))
    ));
}

#[tokio::test]
async fn legacy_native_parent_remains_usable_but_unclassified() {
    use crate::session_owner::coverage::contract::{fixture_from_parent, native_parent};
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    let (request, parent) = native_parent(&store).await;
    let session = request.binding.session_id.clone();
    let original: String = sqlx::query_scalar(
        "SELECT data_json FROM native_parent_witnesses WHERE session_id=$1 AND kind='phase'",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    let mut old: serde_json::Value = serde_json::from_str(&original).unwrap();
    old["version"] = serde_json::json!(1);
    old.as_object_mut().unwrap().remove("run_issuance_digest");
    sqlx::query(
        "UPDATE native_parent_witnesses SET data_json=$1 WHERE session_id=$2 AND kind='phase'",
    )
    .bind(old.to_string())
    .bind(&session.0)
    .execute(&store.writer)
    .await
    .unwrap();
    fixture_from_parent(&store, Mode::Declared, request, parent).await;
    let owners = check(&store, &session, 32 * 1024 * 1024).await.0.unwrap();
    assert_eq!(owners.live.len(), 0);
    assert!(owners.unclassified);
}
