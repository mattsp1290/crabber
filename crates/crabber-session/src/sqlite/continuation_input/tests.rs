//! Old mutable record/receipt/event pairs cannot rewrite independently anchored births.
use super::*;
use crate::Store;
use crate::continuation_input::tests::{Case, fixture, rewrite};
#[tokio::test]
async fn coherent_birth_carrier_rewrites_are_refused() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("birth.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for domain in [Domain::Approval, Domain::Tool] {
        for answer in [false, true] {
            let case = fixture(&store, domain, answer).await;
            Box::pin(rewritten(&store, &case)).await;
            Box::pin(guards(&store, &case)).await;
            Box::pin(legacy_scope(&store, &case)).await;
            Box::pin(erased_current(&store, &case)).await;
            // Cold original reads still work after rolling back the corrupt projections.
            match domain {
                Domain::Approval => {
                    store
                        .original_approval_plan(
                            &case.scope.session,
                            &case.scope.run,
                            &case.scope.continuation,
                        )
                        .await
                        .unwrap();
                }
                Domain::Tool => {
                    store
                        .original_tool_continuation_plan(
                            &case.scope.session,
                            &case.scope.run,
                            &case.scope.continuation,
                        )
                        .await
                        .unwrap();
                }
            }
        }
    }
}
async fn rewritten(store: &crate::SqliteStore, case: &Case) {
    let scope = &case.scope;
    let (table, _, _) = tables(scope.domain);
    let mut tx = store.begin_write().await.unwrap();
    let raw: String = sqlx::query_scalar(&format!("SELECT data_json FROM {table} WHERE id=$1"))
        .bind(&scope.continuation.id.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let receipt: String = sqlx::query_scalar(
        "SELECT data_json FROM operation_receipts WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&scope.session.0)
    .bind(&case.operation.0)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let (record, receipt) = rewrite(
        case,
        serde_json::from_str(&raw).unwrap(),
        serde_json::from_str(&receipt).unwrap(),
    );
    let cursor: crabber_core::EventCursor =
        serde_json::from_value(receipt["event_cursor"].clone()).unwrap();
    let mut event = match scope.domain {
        Domain::Approval => {
            super::super::read_proof::event(
                &mut tx,
                &scope.session,
                cursor,
                false,
                &mut Proof::bounded(32 << 20),
            )
            .await
        }
        Domain::Tool => {
            super::super::read_proof::control_event(
                &mut tx,
                &scope.session,
                cursor,
                &mut Proof::bounded(32 << 20),
            )
            .await
        }
    }
    .unwrap();
    crate::continuation_input::tests::event(case, &receipt, &mut event);
    sqlx::query(&format!("UPDATE {table} SET data_json=$2 WHERE id=$1"))
        .bind(&scope.continuation.id.0)
        .bind(serde_json::to_string(&record).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE operation_receipts SET data_json=$3,semantic_digest=$4 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&scope.session.0)
    .bind(&case.operation.0)
    .bind(serde_json::to_string(&receipt).unwrap())
    .bind(receipt["semantic_digest"].as_str().unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query("UPDATE events SET data=$3 WHERE session_id=$1 AND seq=$2")
        .bind(&scope.session.0)
        .bind(i64::try_from(cursor.0).unwrap())
        .bind(serde_json::to_string(&event).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
    verify_rewritten(&mut tx, case, record, &event).await;
    tx.rollback().await.unwrap();
}

async fn guards(store: &crate::SqliteStore, case: &Case) {
    let scope = &case.scope;
    let (current, sources, cohorts) = tables(scope.domain);
    let mut tx = store.begin_write().await.unwrap();
    for command in [
        format!("UPDATE {sources} SET source_digest=source_digest WHERE session_id=$1"),
        format!("DELETE FROM {sources} WHERE session_id=$1"),
        format!("UPDATE {cohorts} SET origin_digest=origin_digest WHERE session_id=$1"),
        format!("DELETE FROM {cohorts} WHERE session_id=$1"),
        format!("UPDATE {current} SET input_source_protocol=0 WHERE session_id=$1"),
        format!("DELETE FROM {current} WHERE session_id=$1"),
    ] {
        sqlx::query("SAVEPOINT immutable_guard")
            .execute(&mut *tx)
            .await
            .unwrap();
        let result = sqlx::query(&command)
            .bind(&scope.session.0)
            .execute(&mut *tx)
            .await;
        assert!(result.is_err(), "protected source guard: {command}");
        sqlx::query("ROLLBACK TO SAVEPOINT immutable_guard")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("RELEASE SAVEPOINT immutable_guard")
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    replacements(&mut tx, case).await;
    consistent(
        &mut tx,
        scope.domain,
        &scope.session,
        Some(&scope.run),
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    assert!(
        consistent(
            &mut tx,
            scope.domain,
            &scope.session,
            None,
            &mut Proof::bounded(0)
        )
        .await
        .is_err()
    );
    let source = load(
        &mut tx,
        scope,
        &case.operation,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    let mut proof = Proof::bounded(32 << 20);
    assert_eq!(
        load(&mut tx, scope, &case.operation, &mut proof)
            .await
            .unwrap(),
        source
    );
    assert!(
        load(
            &mut tx,
            scope,
            &case.operation,
            &mut Proof::bounded(proof.used() - 1)
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

async fn verify_rewritten(
    tx: &mut Transaction<'_, Sqlite>,
    case: &Case,
    record: serde_json::Value,
    event: &crabber_core::EventRecord,
) {
    let scope = &case.scope;
    // Existing receipt/event verification accepts the coherent pair. Only the birth anchor rejects it.
    match scope.domain {
        Domain::Approval => {
            let saved = super::super::approval::receipt_with_proof(
                tx,
                &scope.session,
                &case.operation,
                &mut Proof::bounded(32 << 20),
            )
            .await
            .unwrap()
            .unwrap();
            let r: crate::approval::ApprovalRecord = serde_json::from_value(record).unwrap();
            let input = if case.answer {
                Input::ApprovalAnswer(r.questions[0].answer.as_ref().unwrap())
            } else {
                Input::ApprovalSuspension(&r.batch)
            };
            if case.answer {
                super::super::approval::original_batch_with_proof(
                    tx,
                    &scope.session,
                    &scope.run,
                    &scope.continuation,
                    &mut Proof::bounded(32 << 20),
                )
                .await
                .unwrap();
            }
            assert!(
                verify(
                    tx,
                    input,
                    Receipt::Approval(&saved),
                    event,
                    &mut Proof::bounded(32 << 20)
                )
                .await
                .is_err()
            );
            if case.answer {
                assert!(
                    answers(tx, scope, &[input], &mut Proof::bounded(32 << 20))
                        .await
                        .is_err()
                );
            } else {
                assert!(
                    super::super::approval::original_batch_with_proof(
                        tx,
                        &scope.session,
                        &scope.run,
                        &scope.continuation,
                        &mut Proof::bounded(32 << 20)
                    )
                    .await
                    .is_err()
                );
            }
        }
        Domain::Tool => {
            let (saved, _) = super::super::tool_continuation::read::receipt_with_proof(
                tx,
                &scope.session,
                &case.operation,
                &mut Proof::bounded(32 << 20),
            )
            .await
            .unwrap()
            .unwrap();
            let r: crate::tool_continuation::ToolContinuationRecord =
                serde_json::from_value(record).unwrap();
            let input = if case.answer {
                Input::ToolAnswer(r.questions[0].answer.as_ref().unwrap())
            } else {
                Input::ToolSuspension(&r.batch)
            };
            assert!(
                verify(
                    tx,
                    input,
                    Receipt::Tool(&saved),
                    event,
                    &mut Proof::bounded(32 << 20)
                )
                .await
                .is_err()
            );
            if case.answer {
                assert!(
                    answers(tx, scope, &[input], &mut Proof::bounded(32 << 20))
                        .await
                        .is_err()
                );
            }
        }
    }
}

async fn replacements(tx: &mut Transaction<'_, Sqlite>, case: &Case) {
    let (current, sources, cohorts) = tables(case.scope.domain);
    let commands = [
        format!("INSERT OR REPLACE INTO {sources} SELECT * FROM {sources} WHERE session_id=$1"),
        format!(
            "INSERT OR REPLACE INTO {sources}(session_id,run_id,continuation_id,generation,protocol_version,operation_id,kind,subject,source_digest,event_seq,data_json) SELECT session_id,run_id,continuation_id,generation,protocol_version,'alternate-operation',kind,subject,source_digest,event_seq,data_json FROM {sources} WHERE session_id=$1 LIMIT 1"
        ),
        format!("INSERT OR REPLACE INTO {cohorts} SELECT * FROM {cohorts} WHERE session_id=$1"),
        format!("INSERT OR REPLACE INTO {current} SELECT * FROM {current} WHERE session_id=$1"),
        format!(
            "INSERT OR REPLACE INTO {current}(id,session_id,run_id,generation,state,format_version,data_json,input_source_protocol) SELECT 'alternate-continuation',session_id,run_id,generation,state,format_version,data_json,input_source_protocol FROM {current} WHERE session_id=$1"
        ),
    ];
    for command in commands {
        let error = sqlx::query(&command)
            .bind(&case.scope.session.0)
            .execute(&mut **tx)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("crabber_continuation_input_replace"),
            "{command}: {error}"
        );
    }
}

/// Explicit legacy-format import fixture; bypasses birth guards only inside rolled-back test Tx.
async fn legacy_scope(store: &crate::SqliteStore, case: &Case) {
    let scope = &case.scope;
    let (current, sources, cohorts) = tables(scope.domain);
    let mut tx = store.begin_write().await.unwrap();
    let prefix = if scope.domain == Domain::Approval {
        "approval_input"
    } else {
        "tool_input"
    };
    for suffix in ["_cohorts_delete", "_sources_delete", "_identity"] {
        sqlx::query(&format!("DROP TRIGGER {prefix}{suffix}"))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for command in [
        format!("DELETE FROM {cohorts} WHERE session_id=$1"),
        format!("DELETE FROM {sources} WHERE session_id=$1"),
        format!("UPDATE {current} SET input_source_protocol=0 WHERE session_id=$1"),
    ] {
        sqlx::query(&command)
            .bind(&scope.session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }

    legacy_delete_guard(&mut tx, current, scope).await;
    assert!(
        !protocol(&mut tx, scope, &mut Proof::bounded(32 << 20))
            .await
            .unwrap()
    );
    consistent(
        &mut tx,
        scope.domain,
        &scope.session,
        None,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    match scope.domain {
        Domain::Approval => {
            super::super::approval::load_with_proof(
                &mut tx,
                &scope.session,
                &scope.run,
                &mut Proof::bounded(32 << 20),
            )
            .await
            .unwrap()
            .unwrap();
            super::super::approval::original_batch_with_proof(
                &mut tx,
                &scope.session,
                &scope.run,
                &scope.continuation,
                &mut Proof::bounded(32 << 20),
            )
            .await
            .unwrap();
        }
        Domain::Tool => {
            super::super::tool_continuation::read::load_with_proof(
                &mut tx,
                &scope.session,
                &scope.run,
                Some(&scope.continuation.id),
                &mut Proof::bounded(32 << 20),
            )
            .await
            .unwrap()
            .unwrap();
        }
    }
    tx.rollback().await.unwrap();
}
/// A genuine reader corruption test, separate from ordinary immutable-write refusal.
async fn erased_current(store: &crate::SqliteStore, case: &Case) {
    let scope = &case.scope;
    let (current, _, _) = tables(scope.domain);
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("PRAGMA defer_foreign_keys=ON")
        .execute(&mut *tx)
        .await
        .unwrap();
    let prefix = if scope.domain == Domain::Approval {
        "approval_input"
    } else {
        "tool_input"
    };
    sqlx::query(&format!("DROP TRIGGER {prefix}_delete"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(&format!("DELETE FROM {current} WHERE session_id=$1"))
        .bind(&scope.session.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        consistent(
            &mut tx,
            scope.domain,
            &scope.session,
            None,
            &mut Proof::bounded(32 << 20)
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}

async fn legacy_delete_guard(tx: &mut Transaction<'_, Sqlite>, current: &str, scope: &Scope) {
    sqlx::query("SAVEPOINT legacy_delete")
        .execute(&mut **tx)
        .await
        .unwrap();
    let links: &[&str] = match scope.domain {
        Domain::Approval => &["continuation_interactions"],
        Domain::Tool => &["tool_continuation_interactions", "tool_continuation_phases"],
    };
    for table in links {
        sqlx::query(&format!("DELETE FROM {table} WHERE continuation_id=$1"))
            .bind(&scope.continuation.id.0)
            .execute(&mut **tx)
            .await
            .unwrap();
    }
    let barrier = if scope.domain == Domain::Approval {
        "continuation_barriers"
    } else {
        "tool_continuation_barriers"
    };
    sqlx::query(&format!("DELETE FROM {barrier} WHERE session_id=$1"))
        .bind(&scope.session.0)
        .execute(&mut **tx)
        .await
        .unwrap();
    let error = sqlx::query(&format!("DELETE FROM {current} WHERE session_id=$1"))
        .bind(&scope.session.0)
        .execute(&mut **tx)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("crabber_continuation_input_identity"),
        "{error}"
    );
    sqlx::query("ROLLBACK TO SAVEPOINT legacy_delete")
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("RELEASE SAVEPOINT legacy_delete")
        .execute(&mut **tx)
        .await
        .unwrap();
}
