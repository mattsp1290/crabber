//! Coherent phase substitution and later-current source must fail closed.
use super::*;
use crate::Store;
use crate::successor_driver_contract::tools::{Fixture, fixture};
use crabber_core::{Clock, ManualClock};
use std::sync::Arc;
#[tokio::test]
async fn tool_fresh_current_carriers_and_exact_source() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let native = crate::PostgresStore::connect(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = native.with_clock(clock.clone());
    for (frontend, mixed) in [(false, false), (true, false), (true, true)] {
        for recovery in [false, true] {
            let f = fixture_for(&store, &clock, (frontend, mixed), recovery).await;
            let run = store.get_run(&f.receipt.run_id).await.unwrap().unwrap();
            let mut tx = store
                .session_transaction(f.context.session_id())
                .await
                .unwrap();
            let request = request(&mut tx, &f).await;
            let mut proof = Proof::bounded(32 << 20);
            Box::pin(fresh::fresh(
                &mut tx,
                &request,
                None,
                clock.now(),
                &mut proof,
            ))
            .await
            .unwrap();
            Box::pin(fresh::fresh(
                &mut tx,
                &request,
                None,
                clock.now(),
                &mut Proof::bounded(proof.used()),
            ))
            .await
            .unwrap();
            assert!(
                Box::pin(fresh::fresh(
                    &mut tx,
                    &request,
                    None,
                    clock.now(),
                    &mut Proof::bounded(proof.used() - 1)
                ))
                .await
                .is_err()
            );
            for mutation in 0..5 {
                sqlx::query("SAVEPOINT carrier")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                mutate(&mut tx, &f, mutation).await;
                assert!(
                    Box::pin(fresh::fresh(
                        &mut tx,
                        &request,
                        None,
                        clock.now(),
                        &mut Proof::bounded(32 << 20)
                    ))
                    .await
                    .is_err(),
                    "frontend={frontend} recovery={recovery} mutation={mutation}"
                );
                sqlx::query("ROLLBACK TO SAVEPOINT carrier")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                sqlx::query("RELEASE SAVEPOINT carrier")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            if mixed {
                ordinary_later(&mut tx, &run, &f, &request, clock.now()).await;
                if !recovery {
                    status_carriers(&mut tx, &f, &request, clock.now()).await;
                }
            }
            if frontend {
                substitution(&mut tx, &f, &request, clock.now()).await;
            }
            if !recovery {
                later(&mut tx, &run, &f, clock.now()).await;
            }
            tx.rollback().await.unwrap();
            if mixed {
                Box::pin(crate::successor_driver_contract::tools::fresh::exercise(
                    &store, request,
                ))
                .await;
            }
        }
    }
}
pub(super) async fn request(
    tx: &mut Transaction<'_, Postgres>,
    f: &Fixture,
) -> RegisterSuccessorExecutionDriver {
    let join = f.joined.observed_grant().reconciliation().unwrap().join_id;
    let slots: Vec<i64> = sqlx::query_scalar("SELECT sequence FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND data_json::json->'completion_source'->>'root_join_id'=$3 LIMIT 2")
        .bind(&f.receipt.session_id.0).bind(&f.receipt.run_id.0).bind(&join.0).fetch_all(&mut **tx).await.unwrap();
    assert_eq!(slots.len(), 1);
    RegisterSuccessorExecutionDriver {
        context: f.context.clone(),
        fence: f.native.fence.clone(),
        claim: crate::session_owner::driver::NativeClaimLocator {
            issuance: u64::try_from(slots[0]).unwrap(),
            root_join_id: join,
        },
    }
}
async fn fixture_for(
    store: &crate::PostgresStore,
    clock: &ManualClock,
    kind: (bool, bool),
    recovery: bool,
) -> Fixture {
    if kind.1 {
        Box::pin(crate::successor_driver_contract::tools::fixture_mixed(
            store, clock, recovery, false,
        ))
        .await
    } else {
        Box::pin(fixture(store, clock, kind.0, recovery, false)).await
    }
}

async fn mutate(tx: &mut Transaction<'_, Postgres>, f: &Fixture, mutation: u8) {
    let (target, _) =
        crate::session_owner::driver::successor::tool_fresh::expected_phases(&f.batch)
            .next()
            .unwrap();
    let query = match mutation {
        0 => "UPDATE tool_phases SET state='Started' WHERE run_id=$1 AND id=$2",
        1 => "UPDATE tool_phases SET quiescence_state='Complete' WHERE run_id=$1 AND id=$2",
        2 => "UPDATE tool_phases SET claim_token='' WHERE run_id=$1 AND id=$2",
        3 => "UPDATE tool_phases SET generation=generation+1 WHERE run_id=$1 AND id=$2",
        4 => "UPDATE tool_calls SET status='running' WHERE run_id=$1 AND id=$2",
        _ => unreachable!(),
    };
    sqlx::query(query)
        .bind(&f.receipt.run_id.0)
        .bind(if mutation == 4 {
            &target.call_id.0
        } else {
            &target.id.0
        })
        .execute(&mut **tx)
        .await
        .unwrap();
}
async fn substitution(
    tx: &mut Transaction<'_, Postgres>,
    f: &Fixture,
    request: &RegisterSuccessorExecutionDriver,
    now: time::OffsetDateTime,
) {
    sqlx::query("SAVEPOINT substitution")
        .execute(&mut **tx)
        .await
        .unwrap();
    let target = &f.batch.frontend[0].target;
    let ordinary = f
        .batch
        .calls
        .iter()
        .find(|v| {
            !crate::session_owner::driver::successor::tool_fresh::expected_phases(&f.batch)
                .any(|(target, _)| *target == v.target)
        })
        .unwrap();
    let mut replacement = crate::postgres::phase::load_with_proof(
        tx,
        &f.receipt.run_id,
        &target.id,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    replacement.snapshot.target = ordinary.target.clone();
    replacement.snapshot.call_binding = ordinary.call_binding.clone();
    replacement.validate().unwrap();
    sqlx::query("UPDATE tool_phases SET id=$3,call_id=$4,generation=$5,outcome_json=$6 WHERE run_id=$1 AND id=$2")
        .bind(&f.receipt.run_id.0).bind(&target.id.0).bind(&ordinary.target.id.0).bind(&ordinary.target.call_id.0)
        .bind(i64::try_from(ordinary.target.generation.get()).unwrap()).bind(crate::phase::sql::encode_outcome(&replacement).unwrap())
        .execute(&mut **tx).await.unwrap();
    assert!(
        Box::pin(fresh::fresh(
            tx,
            request,
            None,
            now,
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    sqlx::query("ROLLBACK TO SAVEPOINT substitution")
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("RELEASE SAVEPOINT substitution")
        .execute(&mut **tx)
        .await
        .unwrap();
}
async fn later(
    tx: &mut Transaction<'_, Postgres>,
    run: &crabber_core::Run,
    f: &Fixture,
    now: time::OffsetDateTime,
) {
    let mut record = crate::postgres::tool_continuation::read::origin_with_proof(
        tx,
        &f.receipt.session_id,
        &f.receipt.run_id,
        None,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    let recovery = crate::ToolContinuationRecovery {
        id: crabber_core::ControlId::new(),
        expected_revision: f.receipt.revision,
        original: record.claim.clone().unwrap(),
        attempt: f.batch.next_attempt.clone(),
        owner: "forged-later".into(),
        lease_ms: 30000,
    };
    let frontend =
        record
            .frontend_claim
            .as_ref()
            .map(|original| crate::FrontendContinuationRecovery {
                id: recovery.id.clone(),
                expected_revision: recovery.expected_revision,
                original: (**original).clone(),
                attempt: recovery.attempt.clone(),
                owner: recovery.owner.clone(),
                lease_ms: recovery.lease_ms,
            });
    record
        .capture_recovery(&recovery, frontend.as_ref())
        .unwrap();
    let parent = crate::postgres::attempt::load_host_attempt_with_proof(
        tx,
        &run.id,
        None,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap()
    .0
    .snapshot;
    let outcome = if frontend.is_some() {
        crate::ToolContinuationOutcome::FrontendReclaimedUnstarted {
            continuation: f.batch.continuation.clone(),
            original_claim_id: recovery.original.id.clone(),
            attempt: parent,
        }
    } else {
        crate::ToolContinuationOutcome::ReclaimedUnstarted {
            continuation: f.batch.continuation.clone(),
            original_claim_id: recovery.original.id.clone(),
            attempt: parent,
        }
    };
    crate::postgres::tool_continuation::write::save(tx, &record, false)
        .await
        .unwrap();
    crate::postgres::tool_continuation::write::publish(
        tx,
        run,
        recovery.id,
        record.latest_claim_source().unwrap().1,
        outcome,
        now,
    )
    .await
    .unwrap();
    crate::postgres::tool_continuation::current::verify_with_proof(
        tx,
        &record,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    assert!(
        Box::pin(tool::check(
            tx,
            run,
            &f.batch.continuation,
            &f.batch.next_attempt,
            &f.receipt,
            now,
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
}

async fn ordinary_later(
    tx: &mut Transaction<'_, Postgres>,
    run: &crabber_core::Run,
    f: &Fixture,
    request: &RegisterSuccessorExecutionDriver,
    now: time::OffsetDateTime,
) {
    sqlx::query("SAVEPOINT ordinary_later")
        .execute(&mut **tx)
        .await
        .unwrap();
    let ordinary = f
        .batch
        .calls
        .iter()
        .find(|v| {
            !crate::session_owner::driver::successor::tool_fresh::expected_phases(&f.batch)
                .any(|(target, _)| *target == v.target)
        })
        .unwrap();
    let mut extra = crate::postgres::phase::load_with_proof(
        tx,
        &f.receipt.run_id,
        &f.batch.frontend[0].target.id,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    extra.snapshot.target = crate::PhaseTarget {
        id: crabber_core::PhaseId::new(),
        generation: ordinary.target.generation.next().unwrap(),
        call_id: ordinary.target.call_id.clone(),
        attempt: f.batch.attempt.clone(),
    };
    extra.snapshot.call_binding = ordinary.call_binding.clone();
    extra.validate().unwrap();
    sqlx::query("INSERT INTO tool_phases(run_id,id,claim_token,state,quiescence_state,outcome_json,generation,call_id,attempt_id) VALUES($1,$2,$3,'Claimed','Pending',$4,$5,$6,$7)")
        .bind(&f.receipt.run_id.0).bind(&extra.snapshot.target.id.0).bind(&extra.token).bind(crate::phase::sql::encode_outcome(&extra).unwrap())
        .bind(i64::try_from(extra.snapshot.target.generation.get()).unwrap()).bind(&ordinary.target.call_id.0).bind(&f.batch.attempt.id.0).execute(&mut **tx).await.unwrap();
    let record = crate::postgres::tool_continuation::read::origin_with_proof(
        tx,
        &f.receipt.session_id,
        &f.receipt.run_id,
        None,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap()
    .unwrap();
    crate::postgres::tool_continuation::current::verify_with_proof(
        tx,
        &record,
        &mut Proof::bounded(32 << 20),
    )
    .await
    .unwrap();
    assert!(
        Box::pin(fresh::fresh(
            tx,
            request,
            None,
            now,
            &mut Proof::bounded(32 << 20)
        ))
        .await
        .is_err()
    );
    // The injected phase belongs to the OLD parent, so the current-parent phase count alone cannot reject it.
    assert_eq!(run.id, f.receipt.run_id);
    sqlx::query("ROLLBACK TO SAVEPOINT ordinary_later")
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("RELEASE SAVEPOINT ordinary_later")
        .execute(&mut **tx)
        .await
        .unwrap();
}

async fn status_carriers(
    tx: &mut Transaction<'_, Postgres>,
    f: &Fixture,
    request: &RegisterSuccessorExecutionDriver,
    now: time::OffsetDateTime,
) {
    for mutation in 0..5 {
        sqlx::query("SAVEPOINT status_carrier")
            .execute(&mut **tx)
            .await
            .unwrap();
        let extra = crabber_core::ToolCallRecord {
            id: crabber_core::ToolCallId::new(),
            run_id: f.receipt.run_id.clone(),
            name: "hidden".into(),
            arguments: serde_json::json!({"x": if mutation == 3 { "x".repeat(4 << 20) } else { String::new() }}),
            status: crabber_core::ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        let accounting = crate::sql_snapshot::call_record(&extra).unwrap();
        let mut value = serde_json::to_value(&extra).unwrap();
        match mutation {
            1 => value["status"] = serde_json::Value::Null,
            2 => value["status"] = serde_json::json!("unknown"),
            3 | 4 => value["status"] = serde_json::json!("completed"),
            _ => (),
        }
        let raw = if mutation == 4 {
            "{".to_owned()
        } else {
            serde_json::to_string(&value).unwrap()
        };
        sqlx::query("INSERT INTO tool_call_identities(id,kind) VALUES($1,'native')")
            .bind(&extra.id.0)
            .execute(&mut **tx)
            .await
            .unwrap();
        // The real writer rejects malformed JSON. Bypass only its snapshot trigger in this rolled-back reader-corruption fixture.
        if mutation == 4 {
            sqlx::query("ALTER TABLE tool_calls DISABLE TRIGGER snapshot_calls")
                .execute(&mut **tx)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO tool_calls(id,run_id,status,data,snapshot_record,snapshot_parts,snapshot_text,snapshot_bytes) VALUES($1,$2,'completed',$3,$4,$5,$6,$7)")
            .bind(&extra.id.0).bind(&f.receipt.run_id.0).bind(crate::postgres::json(&value).unwrap()).bind(&raw).bind(accounting.parts).bind(accounting.text).bind(i64::try_from(raw.len()).unwrap()).execute(&mut **tx).await.unwrap();
        let old_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running')",
        )
        .bind(&f.receipt.run_id.0)
        .fetch_one(&mut **tx)
        .await
        .unwrap();
        assert_eq!(
            usize::try_from(old_count).unwrap(),
            f.batch.calls.len(),
            "the indexed-only count hides this extra carrier"
        );
        assert!(
            Box::pin(fresh::fresh(
                tx,
                request,
                None,
                now,
                &mut Proof::bounded(32 << 20)
            ))
            .await
            .is_err(),
            "status carrier mutation={mutation}"
        );
        sqlx::query("ROLLBACK TO SAVEPOINT status_carrier")
            .execute(&mut **tx)
            .await
            .unwrap();
        sqlx::query("RELEASE SAVEPOINT status_carrier")
            .execute(&mut **tx)
            .await
            .unwrap();
    }
}
