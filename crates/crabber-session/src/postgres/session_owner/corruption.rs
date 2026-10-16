//! Mutable projections cannot choose or erase their immutable source evidence.
use super::*;
use crate::{Store, session_owner_contract};
use crabber_core::ManualClock;
use std::sync::Arc;
use time::OffsetDateTime;
pub(super) async fn run(store: &PostgresStore) {
    for corruption in 0..8 {
        let request = session_owner_contract::reservation(SessionId::new());
        let SessionOwnerOutcome::Applied { receipt, authority } = store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap()
        else {
            panic!("fresh")
        };
        let original: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&request.binding.session_id.0)
        .bind(&request.binding.id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        let begin = MutateSessionOwner {
            id: SessionOwnerOperationId::new(),
            session_id: request.binding.session_id.clone(),
            target: receipt.owner.target.clone(),
            command: SessionOwnerCommand::Begin,
        };
        store
            .mutate_session_owner(begin.clone(), authority)
            .await
            .unwrap();
        corrupt(store, &request, &begin, &original, corruption).await;
        assert_eq!(
            store
                .get_session_owner(&request.binding.session_id, &request.binding.id)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome,
            "corruption {corruption}"
        );
        if !matches!(corruption, 4 | 6) {
            assert!(
                matches!(store.reserve_session_owner(request.clone(),None).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
            );
        }
        if corruption == 6 {
            assert!(
                matches!(
                    store.reserve_session_owner(request.clone(), None).await,
                    Err(Error::UnknownStoreOutcome)
                ),
                "an altered original audit cannot authenticate metadata replay"
            );
            let mut fresh = request.clone();
            fresh.id = SessionOwnerOperationId::new();
            fresh.binding.id = SessionOwnerId::new();
            assert!(matches!(
                store.reserve_session_owner(fresh, None).await,
                Err(Error::UnknownStoreOutcome)
            ));
        }
    }
    unrelated_history(store).await;
    native_sources(store).await;
    restored_phase(store).await;
    immutable_capacity(store).await;
}
async fn corrupt(
    store: &PostgresStore,
    request: &ReserveSessionOwner,
    begin: &MutateSessionOwner,
    original: &str,
    corruption: u8,
) {
    let mut tx = store.pool.begin().await.unwrap();
    match corruption {
        0 => {
            sqlx::query(
                "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
            )
            .bind(&request.binding.session_id.0)
            .bind(&request.binding.id.0)
            .bind(original)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        1 => {
            assert_current_guard(&mut tx, request, false).await;
            sqlx::query("UPDATE session_owner_current SET data_json='{}' WHERE session_id=$1")
                .bind(&request.binding.session_id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        2 => {
            sqlx::query("UPDATE session_owner_current SET data_json=$2 WHERE session_id=$1")
                .bind(&request.binding.session_id.0)
                .bind("x".repeat(1_048_576))
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        3 => {
            sqlx::query("SAVEPOINT context_delete_guard")
                .execute(&mut *tx)
                .await
                .unwrap();
            // The retained identity cannot be erased; corrupt its mutable body instead.
            assert!(
                sqlx::query("DELETE FROM session_owner_contexts WHERE session_id=$1")
                    .bind(&request.binding.session_id.0)
                    .execute(&mut *tx)
                    .await
                    .is_err()
            );
            sqlx::query("ROLLBACK TO SAVEPOINT context_delete_guard")
                .execute(&mut *tx)
                .await
                .unwrap();
            sqlx::query("UPDATE session_owner_contexts SET data_json='{}' WHERE session_id=$1")
                .bind(&request.binding.session_id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        4 => {
            sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
                .bind(&request.binding.session_id.0)
                .bind(&request.id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        5 => {
            sqlx::query("DELETE FROM session_owner_audits WHERE session_id=$1 AND operation_id=$2")
                .bind(&request.binding.session_id.0)
                .bind(&begin.id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        6 => {
            assert_current_guard(&mut tx, request, true).await;
            // Original audit routing is still independently checked even
            // when the current identity cannot be erased or moved.
            sqlx::query("UPDATE session_owner_audits SET owner_id=$2 WHERE session_id=$1 AND operation_id=$3")
                .bind(&request.binding.session_id.0)
                .bind(SessionOwnerId::new().0)
                .bind(&request.id.0)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        7 => {
            sqlx::query("UPDATE session_owner_audits SET state='Joined' WHERE session_id=$1 AND operation_id=$2").bind(&request.binding.session_id.0).bind(&begin.id.0).execute(&mut *tx).await.unwrap();
        }
        _ => unreachable!(),
    }
    tx.commit().await.unwrap();
}
async fn assert_current_guard(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &ReserveSessionOwner,
    moving: bool,
) {
    sqlx::query("SAVEPOINT current_guard")
        .execute(&mut **tx)
        .await
        .unwrap();
    let query = if moving {
        "UPDATE session_owner_current SET owner_id=$2 WHERE session_id=$1"
    } else {
        "DELETE FROM session_owner_current WHERE session_id=$1 AND owner_id=$2"
    };
    sqlx::query(query)
        .bind(&request.binding.session_id.0)
        .bind(if moving {
            SessionOwnerId::new().0
        } else {
            request.binding.id.0.clone()
        })
        .execute(&mut **tx)
        .await
        .unwrap_err();
    sqlx::query("ROLLBACK TO SAVEPOINT current_guard")
        .execute(&mut **tx)
        .await
        .unwrap();
}
async fn unrelated_history(store: &PostgresStore) {
    let first = session_owner_contract::reservation(SessionId::new());
    store
        .reserve_session_owner(first.clone(), None)
        .await
        .unwrap();
    let mut request = first.clone();
    request.id = SessionOwnerOperationId::new();
    request.binding.id = SessionOwnerId::new();
    let SessionOwnerOutcome::Applied { receipt, authority } = store
        .reserve_session_owner(request.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.binding.session_id.clone(),
        target: receipt.owner.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    store
        .mutate_session_owner(begin.clone(), authority.clone())
        .await
        .unwrap();
    let join = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.binding.session_id.clone(),
        target: receipt.owner.target,
        command: SessionOwnerCommand::Join {
            termination: crate::session_owner::SessionOwnerTermination::Returned,
        },
    };
    store.mutate_session_owner(join, authority).await.unwrap();
    sqlx::query(
        "UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
    )
    .bind(&request.binding.session_id.0)
    .bind(&begin.id.0)
    .bind("x".repeat(1_048_576))
    .execute(&store.pool)
    .await
    .unwrap();
    let mut fresh = first;
    fresh.id = SessionOwnerOperationId::new();
    fresh.binding.id = SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(fresh, None).await.unwrap(),
        SessionOwnerOutcome::Applied { .. }
    ));
    assert_eq!(
        store
            .get_session_owner(&request.binding.session_id, &request.binding.id)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome,
        "selected new-cohort snapshot authenticates every retained publication",
    );
}
async fn native_sources(store: &PostgresStore) {
    for corruption in 0..6 {
        let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
        let store = store.clone().with_clock(clock.clone());
        let (request, parent, admitted, _execution) =
            session_owner_contract::parent::fixture(&store).await;
        let SessionOwnerOutcome::Applied { receipt, .. } = store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await
            .unwrap()
        else {
            panic!("fresh parent")
        };
        let mut fresh = request.clone();
        fresh.id = SessionOwnerOperationId::new();
        fresh.binding.id = SessionOwnerId::new();
        match corruption {
            0 => {
                sqlx::query(
                    "DELETE FROM native_parent_witnesses WHERE session_id=$1 AND kind='phase'",
                )
                .bind(&admitted.session.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            }
            1 => {
                sqlx::query(
                    "DELETE FROM native_parent_witnesses WHERE session_id=$1 AND kind='attempt'",
                )
                .bind(&admitted.session.id.0)
                .execute(&store.pool)
                .await
                .unwrap();
            }
            2 => {
                sqlx::query("DELETE FROM native_run_issuances WHERE session_id=$1")
                    .bind(&admitted.session.id.0)
                    .execute(&store.pool)
                    .await
                    .unwrap();
            }
            3 => {
                sqlx::query("UPDATE native_parent_witnesses SET data_json=$2 WHERE session_id=$1 AND kind='phase'").bind(&admitted.session.id.0).bind("x".repeat(1_048_576)).execute(&store.pool).await.unwrap();
            }
            4 => {
                // Real Run-only takeover rotates its journal without an Attempt event. Restoring
                // the old mutable Run must not revive the parent for a new handoff.
                let run = admitted.run.clone();
                clock.set(run.lease_until + time::Duration::seconds(1));
                store
                    .claim_expired_run(&run.id, "replacement")
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE runs SET status=$2,claim_token=$3,lease_until=$4,data=$5 WHERE id=$1",
                )
                .bind(&run.id.0)
                .bind(super::super::status(run.status))
                .bind(&run.claim_token)
                .bind(super::super::micros(run.lease_until))
                .bind(super::super::json(&run).unwrap())
                .execute(&store.pool)
                .await
                .unwrap();
                clock.set(OffsetDateTime::UNIX_EPOCH);
            }
            5 => {
                sqlx::query("UPDATE runs SET lease_until=0 WHERE id=$1")
                    .bind(&admitted.run.id.0)
                    .execute(&store.pool)
                    .await
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(
                store.reserve_session_owner(fresh, Some(parent)).await,
                Err(Error::UnknownStoreOutcome)
            ),
            "native corruption {corruption}"
        );
        assert!(
            matches!(store.reserve_session_owner(request,None).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
        );
    }
}

async fn restored_phase(store: &PostgresStore) {
    let (request, parent, admitted, execution) =
        session_owner_contract::parent::fixture(store).await;
    let target = request
        .binding
        .parent
        .as_ref()
        .unwrap()
        .phase
        .as_ref()
        .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    let original = super::super::phase::load_owned(&mut tx, &admitted.run.id, &target.id)
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    let crate::session_owner::SessionOwnerParentAuthority::Phase(authority) = parent.clone() else {
        unreachable!()
    };
    execution
        .mutate_phase(
            crate::PhaseMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                command: crate::PhaseCommand::Complete {
                    target: target.clone(),
                    output: crate::phase_contract::output(),
                },
            },
            Some(crate::PhaseWriteAuthority::Phase(authority)),
        )
        .await
        .unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    super::super::phase::save_host_record(&mut tx, &original, false)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        store.reserve_session_owner(request, Some(parent)).await,
        Err(Error::UnknownStoreOutcome)
    ));
}

async fn immutable_capacity(store: &PostgresStore) {
    let (first, authority) = session_owner_contract::capacity::fill(store).await;
    let original: String = sqlx::query_scalar(
        "SELECT data_json FROM session_owner_current WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&first.binding.session_id.0)
    .bind(&first.binding.id.0)
    .fetch_one(&store.pool)
    .await
    .unwrap();
    // A coherently changed mutable Joined record must not free capacity.
    let mut record: Record = serde_json::from_str(&original).unwrap();
    record.snapshot.state = SessionOwnerState::Joined;
    record.snapshot.termination = Some(crate::session_owner::SessionOwnerTermination::Returned);
    record.token.clear();
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&first.binding.session_id.0)
    .bind(&first.binding.id.0)
    .bind(serde_json::to_string(&record).unwrap())
    .execute(&store.pool)
    .await
    .unwrap();
    let mut next = first.clone();
    next.id = SessionOwnerOperationId::new();
    next.binding.id = SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(next, None).await,
        Err(Error::Capacity)
    ));
    sqlx::query(
        "UPDATE session_owner_current SET data_json=$3 WHERE session_id=$1 AND owner_id=$2",
    )
    .bind(&first.binding.session_id.0)
    .bind(&first.binding.id.0)
    .bind(original)
    .execute(&store.pool)
    .await
    .unwrap();
    session_owner_contract::capacity::joined_slot(store, first, authority).await;
}
