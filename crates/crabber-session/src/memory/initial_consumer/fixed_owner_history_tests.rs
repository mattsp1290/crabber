//! Defining owner fold exercised against real fixed source/native retirements.
use super::Proof;
use crate::memory::{MemoryStore, State};
use crate::public_session::v2::proof::Failure;
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
    owned_scope::{FixedMemoryInitialHost, FixedMemoryInitialInput, PendingInitialProducerStatus},
};
use crate::{
    DeleteSession, DeletionBlocker, DeletionError, DeletionOutcome, ManageSession, SessionChange,
    SessionDefaults, Store,
};
use crabber_core::{
    AdmissionKey, ControlId, InputEnvelope, SessionContext, SessionId, SessionOwnerOperationId,
    SessionRevision,
};
const MAX: usize = 32 << 20;

async fn completed(keyed: bool) -> (FixedMemoryInitialHost, SessionId) {
    let host = FixedMemoryInitialHost::new();
    let session = SessionId::new();
    let receipt = host
        .store()
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "owner-fold".into(),
                directory: "/owner-fold".into(),
                title: "actual fixed owner history".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let mut task = host
        .register_scripted(
            FixedMemoryInitialInput {
                context: SessionContext::new(session.clone(), "owner-fold", "/owner-fold"),
                title: "actual fixed owner fold".into(),
                input: InputEnvelope::text("actual source and owner generations", receipt.revision)
                    .unwrap(),
                key: keyed.then(|| AdmissionKey::new("actual-owner-fold").unwrap()),
            },
            super::tests::source(),
        )
        .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(
        observed.status(),
        Some(&PendingInitialProducerStatus::ClosedAssignment)
    );
    assert!(!observed.observation_failed());
    assert!(observed.error().is_none());
    drop(task);
    host.wait_for_supervision().await;
    (host, session)
}
fn fold(state: &State, session: &SessionId, proof: &mut Proof) -> Result<bool, Failure> {
    let run = state
        .runs
        .values()
        .find(|run| &run.session_id == session)
        .unwrap();
    let source = MemoryStore::fixed_closed_initial_source(state, run, proof)?.unwrap();
    state
        .session_owners
        .uncovered_fixed_initial_history(session, source.owner_roles(), proof)
}
#[tokio::test]
async fn actual_fixed_three_owner_roles_and_whole_fold_budget_are_exact() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let state = host.store().state.lock().unwrap();
        let before = state.session_owners.raw_initial_snapshot(&session);
        let mut proof = Proof::bounded(MAX);
        assert!(!fold(&state, &session, &mut proof).unwrap());
        let n = proof.used();
        assert!(!fold(&state, &session, &mut Proof::bounded(n)).unwrap());
        assert!(matches!(
            fold(&state, &session, &mut Proof::bounded(n - 1)),
            Err(Failure::Limited(_))
        ));
        assert_eq!(state.session_owners.raw_initial_snapshot(&session), before);
        let run = state
            .runs
            .values()
            .find(|run| run.session_id == session)
            .unwrap();
        let source =
            MemoryStore::fixed_closed_initial_source(&state, run, &mut Proof::bounded(MAX))
                .unwrap()
                .unwrap();
        let roles = source.owner_roles();
        assert_eq!(roles.len(), 3);
        assert!(roles[0].activation.is_none());
        assert!(roles[1].activation.is_some() && roles[2].activation.is_some());
        for role in roles {
            for source_id in [
                &role.reserve.receipt.operation_id,
                &role.joined.receipt.operation_id,
            ] {
                let mut changed = state.session_owners.clone();
                changed.erase_historical_audit_for_test(&session, source_id);
                let result = changed.uncovered_fixed_initial_history(
                    &session,
                    roles,
                    &mut Proof::bounded(MAX),
                );
                assert!(
                    result.is_err(),
                    "saved role cannot replace its actual native original"
                );
            }
            let mut changed = state.session_owners.clone();
            changed.corrupt_retirement_current_token_for_test(
                &session,
                &role.joined.record.snapshot.target.id,
            );
            assert!(
                changed
                    .uncovered_fixed_initial_history(&session, roles, &mut Proof::bounded(MAX))
                    .is_err()
            );
        }
        assert_eq!(state.session_owners.raw_initial_snapshot(&session), before);
    }
}
async fn deletion(
    host: &FixedMemoryInitialHost,
    session: &SessionId,
) -> Result<DeletionOutcome, DeletionError> {
    let revision = host.store().session_revision(session).await.unwrap();
    host.store()
        .delete_session(DeleteSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
        })
        .await
}
async fn extra_owner(host: &FixedMemoryInitialHost, session: &SessionId, mode: u8) {
    let mut reservation = crate::session_owner_contract::reservation(session.clone());
    reservation.binding.workspace_id = "owner-fold".into();
    reservation.binding.directory = "/owner-fold".into();
    reservation.lease_ms = if mode == 2 { 1 } else { 30_000 };
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(mut authority),
    } = host
        .store()
        .reserve_session_owner(reservation, None)
        .await
        .unwrap()
    else {
        panic!("real extra native owner")
    };
    let mut target = receipt.owner.target;
    if mode == 2 {
        // Actual SystemClock expiry, followed by native reclamation. No owner
        // generation, token or grant is constructed by the fixture.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let SessionOwnerOutcome::Applied {
            receipt,
            authority: Some(replacement),
        } = host
            .store()
            .mutate_session_owner(
                MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target,
                    command: SessionOwnerCommand::ReclaimUnstarted {
                        owner: "real reclaimed owner".into(),
                        lease_ms: 30_000,
                    },
                },
                None,
            )
            .await
            .unwrap()
        else {
            panic!("actual replacement grant")
        };
        target = receipt.owner.target;
        assert_eq!(target.generation.get(), 2);
        authority = replacement;
    }
    let commands = [
        SessionOwnerCommand::Begin,
        SessionOwnerCommand::Renew { lease_ms: 30_000 },
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    ];
    for command in commands {
        if mode != 1 && matches!(command, SessionOwnerCommand::Renew { .. }) {
            continue;
        }
        host.store()
            .mutate_session_owner(
                MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target: target.clone(),
                    command,
                },
                Some(authority.clone()),
            )
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn unrelated_joined_reclaimed_and_renewed_owners_keep_whole_readiness_unknown() {
    let (host, session) = completed(false).await;
    let genuine = host.store().state.lock().unwrap().clone();
    for mode in 0..3 {
        extra_owner(&host, &session, mode).await;
        {
            let state = host.store().state.lock().unwrap();
            // Direct fold assertion rules out an unrelated Run/source blocker
            // accidentally making this owner inventory negative vacuous.
            assert!(fold(&state, &session, &mut Proof::bounded(MAX)).unwrap());
        }
        let outcome = deletion(&host, &session).await;
        let retained = host.store().state.lock().unwrap().clone();
        *host.store().state.lock().unwrap() = genuine.clone();
        assert_eq!(
            outcome,
            Ok(DeletionOutcome::Blocked(DeletionBlocker::OwnershipUnknown))
        );
        assert!(!retained.tombstones.contains_key(&session));
        assert!(retained.sessions.contains_key(&session));
    }
    assert!(matches!(
        deletion(&host, &session).await.unwrap(),
        DeletionOutcome::Applied(_)
    ));
}
#[tokio::test]
async fn each_real_owner_join_and_cleared_current_is_required_by_public_deletion() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let genuine = host.store().state.lock().unwrap().clone();
        let run = genuine
            .runs
            .values()
            .find(|run| run.session_id == session)
            .unwrap();
        let source =
            MemoryStore::fixed_closed_initial_source(&genuine, run, &mut Proof::bounded(MAX))
                .unwrap()
                .unwrap();
        let before = genuine.session_owners.raw_initial_snapshot(&session);
        for role in source.owner_roles() {
            for clear_current in [false, true] {
                let mut changed = genuine.clone();
                if clear_current {
                    changed
                        .session_owners
                        .corrupt_retirement_current_token_for_test(
                            &session,
                            &role.joined.record.snapshot.target.id,
                        );
                } else {
                    changed.session_owners.erase_historical_audit_for_test(
                        &session,
                        &role.joined.receipt.operation_id,
                    );
                }
                let changed_native = changed.session_owners.raw_initial_snapshot(&session);
                *host.store().state.lock().unwrap() = changed;
                let outcome = deletion(&host, &session).await;
                let after = host.store().state.lock().unwrap().clone();
                *host.store().state.lock().unwrap() = genuine.clone();
                assert_eq!(outcome, Err(DeletionError::UnknownStoreOutcome));
                assert_eq!(
                    after.session_owners.raw_initial_snapshot(&session),
                    changed_native
                );
                assert!(!after.tombstones.contains_key(&session));
                assert!(after.sessions.contains_key(&session));
                assert_eq!(
                    host.store()
                        .state
                        .lock()
                        .unwrap()
                        .session_owners
                        .raw_initial_snapshot(&session),
                    before
                );
            }
        }
        assert!(matches!(
            deletion(&host, &session).await.unwrap(),
            DeletionOutcome::Applied(_)
        ));
    }
}
