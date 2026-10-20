//! Real Ledger transitions, not source-policy or task-ownership positive proofs.
use super::{Failure, FixedInitialOwnerRole, Ledger, Proof, SessionId};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerOutcome, SessionOwnerTermination,
    audit::RevisionState,
    preparation::{PreparationKey, PreparationPurpose, ReservePreparation},
};
use crabber_core::{ControlId, InputFingerprint, SessionOwnerOperationId, SessionRevision};
use time::OffsetDateTime;
const MAX: usize = 32 << 20;

fn preparation(renew: bool) -> (Ledger, SessionId, [SessionOwnerOperationId; 3]) {
    let session = SessionId::new();
    let key = PreparationKey::Unkeyed(ControlId::new());
    let request = ReservePreparation {
        key: key.clone(),
        source: None,
        resource_contract: None,
        owner: crate::session_owner_contract::reservation(session.clone()),
        expected_revision: SessionRevision::INITIAL,
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: InputFingerprint::new("a".repeat(64)).unwrap(),
    }
    .bind_key(key)
    .unwrap();
    let now = OffsetDateTime::UNIX_EPOCH;
    let mut ledger = Ledger::default();
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = ledger
        .reserve_with_origin(
            &request.owner,
            None,
            now,
            RevisionState::Absent(0),
            Some(crate::session_owner::preparation::origin(&request)),
        )
        .unwrap()
    else {
        panic!("actual fresh preparation authority")
    };
    let ids = request.reconciliation().unwrap();
    for (id, command) in [
        (ids.begin_id.clone(), SessionOwnerCommand::Begin),
        (
            SessionOwnerOperationId::new(),
            SessionOwnerCommand::Renew { lease_ms: 1000 },
        ),
        (
            ids.join_id.clone(),
            SessionOwnerCommand::Join {
                termination: SessionOwnerTermination::Returned,
            },
        ),
    ] {
        if !renew && matches!(command, SessionOwnerCommand::Renew { .. }) {
            continue;
        }
        ledger
            .mutate(
                &MutateSessionOwner {
                    id,
                    session_id: session.clone(),
                    target: authority.target().clone(),
                    command,
                },
                Some(&authority),
                now,
            )
            .unwrap();
    }
    (
        ledger,
        session,
        [request.owner.id, ids.begin_id, ids.join_id],
    )
}
fn role<'a>(
    ledger: &'a Ledger,
    session: &SessionId,
    ids: &[SessionOwnerOperationId; 3],
) -> FixedInitialOwnerRole<'a> {
    FixedInitialOwnerRole {
        reserve: ledger.source_audit(session, &ids[0]).unwrap(),
        begin: ledger.source_audit(session, &ids[1]).unwrap(),
        activation: None,
        joined: ledger.source_audit(session, &ids[2]).unwrap(),
    }
}
#[test]
fn exact_original_role_and_actual_whole_fold_n_minus_one() {
    let (ledger, session, ids) = preparation(false);
    let before = ledger.raw_initial_snapshot(&session);
    let roles = [role(&ledger, &session, &ids)];
    let mut baseline = Proof::bounded(MAX);
    assert!(
        !ledger
            .uncovered_fixed_initial_history(&session, &roles, &mut baseline)
            .unwrap()
    );
    let n = baseline.used();
    assert!(
        !ledger
            .uncovered_fixed_initial_history(&session, &roles, &mut Proof::bounded(n))
            .unwrap()
    );
    assert!(matches!(
        ledger.uncovered_fixed_initial_history(&session, &roles, &mut Proof::bounded(n - 1)),
        Err(Failure::Limited(_))
    ));
    assert_eq!(ledger.raw_initial_snapshot(&session), before);
}
#[test]
fn genuine_extra_renew_is_uncovered_even_with_exact_retired_generation() {
    let (ledger, session, ids) = preparation(true);
    let roles = [role(&ledger, &session, &ids)];
    // The supplied exact current Join still authenticates. Only the independent
    // full history traversal detects this extra genuine same-generation audit.
    ledger
        .checked_for_read(
            &session,
            &roles[0].joined.record.snapshot.target.id,
            &roles[0].joined.record,
            &mut Proof::bounded(MAX),
        )
        .unwrap();
    assert!(
        ledger
            .uncovered_fixed_initial_history(&session, &roles, &mut Proof::bounded(MAX))
            .unwrap()
    );
}
#[test]
fn role_sources_and_current_are_independently_selected_and_restored() {
    let (mut ledger, session, ids) = preparation(false);
    let genuine = ledger.clone();
    let held = role(&genuine, &session, &ids);
    let target = held.joined.record.snapshot.target.id.clone();
    let before = ledger.raw_initial_snapshot(&session);
    for mode in 0..5 {
        match mode {
            0 => {
                ledger.audits.remove(&(session.clone(), ids[0].clone()));
            }
            1 => {
                ledger.audits.remove(&(session.clone(), ids[2].clone()));
            }
            2 => {
                ledger
                    .records
                    .get_mut(&(session.clone(), target.clone()))
                    .unwrap()
                    .token = "not-cleared".into();
            }
            3 => {
                let audit = ledger
                    .audits
                    .remove(&(session.clone(), ids[1].clone()))
                    .unwrap();
                ledger
                    .audits
                    .insert((SessionId::new(), ids[1].clone()), audit);
            }
            _ => {
                ledger
                    .records
                    .get_mut(&(session.clone(), target.clone()))
                    .unwrap()
                    .snapshot
                    .updated_at += time::Duration::seconds(1);
            }
        }
        let outcome = ledger.uncovered_fixed_initial_history(
            &session,
            &[role(&genuine, &session, &ids)],
            &mut Proof::bounded(MAX),
        );
        ledger = genuine.clone();
        assert!(
            outcome.is_err(),
            "retained source cannot cover altered native history, mode {mode}"
        );
        assert_eq!(ledger.raw_initial_snapshot(&session), before);
    }
    assert!(
        ledger
            .uncovered_fixed_initial_history(
                &session,
                &[
                    role(&genuine, &session, &ids),
                    role(&genuine, &session, &ids)
                ],
                &mut Proof::bounded(MAX)
            )
            .is_err()
    );
}
#[test]
fn genuine_reclaims_preserve_all_original_generations_in_the_fold() {
    let session = SessionId::new();
    let reservation = crate::session_owner_contract::reservation(session.clone());
    let mut ledger = Ledger::default();
    let now = OffsetDateTime::UNIX_EPOCH;
    let SessionOwnerOutcome::Applied { receipt, .. } = ledger.reserve(&reservation, now).unwrap()
    else {
        panic!("actual fresh owner")
    };
    let mut target = receipt.owner.target;
    let mut now = now + time::Duration::days(1);
    let mut middle = None;
    let mut authority = None;
    for _ in 0..2 {
        let id = SessionOwnerOperationId::new();
        let (
            SessionOwnerOutcome::Applied {
                receipt,
                authority: replacement,
            },
            _,
        ) = ledger
            .mutate(
                &MutateSessionOwner {
                    id: id.clone(),
                    session_id: session.clone(),
                    target,
                    command: SessionOwnerCommand::ReclaimUnstarted {
                        owner: "genuine replacement".into(),
                        lease_ms: 1000,
                    },
                },
                None,
                now,
            )
            .unwrap()
        else {
            panic!("actual fresh reclaimed authority")
        };
        middle.get_or_insert(id);
        target = receipt.owner.target;
        authority = replacement;
        now += time::Duration::seconds(2);
    }
    now -= time::Duration::seconds(2);
    for command in [
        SessionOwnerCommand::Begin,
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    ] {
        ledger
            .mutate(
                &MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target: target.clone(),
                    command,
                },
                authority.as_ref(),
                now,
            )
            .unwrap();
    }
    assert_eq!(target.generation.get(), 3);
    assert!(
        ledger
            .uncovered_fixed_initial_history(&session, &[], &mut Proof::bounded(MAX))
            .unwrap()
    );
    let genuine = ledger.clone();
    let audit = ledger
        .audits
        .get_mut(&(session.clone(), middle.unwrap()))
        .unwrap();
    assert_eq!(audit.record.snapshot.target.generation.get(), 2);
    // Corrupt only the actual historical middle Reclaim, retaining generation3's
    // genuine current Join and generation1's original Reserve.
    audit.record.snapshot.target.generation = crabber_core::SessionOwnerGeneration::INITIAL;
    audit.receipt.owner.target.generation = crabber_core::SessionOwnerGeneration::INITIAL;
    let outcome = ledger.uncovered_fixed_initial_history(&session, &[], &mut Proof::bounded(MAX));
    ledger = genuine;
    assert!(matches!(outcome, Err(Failure::Error(_))));
    assert!(
        ledger
            .uncovered_fixed_initial_history(&session, &[], &mut Proof::bounded(MAX))
            .unwrap()
    );
}
