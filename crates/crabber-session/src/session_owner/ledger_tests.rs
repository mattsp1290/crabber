use super::*;
use crate::session_owner::{SessionOwnerCommand, SessionOwnerTermination};
#[test]
fn session_owner_missing_or_rolled_back_current_evidence_fails_closed() {
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    let SessionOwnerOutcome::Applied {
        receipt: reserved,
        authority,
    } = ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap()
    else {
        panic!("fresh owner")
    };
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.binding.session_id.clone(),
        target: reserved.owner.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    ledger
        .mutate(&begin, authority.as_ref(), OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let key = (
        request.binding.session_id.clone(),
        request.binding.id.clone(),
    );
    let correct = ledger.clone();
    for kind in 0..5 {
        ledger = correct.clone();
        match kind {
            0 => {
                ledger.records.get_mut(&key).unwrap().snapshot.state =
                    SessionOwnerState::ReservedUnstarted;
                ledger.records.get_mut(&key).unwrap().ever_began = false;
            }
            1 => {
                let record = ledger.records.get_mut(&key).unwrap();
                record.snapshot.state = SessionOwnerState::Joined;
                record.snapshot.termination = Some(SessionOwnerTermination::Returned);
                record.token.clear();
            }
            2 => {
                ledger.records.remove(&key);
            }
            3 => {
                ledger.records.get_mut(&key).unwrap().lease_until += time::Duration::days(1);
            }
            _ => {
                ledger.contexts.remove(&request.binding.session_id);
            }
        }
        assert_eq!(
            ledger.snapshot(&request.binding.session_id, &request.binding.id),
            Err(Error::UnknownStoreOutcome),
            "case {kind}"
        );
        assert_eq!(
            ledger
                .receipt(&request.binding.session_id, &request.id)
                .unwrap(),
            Some(reserved.clone())
        );
        assert!(matches!(
            ledger
                .reserve(&request, OffsetDateTime::UNIX_EPOCH)
                .unwrap(),
            SessionOwnerOutcome::Replayed(_)
        ));
        let fresh = crate::session_owner_contract::reservation(request.binding.session_id.clone());
        let outcome = ledger.reserve(&fresh, OffsetDateTime::UNIX_EPOCH);
        if kind == 2 || kind == 4 {
            assert!(
                matches!(outcome, Err(Error::UnknownStoreOutcome)),
                "case {kind}"
            );
        } else {
            assert!(
                matches!(outcome, Ok(SessionOwnerOutcome::Applied { .. })),
                "case {kind}"
            );
        }
    }
}
#[test]
fn session_owner_oversized_current_identity_is_rejected_before_clone() {
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    ledger
        .records
        .get_mut(&(
            request.binding.session_id.clone(),
            request.binding.id.clone(),
        ))
        .unwrap()
        .snapshot
        .binding
        .directory = "private/".repeat(150_000);
    let allocations = allocation_counter::measure(|| {
        assert_eq!(
            ledger.snapshot(&request.binding.session_id, &request.binding.id),
            Err(Error::UnknownStoreOutcome)
        );
    });
    assert!(
        allocations.bytes_total < 8192,
        "unexpected materialization: {}",
        allocations.bytes_total
    );
}

#[test]
fn session_owner_scope_checks_identity_even_when_current_count_matches() {
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let original = (
        request.binding.session_id.clone(),
        request.binding.id.clone(),
    );
    let record = ledger.records.remove(&original).unwrap();
    ledger
        .records
        .insert((original.0.clone(), SessionOwnerId::new()), record);
    let fresh = crate::session_owner_contract::reservation(original.0);
    assert!(matches!(
        ledger.reserve(&fresh, OffsetDateTime::UNIX_EPOCH),
        Err(Error::UnknownStoreOutcome)
    ));
}

#[test]
fn session_owner_fresh_reservation_does_not_materialize_unrelated_private_records() {
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    ledger
        .records
        .get_mut(&(
            request.binding.session_id.clone(),
            request.binding.id.clone(),
        ))
        .unwrap()
        .snapshot
        .binding
        .directory = "private/".repeat(150_000);
    let fresh = crate::session_owner_contract::reservation(request.binding.session_id);
    let allocations = allocation_counter::measure(|| {
        assert!(matches!(
            ledger.reserve(&fresh, OffsetDateTime::UNIX_EPOCH),
            Ok(SessionOwnerOutcome::Applied { .. })
        ));
    });
    assert!(
        allocations.bytes_total < 32768,
        "unexpected historical materialization: {}",
        allocations.bytes_total
    );
}

#[test]
fn session_owner_context_preserves_opaque_newlines_and_refuses_nul() {
    let mut request = crate::session_owner_contract::reservation(SessionId::new());
    request.binding.directory = "opaque\ncontext".into();
    let mut ledger = Ledger::default();
    ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    ledger
        .context(
            &request.binding.session_id,
            &request.binding.workspace_id,
            "opaque\ncontext",
        )
        .unwrap();
    assert_eq!(
        ledger.context(
            &request.binding.session_id,
            &request.binding.workspace_id,
            "opaque context"
        ),
        Err(Error::IdentityMismatch)
    );
    request.binding.directory.push('\0');
    assert_eq!(request.fingerprint(), Err(Error::InvalidRequest));
}

#[test]
fn session_owner_deleted_audit_cannot_reuse_independent_publication_key() {
    let now = OffsetDateTime::UNIX_EPOCH;
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    let SessionOwnerOutcome::Applied {
        receipt, authority, ..
    } = ledger.reserve(&request, now).unwrap()
    else {
        panic!("fresh owner")
    };
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.binding.session_id.clone(),
        target: receipt.owner.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    ledger.mutate(&begin, authority.as_ref(), now).unwrap();
    ledger
        .mutate(
            &MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: begin.session_id.clone(),
                target: begin.target.clone(),
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Returned,
                },
            },
            authority.as_ref(),
            now,
        )
        .unwrap();
    ledger.erase_historical_audit_for_test(&begin.session_id, &begin.id);
    let original_roster = ledger.publications.clone();
    let mut foreign = crate::session_owner_contract::reservation(begin.session_id.clone());
    foreign.id = begin.id;
    assert!(matches!(
        ledger.reserve(&foreign, now),
        Err(Error::SemanticConflict)
    ));
    assert_eq!(ledger.publications, original_roster);
    assert!(
        !ledger
            .records
            .contains_key(&(begin.session_id, foreign.binding.id))
    );
}
