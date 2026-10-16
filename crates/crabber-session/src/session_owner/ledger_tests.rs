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

#[test]
fn cancellation_sources_and_absence_are_independently_authenticated() {
    use crate::session_owner::cancellation_contract::{observe, request};
    let reservation = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    let SessionOwnerOutcome::Applied {
        receipt: reserved,
        authority: Some(authority),
    } = ledger
        .reserve(&reservation, OffsetDateTime::UNIX_EPOCH)
        .unwrap()
    else {
        panic!("reserve")
    };
    let request = request(&reserved.owner, None);
    let revision = RevisionState::Absent(ledger.publication_count(&request.session_id));
    ledger
        .request_cancel(&request, revision, OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.session_id.clone(),
        target: request.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    ledger
        .mutate(&begin, Some(&authority), OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let key = (request.session_id.clone(), request.target.id.clone());
    let unobserved = ledger.records[&key].clone();
    let observation = observe(&request);
    let revision = RevisionState::Absent(ledger.publication_count(&request.session_id));
    ledger
        .observe_cancel(
            &observation,
            &authority,
            revision,
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
    assert_eq!(ledger.records[&key].token, unobserved.token);
    assert_eq!(ledger.records[&key].lease_until, unobserved.lease_until);
    let renew = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: request.session_id.clone(),
        target: request.target.clone(),
        command: SessionOwnerCommand::Renew { lease_ms: 100 },
    };
    ledger
        .mutate(&renew, Some(&authority), OffsetDateTime::UNIX_EPOCH)
        .unwrap();
    let correct = ledger.clone();
    for case in 0..6 {
        let mut ledger = correct.clone();
        corrupt_cancellation_evidence(&mut ledger, &key, &request, &observation, &unobserved, case);
        assert_eq!(
            ledger.snapshot(&request.session_id, &request.target.id),
            Err(Error::UnknownStoreOutcome),
            "case{case}"
        );
        assert_eq!(
            ledger.pending_cancel(&authority),
            Err(Error::UnknownStoreOutcome),
            "case{case}"
        );
        let before = ledger.publication_count(&request.session_id);
        let mut fresh = request.clone();
        fresh.id = SessionOwnerOperationId::new();
        assert!(
            matches!(
                ledger.request_cancel(
                    &fresh,
                    RevisionState::Absent(before),
                    OffsetDateTime::UNIX_EPOCH
                ),
                Err(Error::UnknownStoreOutcome)
            ),
            "case{case}"
        );
        assert_eq!(ledger.publication_count(&request.session_id), before);
        assert!(
            matches!(ledger.reserve(&reservation,OffsetDateTime::UNIX_EPOCH).unwrap(),SessionOwnerOutcome::Replayed(receipt) if receipt==reserved)
        );
        if case < 3 || case == 4 {
            assert!(matches!(
                ledger
                    .request_cancel(
                        &request,
                        RevisionState::Absent(before),
                        OffsetDateTime::UNIX_EPOCH
                    )
                    .unwrap(),
                SessionOwnerOutcome::Replayed(_)
            ));
        }
    }
}

fn corrupt_cancellation_evidence(
    ledger: &mut Ledger,
    key: &(SessionId, SessionOwnerId),
    request: &crate::session_owner::RequestSessionOwnerCancel,
    observation: &crate::session_owner::ObserveSessionOwnerCancel,
    unobserved: &Record,
    case: u8,
) {
    match case {
        0 => {
            let record = ledger.records.get_mut(key).unwrap();
            record.cancellation = None;
            record.snapshot.cancellation = None;
        }
        1 => ledger
            .records
            .get_mut(key)
            .unwrap()
            .cancellation
            .as_mut()
            .unwrap()
            .request
            .claim
            .authority_claim
            .push('!'),
        2 => {
            ledger.records.insert(key.clone(), unobserved.clone());
        }
        3 => {
            ledger
                .audits
                .remove(&(request.session_id.clone(), request.id.clone()));
        }
        4 => {
            ledger
                .audits
                .remove(&(request.session_id.clone(), observation.id.clone()));
        }
        _ => {
            ledger
                .audits
                .get_mut(&(request.session_id.clone(), request.id.clone()))
                .unwrap()
                .record
                .owner = "x".repeat(1024 * 1024);
        }
    }
}

#[test]
fn aggregate_owner_membership_authenticates_joined_filter_and_sentinel() {
    use crate::public_session::{
        budget::Budget,
        v2::proof::{Failure, Proof},
    };
    use crate::{PublicSessionLimit, PublicSessionLimitV2};
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let mut ledger = Ledger::default();
    let SessionOwnerOutcome::Applied { receipt, authority } = ledger
        .reserve(&request, OffsetDateTime::UNIX_EPOCH)
        .unwrap()
    else {
        panic!("fresh")
    };
    let read = |ledger: &Ledger, limit, cap| {
        let mut safe = Budget::new(65536, PublicSessionLimit::HeaderBytes);
        let mut proof = Proof::bounded(cap);
        ledger.public_registered(&request.binding.session_id, limit, &mut safe, &mut proof)
    };
    assert_eq!(
        read(&ledger, 1, 65536).unwrap()[0].target,
        receipt.owner.target
    );
    assert!(matches!(
        read(&ledger, 0, 65536),
        Err(Failure::Limited(PublicSessionLimitV2::RegisteredOwners))
    ));
    assert!(matches!(
        read(&ledger, 1, 1),
        Err(Failure::Limited(PublicSessionLimitV2::ProofBytes))
    ));
    let valid = ledger.clone();
    let key = (
        request.binding.session_id.clone(),
        request.binding.id.clone(),
    );
    let row = ledger.records.get_mut(&key).unwrap();
    row.snapshot.state = SessionOwnerState::Joined;
    row.snapshot.termination = Some(SessionOwnerTermination::Returned);
    row.token.clear();
    // Even a zero-count caller must authenticate this alleged Joined sentinel.
    assert!(matches!(
        read(&ledger, 0, 65536),
        Err(Failure::Error(
            crate::PublicSessionError::UnknownStoreOutcome
        ))
    ));
    ledger = valid.clone();
    ledger.records.remove(&key);
    assert!(matches!(
        read(&ledger, 1, 65536),
        Err(Failure::Error(
            crate::PublicSessionError::UnknownStoreOutcome
        ))
    ));
    ledger = valid;
    ledger
        .mutate(
            &MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: request.binding.session_id.clone(),
                target: receipt.owner.target,
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Cancelled,
                },
            },
            authority.as_ref(),
            OffsetDateTime::UNIX_EPOCH,
        )
        .unwrap();
    assert!(read(&ledger, 0, 65536).unwrap().is_empty());
}

#[test]
fn aggregate_owner_proof_is_charged_before_private_clone() {
    use crate::public_session::{
        budget::Budget,
        v2::proof::{Failure, Proof},
    };
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
        .owner = "private-carrier-marker".repeat(100_000);
    let allocations = allocation_counter::measure(|| {
        let mut safe = Budget::new(65536, crate::PublicSessionLimit::HeaderBytes);
        let mut proof = Proof::bounded(128);
        assert!(matches!(
            ledger.public_registered(&request.binding.session_id, 1, &mut safe, &mut proof),
            Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
        ));
    });
    assert!(
        allocations.bytes_total < 8192,
        "{}",
        allocations.bytes_total
    );
}

#[test]
fn aggregate_owner_invalid_sentinel_does_not_become_limited() {
    use crate::public_session::{
        budget::Budget,
        v2::proof::{Failure, Proof},
    };
    let session = SessionId::new();
    let mut ledger = Ledger::default();
    let mut requests = Vec::new();
    for id in ["owner-a", "owner-b"] {
        let mut request = crate::session_owner_contract::reservation(session.clone());
        request.binding.id = crabber_core::SessionOwnerId::from(id);
        ledger
            .reserve(&request, OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        requests.push(request);
    }
    let record = ledger
        .records
        .get_mut(&(session.clone(), requests[1].binding.id.clone()))
        .unwrap();
    record.snapshot.state = SessionOwnerState::Joined;
    record.snapshot.termination = Some(SessionOwnerTermination::Cancelled);
    record.token.clear();
    let mut safe = Budget::new(65536, crate::PublicSessionLimit::HeaderBytes);
    let mut proof = Proof::bounded(65536);
    assert!(matches!(
        ledger.public_registered(&session, 1, &mut safe, &mut proof),
        Err(Failure::Error(
            crate::PublicSessionError::UnknownStoreOutcome
        ))
    ));
}
