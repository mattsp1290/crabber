use super::{Ledger, SessionId};
use crate::public_session::v2::proof::Proof;
use time::OffsetDateTime;

#[test]
fn session_purge_keeps_other_owner_sources_and_global_publication_allocation() {
    let mut ledger = Ledger::default();
    let target = crate::session_owner_contract::reservation(SessionId::new());
    let survivor = crate::session_owner_contract::reservation(SessionId::new());
    let now = OffsetDateTime::UNIX_EPOCH;
    ledger.reserve(&survivor, now).unwrap();
    ledger.reserve(&target, now).unwrap();
    let before = ledger
        .receipt(&survivor.binding.session_id, &survivor.id)
        .unwrap();
    let high = ledger.next_publication;
    ledger
        .purge_session(&target.binding.session_id, &mut Proof::bounded(1 << 20))
        .unwrap();
    assert!(!ledger.contains_scope(&target.binding.session_id));
    assert_eq!(ledger.publication_count(&target.binding.session_id), 0);
    assert_eq!(
        ledger
            .receipt(&survivor.binding.session_id, &survivor.id)
            .unwrap(),
        before
    );
    assert_eq!(ledger.next_publication, high);
    let later = crate::session_owner_contract::reservation(SessionId::new());
    ledger.reserve(&later, now).unwrap();
    assert_eq!(ledger.next_publication, high + 1);
    assert_eq!(
        ledger.publications[&(later.binding.session_id, later.id)].0,
        high + 1
    );
}

#[test]
fn independent_publication_refuses_erased_owner_history_before_joined_filter() {
    let mut ledger = Ledger::default();
    let target = crate::session_owner_contract::reservation(SessionId::new());
    ledger.reserve(&target, OffsetDateTime::UNIX_EPOCH).unwrap();
    ledger.records.clear();
    ledger.audits.clear();
    ledger.contexts.clear();
    let session = &target.binding.session_id;
    assert!(ledger.contains_scope(session));
    let mut safe =
        crate::public_session::budget::Budget::new(1 << 20, crate::PublicSessionLimit::HeaderBytes);
    assert!(matches!(
        ledger.public_registered(session, 1024, &mut safe, &mut Proof::bounded(1 << 20)),
        Err(crate::public_session::v2::proof::Failure::Error(_))
    ));
    assert!(matches!(
        ledger.public_registered(session, 1024, &mut safe, &mut Proof::bounded(1)),
        Err(crate::public_session::v2::proof::Failure::Limited(_))
    ));
    assert!(ledger.validate_scope(session).is_err());
    assert!(
        ledger
            .purge_session(session, &mut Proof::bounded(1 << 20))
            .is_err()
    );
    assert_eq!(ledger.publication_count(session), 1);
}
