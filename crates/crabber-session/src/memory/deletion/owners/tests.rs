use super::*;
use crate::session_owner::coverage::contract::{Mode, fixture};
use crate::{MemoryStore, PublicSessionLimit};
fn check(state: &State, session: &SessionId, cap: usize) -> Result<CheckedOwners, Failure> {
    read(
        state,
        session,
        &mut Budget::new(65536, PublicSessionLimit::HeaderBytes),
        &mut Proof::bounded(cap),
    )
}
#[tokio::test]
async fn detached_owner_coverage_requires_original_declaration_activation_and_parent() {
    let store = MemoryStore::new();
    for (mode, unknown_history) in [
        (Mode::Declared, false),
        (Mode::ActivatedNoDispatch, false),
        (Mode::Legacy, true),
        (Mode::Claim, true),
        (Mode::NoActivation, true),
    ] {
        let (session, _) = fixture(&store, mode).await;
        let mut state = store.state.lock().unwrap();
        let owners = check(&state, &session, 32 * 1024 * 1024).unwrap();
        assert_eq!(owners.live.len(), 0);
        assert_eq!(owners.unclassified, unknown_history);
        assert!(matches!(
            check(&state, &session, 1),
            Err(Failure::Limited(_))
        ));
        if !unknown_history {
            let (cursor, original) = state
                .native_parent_witnesses
                .iter()
                .find(|(_, w)| {
                    w.source.session_id == session
                        && w.source.kind == crate::native_parent::Kind::Phase
                })
                .map(|(key, value)| (*key, value.clone()))
                .unwrap();
            state.native_parent_witnesses.remove(&cursor);
            assert!(matches!(
                check(&state, &session, 32 * 1024 * 1024),
                Err(Failure::Error(_))
            ));
            let mut changed = original.clone();
            changed.run_token = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
            state.native_parent_witnesses.insert(cursor, changed);
            assert!(matches!(
                check(&state, &session, 32 * 1024 * 1024),
                Err(Failure::Error(_))
            ));
            let key = (
                session.clone(),
                original.source.run_id.clone(),
                original.run_issuance,
            );
            let issuance = state.native_run_issuances.get(&key).unwrap().clone();
            let mut changed_issuer = issuance.clone();
            changed_issuer.state.token =
                crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
            state
                .native_parent_witnesses
                .insert(cursor, original.clone());
            state
                .native_run_issuances
                .insert(key.clone(), changed_issuer);
            assert!(matches!(
                check(&state, &session, 32 * 1024 * 1024),
                Err(Failure::Error(_))
            ));
            state.native_run_issuances.insert(key, issuance);
            state.native_parent_witnesses.insert(cursor, original);
            assert!(
                !check(&state, &session, 32 * 1024 * 1024)
                    .unwrap()
                    .unclassified
            );
        }
    }
}

#[tokio::test]
async fn legacy_native_parent_remains_usable_but_unclassified() {
    use crate::session_owner::coverage::contract::{fixture_from_parent, native_parent};
    let store = MemoryStore::new();
    let (request, parent) = native_parent(&store).await;
    let session = request.binding.session_id.clone();
    {
        let mut state = store.state.lock().unwrap();
        let witness = state
            .native_parent_witnesses
            .values_mut()
            .find(|w| {
                w.source.session_id == session && w.source.kind == crate::native_parent::Kind::Phase
            })
            .unwrap();
        witness.version = 1;
        witness.run_issuance_digest = None;
    }
    fixture_from_parent(&store, Mode::Declared, request, parent).await;
    let state = store.state.lock().unwrap();
    let owners = check(&state, &session, 32 * 1024 * 1024).unwrap();
    assert_eq!(owners.live.len(), 0);
    assert!(owners.unclassified);
}
