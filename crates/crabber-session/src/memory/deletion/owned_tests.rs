use super::super::MemoryStore;
use crate::deletion::coverage::MAX_PROOF_BYTES;
use crate::public_session::v2::proof::{Failure, Proof};

fn bounded() -> Proof {
    Proof::bounded(MAX_PROOF_BYTES)
}
fn fails_before_mutation(state: &mut super::super::State, session: &crabber_core::SessionId) {
    assert!(matches!(
        super::owned::verify(state, session, &mut bounded()),
        Err(Failure::Error(_))
    ));
    let before = state.clone();
    assert!(matches!(
        super::purge::purge(state, session, &mut bounded()),
        Err(Failure::Error(_))
    ));
    assert!(state.preparation_admissions == before.preparation_admissions);
    assert_eq!(
        state.preparation_admission_joins,
        before.preparation_admission_joins
    );
    assert!(state.admission_execution_sources == before.admission_execution_sources);
    assert_eq!(
        state.admission_execution_protocols,
        before.admission_execution_protocols
    );
    assert_eq!(state.runs, before.runs);
    assert_eq!(
        state.events.next_cursor().unwrap(),
        before.events.next_cursor().unwrap()
    );
}
#[tokio::test]
async fn physical_purge_erases_exact_owned_witness_and_journal_preserving_survivor() {
    let store = MemoryStore::new();
    let first =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let second =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    super::owned::verify(&state, &first.session_id, &mut bounded()).unwrap();
    let survivor_witness = state.preparation_admissions[&second.run_id].clone();
    let survivor_source = state.admission_execution_sources
        [&(second.run_id.clone(), crate::admission_journal::Kind::Init)]
        .clone();
    super::purge::purge(&mut state, &first.session_id, &mut bounded()).unwrap();
    assert!(!state.preparation_admissions.contains_key(&first.run_id));
    assert!(state.preparation_admissions.get(&second.run_id) == Some(&survivor_witness));
    assert!(
        state
            .preparation_admission_joins
            .keys()
            .all(|(scope, _)| scope != &first.session_id)
    );
    assert!(!state.admission_execution_protocols.contains(&first.run_id));
    assert!(state.admission_execution_protocols.contains(&second.run_id));
    assert!(
        state
            .admission_execution_sources
            .keys()
            .all(|(run, _)| run != &first.run_id)
    );
    assert!(
        state
            .admission_execution_sources
            .get(&(second.run_id.clone(), crate::admission_journal::Kind::Init))
            == Some(&survivor_source)
    );
    super::owned::verify(&state, &second.session_id, &mut bounded()).unwrap();
}
#[tokio::test]
async fn owned_membership_rejects_removed_or_misrouted_each_new_source_before_purge() {
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let original = state.clone();
    for variation in 0..8 {
        *state = original.clone();
        match variation {
            0 => {
                state.preparation_admissions.remove(&receipt.run_id);
            }
            1 => {
                state.preparation_admission_joins.clear();
            }
            2 => {
                let key = state
                    .preparation_admission_joins
                    .keys()
                    .next()
                    .unwrap()
                    .clone();
                state
                    .preparation_admission_joins
                    .insert(key, crabber_core::RunId::new());
            }
            3 => {
                state.admission_execution_protocols.remove(&receipt.run_id);
            }
            4 => {
                state.admission_execution_sources.clear();
            }
            5 => {
                let init = state
                    .admission_execution_sources
                    .remove(&(receipt.run_id.clone(), crate::admission_journal::Kind::Init))
                    .unwrap();
                state.admission_execution_sources.insert(
                    (
                        receipt.run_id.clone(),
                        crate::admission_journal::Kind::Begin,
                    ),
                    init,
                );
            }
            6 => {
                state.runs.remove(&receipt.run_id);
            }
            7 => {
                let witness = state
                    .preparation_admissions
                    .remove(&receipt.run_id)
                    .unwrap();
                state
                    .preparation_admissions
                    .insert(crabber_core::RunId::new(), witness);
            }
            _ => unreachable!(),
        }
        fails_before_mutation(&mut state, &receipt.session_id);
    }
}
#[tokio::test]
async fn witness_and_journal_carriers_share_budget_before_source_validation() {
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let mut state = store.state.lock().unwrap();
    let before = state.preparation_admissions.len();
    assert!(matches!(
        super::owned::verify(&state, &receipt.session_id, &mut Proof::bounded(1)),
        Err(Failure::Limited(_))
    ));
    assert!(matches!(
        super::purge::purge(&mut state, &receipt.session_id, &mut Proof::bounded(1)),
        Err(Failure::Limited(_))
    ));
    assert_eq!(state.preparation_admissions.len(), before);
}

#[tokio::test]
async fn physical_purge_removes_both_actual_init_and_begin_sources() {
    use crate::Store;
    let store = MemoryStore::new();
    let keyed = crate::admission_execution_contract::keyed(
        &crabber_core::SessionId::new(),
        time::OffsetDateTime::now_utc(),
    );
    let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(keyed).await.unwrap()
    else {
        panic!("fresh keyed run")
    };
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    assert!(state.admission_execution_sources.contains_key(&(
        receipt.run_id.clone(),
        crate::admission_journal::Kind::Begin
    )));
    super::owned::verify(&state, &receipt.session_id, &mut bounded()).unwrap();
    super::purge::purge(&mut state, &receipt.session_id, &mut bounded()).unwrap();
    assert_eq!(state.admission_execution_sources.len(), 0);
    assert_eq!(state.admission_execution_protocols.len(), 0);
}
#[tokio::test]
async fn orphan_owned_source_cannot_turn_managed_empty_scope_into_ready() {
    use crate::Store;
    let store = MemoryStore::new();
    let owned =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    let crate::deletion::coverage::CoverageRead::Verified(coverage) =
        super::coverage(&state, &created.session_id).unwrap()
    else {
        panic!("managed origin")
    };
    let original = state.clone();
    for witness in [false, true] {
        *state = original.clone();
        if witness {
            let mut value =
                serde_json::to_value(&state.preparation_admissions[&owned.run_id]).unwrap();
            value["initial"]["session_id"] = serde_json::json!(created.session_id);
            state
                .preparation_admissions
                .insert(owned.run_id.clone(), serde_json::from_value(value).unwrap());
        } else {
            let source = &state.admission_execution_sources
                [&(owned.run_id.clone(), crate::admission_journal::Kind::Init)];
            let mut value = serde_json::to_value(source).unwrap();
            value["source"]["issuance"]["state"]["session_id"] =
                serde_json::json!(created.session_id);
            state.admission_execution_sources.insert(
                (owned.run_id.clone(), crate::admission_journal::Kind::Init),
                serde_json::from_value(value).unwrap(),
            );
        }
        assert!(matches!(
            super::readiness::read(&state, &coverage, &mut bounded()),
            Err(crate::DeletionError::UnknownStoreOutcome)
        ));
        fails_before_mutation(&mut state, &created.session_id);
    }
}
