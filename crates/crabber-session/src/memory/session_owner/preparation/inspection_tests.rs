use super::*;
use crate::Store;
use crate::session_owner::preparation::inspection::contract;

#[tokio::test]
async fn original_initial_admission_inspection_is_pure_and_bounded() {
    let store = MemoryStore::new();
    contract::run(&store).await;
    let (original, admitted) = contract::fresh(&store).await;
    let mut proof = Proof::bounded(inspection::MAX_PROOF);
    let expected = store
        .inspect_initial_admission_with_proof(&original, &mut proof)
        .unwrap();
    let used = proof.used();
    assert_eq!(
        store
            .inspect_initial_admission_with_proof(&original, &mut Proof::bounded(used))
            .unwrap(),
        expected
    );
    assert!(matches!(
        store.inspect_initial_admission_with_proof(&original, &mut Proof::bounded(used - 1)),
        Err(PreparationCommitError::Owner(Error::Capacity))
    ));
    // Neither today's private Run body nor later head is original-source proof.
    {
        let mut state = store.state.lock().unwrap();
        state.runs.get_mut(&admitted.run.id).unwrap().error =
            Some("private-current".repeat(3_000_000));
        state
            .native_run_source_heads
            .remove(&(admitted.session.id.clone(), admitted.run.id.clone()));
    }
    assert_eq!(
        store
            .inspect_initial_preparation_admission(&original)
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn initial_inspection_rejects_missing_or_modified_original_sources() {
    for fault in [
        "index", "witness", "issuer", "source", "foreign", "protocol",
    ] {
        let store = MemoryStore::new();
        let (original, admitted) = contract::fresh(&store).await;
        {
            let mut state = store.state.lock().unwrap();
            let run = &admitted.run.id;
            let session = &admitted.session.id;
            match fault {
                "protocol" => {
                    state
                        .native_run_protocols
                        .remove(&(session.clone(), run.clone()));
                }
                "index" => {
                    state.preparation_admission_joins.clear();
                }
                "witness" => {
                    state.preparation_admissions.remove(run);
                }
                "issuer" => {
                    state
                        .native_run_issuances
                        .get_mut(&(session.clone(), run.clone(), 1))
                        .unwrap()
                        .state
                        .token = crabber_core::InputFingerprint::new("c".repeat(64)).unwrap();
                }
                "source" => {
                    state
                        .native_run_sources
                        .remove(&(session.clone(), run.clone(), 1));
                }
                "foreign" => {
                    let value = state.preparation_admissions.get_mut(run).unwrap();
                    let mut text = serde_json::to_value(&*value).unwrap();
                    text["initial"]["session_id"] = serde_json::json!("foreign");
                    *value = serde_json::from_value(text).unwrap();
                }
                _ => unreachable!(),
            }
        }
        assert!(
            store
                .inspect_initial_preparation_admission(&original)
                .await
                .is_err(),
            "{fault}"
        );
    }
}

#[tokio::test]
async fn initial_inspection_never_upgrades_legacy_originals() {
    let store = MemoryStore::new();
    let (original, admitted) = contract::fresh(&store).await;
    {
        // An honest pre-protocol cohort has neither fresh-session/run markers
        // nor independent protected slots. It cannot become a fresh source.
        let mut state = store.state.lock().unwrap();
        let scope = (admitted.session.id.clone(), admitted.run.id.clone());
        state.native_session_protocols.remove(&admitted.session.id);
        state.native_run_protocols.remove(&scope);
        state.execution_enrollment_protocols.remove(&scope);
        state.execution_enrollment_origins.remove(&scope);
        state.execution_enrollment_unclassified.remove(&scope);
        state.native_run_source_heads.remove(&scope);
        state
            .native_run_sources
            .retain(|(session, run, _), _| (session, run) != (&scope.0, &scope.1));
    }
    assert!(matches!(
        store.inspect_initial_preparation_admission(&original).await,
        Err(PreparationCommitError::Owner(Error::Unsupported))
    ));
}
