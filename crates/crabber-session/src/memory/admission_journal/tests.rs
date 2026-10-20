use super::*;
use crate::{MemoryStore, Store};
use crabber_core::SessionId;
#[tokio::test]
async fn execution_sources_reject_rollback_and_missing_new_init() {
    let store = MemoryStore::new();
    let now = time::OffsetDateTime::now_utc();
    let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
    let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
        store.admit_keyed_run(keyed.clone()).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(
        store
            .state
            .lock()
            .unwrap()
            .admission_execution_sources
            .len(),
        1
    );
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    execution.begin_admission_execution().await.unwrap();
    assert_eq!(
        store
            .state
            .lock()
            .unwrap()
            .admission_execution_sources
            .len(),
        2
    );
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        crate::AdmissionExecutionError::AlreadyStarted
    );
    store
        .state
        .lock()
        .unwrap()
        .admission_executions
        .get_mut(&receipt.run_id)
        .unwrap()
        .state = crate::AdmissionExecutionState::Unstarted;
    assert!(
        store
            .load_admission_execution(&receipt.session_id, &keyed.options.key)
            .await
            .is_err()
    );
    assert!(execution.begin_admission_execution().await.is_err());
    assert!(
        store
            .abandon_run(crabber_core::AbandonRequest {
                expected: admitted.fence.clone(),
                expected_owner: admitted.run.owner,
                authority: crabber_core::AbandonAuthority::HostStoppedOwner,
            })
            .await
            .is_err()
    );
    let mut state = store.state.lock().unwrap();
    state
        .admission_execution_sources
        .remove(&(receipt.run_id.clone(), Kind::Begin));
    state
        .admission_execution_sources
        .remove(&(receipt.run_id.clone(), Kind::Init));
    assert!(verify(&state, &receipt.run_id).is_err());
}

#[cfg(test)]
#[tokio::test]
async fn original_unstarted_abandonment_retains_no_effect_only_before_begin() {
    use crate::{MemoryStore, Store};
    use crabber_core::{AbandonAuthority, AbandonRequest, SessionId};
    let store = MemoryStore::new();
    for started in [false, true] {
        let keyed = crate::admission_execution_contract::keyed(
            &SessionId::new(),
            time::OffsetDateTime::now_utc(),
        );
        let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed).await.unwrap()
        else {
            panic!()
        };
        if started {
            store
                .execution(admitted.fence.clone())
                .await
                .unwrap()
                .begin_admission_execution()
                .await
                .unwrap();
        }
        let request = AbandonRequest {
            expected: admitted.fence,
            expected_owner: admitted.run.owner,
            authority: AbandonAuthority::HostStoppedOwner,
        };
        let outcome = store.abandon_run(request.clone()).await.unwrap();
        let state = store.state.lock().unwrap();
        let commit = state.abandonments.get(&receipt.run_id).unwrap();
        assert_eq!(commit.no_effect_v1.is_some(), !started);
        if let Some(marker) = &commit.no_effect_v1 {
            let Some(Source::Init(init)) = state
                .admission_execution_sources
                .get(&(receipt.run_id.clone(), Kind::Init))
            else {
                panic!()
            };
            marker.verify(init, &request).unwrap();
            let original: crate::abandonment::AbandonEvidence =
                serde_json::from_value(outcome.terminal_event.payload["abandonment_v1"].clone())
                    .unwrap();
            assert!(original.no_effect_v1.as_ref() == Some(marker));
        }
    }
}
