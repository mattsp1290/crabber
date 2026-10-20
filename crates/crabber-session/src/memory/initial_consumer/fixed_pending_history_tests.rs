//! Actual readiness must account for every stored original, not only the winner.
use super::Proof;
use super::policy_tests::completed;
use crate::DeletionError;
use crate::native_enrollment::pending_initial::SourceKey;
const MAX: usize = 32 << 20;

#[tokio::test]
async fn fixed_initial_readiness_rejects_extra_misindexed_and_foreign_epoch_rows() {
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let store = host.store();
        let genuine = store.state.lock().unwrap().clone();
        let slot = genuine.pending_initial_protocols.values().next().unwrap();
        let key = &slot.key;
        let native_before = genuine.session_owners.raw_initial_snapshot(&session);
        assert_eq!(
            store
                .fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX))
                .unwrap(),
            None
        );
        for mode in 0..3 {
            let mut changed = genuine.clone();
            match mode {
                0 => changed.pending_initial.test_add_nonwinning_epoch(key),
                1 => changed.pending_initial.test_misindex_winning_epoch(key),
                _ => changed
                    .pending_initial
                    .test_foreign_winning_epoch_session(key),
            }
            // The independent original slot and all genuine current/source
            // publication rows remain; only the selected Epoch map changed.
            assert!(changed.pending_initial.slot(key) == Some(slot));
            assert_eq!(
                changed.session_owners.raw_initial_snapshot(&session),
                native_before
            );
            *store.state.lock().unwrap() = changed;
            let refused =
                store.fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX));
            *store.state.lock().unwrap() = genuine.clone();
            assert!(
                matches!(refused, Err(DeletionError::UnknownStoreOutcome)),
                "malformed Epoch mode {mode} cannot become Ready/absence"
            );
            assert_eq!(
                store
                    .fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX))
                    .unwrap(),
                None
            );
        }
    }
}
fn foreign_session(origin: &mut crate::native_enrollment::initial_consumer::Origin) {
    use crate::native_enrollment::initial_consumer::Origin;
    let Origin::Opened(anchor) = origin else {
        panic!("genuine fixed origin");
    };
    anchor.correlation.key.session_id = crabber_core::SessionId::new();
}
#[tokio::test]
async fn fixed_initial_readiness_authenticates_consumer_originals_and_independent_cohorts() {
    let (donor, _) = completed(false).await;
    let donor_state = donor.store().state.lock().unwrap().clone();
    let actual_donor = donor_state
        .initial_consumer_cohorts
        .values()
        .next()
        .unwrap();
    for keyed in [false, true] {
        let (host, session) = completed(keyed).await;
        let store = host.store();
        let genuine = store.state.lock().unwrap().clone();
        let key: &SourceKey = genuine.initial_consumer_cohorts.keys().next().unwrap();
        assert_eq!(
            store
                .fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX))
                .unwrap(),
            None
        );
        for mode in 0..5 {
            let mut changed = genuine.clone();
            match mode {
                0 => changed.initial_consumer.test_erase_consumer_origin(key),
                1 => {
                    changed.initial_consumer_cohorts.remove(key);
                }
                2 => changed
                    .initial_consumer
                    .test_replace_consumer_origin(key, actual_donor),
                3 => {
                    changed
                        .initial_consumer_cohorts
                        .insert(key.clone(), actual_donor.clone());
                }
                _ => {
                    let mut origin = genuine
                        .initial_consumer_cohorts
                        .get(key)
                        .unwrap()
                        .as_ref()
                        .clone();
                    foreign_session(&mut origin);
                    changed
                        .initial_consumer
                        .test_replace_consumer_origin(key, &origin);
                    changed
                        .initial_consumer_cohorts
                        .insert(key.clone(), std::sync::Arc::new(origin));
                }
            }
            *store.state.lock().unwrap() = changed;
            let refused =
                store.fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX));
            *store.state.lock().unwrap() = genuine.clone();
            assert!(
                matches!(refused, Err(DeletionError::UnknownStoreOutcome)),
                "genuine retained copies cannot hide malformed origin/cohort mode {mode}"
            );
            assert_eq!(
                store
                    .fixed_initial_readiness_with_proof(&session, &mut Proof::bounded(MAX))
                    .unwrap(),
                None
            );
        }
    }
}
