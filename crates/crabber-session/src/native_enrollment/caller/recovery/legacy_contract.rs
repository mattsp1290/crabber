//! Actual compatibility fence claims; no receipts or enrollment are invented.
use crate::native_enrollment::caller::contract::Harness;
use crate::public_session::v2::proof::Proof;
use crabber_core::{Clock, ManualClock, SessionId};

pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    for keyed in [false, true] {
        for lost in [false, true] {
            let admitted = if keyed {
                let request =
                    crate::admission_execution_contract::keyed(&SessionId::new(), clock.now());
                let crate::KeyedAdmitOutcome::Started { admitted, .. } =
                    store.admit_keyed_run(request).await.unwrap()
                else {
                    panic!("fresh initial")
                };
                store
                    .execution(admitted.fence.clone())
                    .await
                    .unwrap()
                    .begin_admission_execution()
                    .await
                    .unwrap();
                *admitted
            } else {
                crate::public_state_contract::admit(store, clock.now()).await
            };
            clock.set(admitted.run.lease_until);
            let (session, run) = (&admitted.session.id, &admitted.run.id);
            let before = store.retained(session, run, false).await;
            let birth = store.retained(session, run, true).await;
            store.probe().arm_legacy_expired(session, run, true);
            assert!(store.claim_expired_run(run, "replacement").await.is_err());
            assert_eq!(store.probe().hits().len(), 1);
            assert_eq!(store.retained(session, run, false).await, before);
            let mut measured = Proof::bounded(32 << 20);
            assert!(
                store
                    .legacy_expired_with_proof(run, "replacement", &mut measured)
                    .await
                    .is_err()
            );
            store.probe().arm_legacy_expired(session, run, false);
            assert!(
                store
                    .legacy_expired_with_proof(
                        run,
                        "replacement",
                        &mut Proof::bounded(measured.used() - 1)
                    )
                    .await
                    .is_err()
            );
            assert_eq!(
                store.probe().hits(),
                [] as [crabber_core::InputFingerprint; 0]
            );
            assert_eq!(store.retained(session, run, false).await, before);
            if lost {
                store.probe().lose_ack();
            }
            let result = if lost {
                store.claim_expired_run(run, "replacement").await
            } else {
                store
                    .legacy_expired_with_proof(
                        run,
                        "replacement",
                        &mut Proof::bounded(measured.used()),
                    )
                    .await
            };
            assert_eq!(result.is_err(), lost);
            assert_eq!(store.probe().hits().len(), 1);
            let updated = store.get_run(run).await.unwrap().unwrap();
            assert_ne!(updated.claim_token, admitted.fence.claim_token);
            assert_eq!(updated.owner, "replacement");
            assert_eq!(store.retained(session, run, true).await, birth);
            let committed = store.retained(session, run, false).await;
            store.probe().arm_legacy_expired(session, run, true);
            assert!(store.claim_expired_run(run, "replacement").await.is_err());
            assert_eq!(
                store.probe().hits(),
                [] as [crabber_core::InputFingerprint; 0]
            );
            assert_eq!(store.retained(session, run, false).await, committed);
        }
    }
}
