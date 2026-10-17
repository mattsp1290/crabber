//! Actual native admission recovery: refusal rollback, exact budget, and lost ACK.
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crate::{AdmissionExecutionError as Error, ClaimUnstartedAdmissionRequest};
use crabber_core::{Clock, ManualClock, SessionId};
pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    for lost in [false, true] {
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), clock.now());
        let crate::KeyedAdmitOutcome::Started { admitted, receipt } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!("genuine initial admission")
        };
        let request = ClaimUnstartedAdmissionRequest {
            session_id: receipt.session_id.clone(),
            key: keyed.options.key.clone(),
            expected_fence: admitted.fence.clone(),
            expected_owner: admitted.run.owner,
            fingerprint: receipt.fingerprint,
            behavior_fingerprint: keyed.options.behavior_fingerprint,
            semantic_digest: receipt.semantic_digest,
            capsule_digest: keyed.execution.unwrap().digest().unwrap(),
            owner: "replacement-owner".into(),
            lease: std::time::Duration::from_secs(30),
        };
        clock.set(admitted.run.lease_until);
        let (session, run) = (&request.session_id, &request.expected_fence.run_id);
        let before = store.retained(session, run, false).await;
        let original = store.retained(session, run, true).await;
        store.probe().arm_admission_recovery(session, run, true);
        assert_eq!(
            store
                .claim_unstarted_admission(request.clone())
                .await
                .unwrap_err(),
            Error::UnknownStoreFailure
        );
        assert_eq!(store.probe().hits().len(), 1);
        assert_eq!(store.retained(session, run, false).await, before);
        assert_eq!(store.retained(session, run, true).await, original);
        let mut measured = Proof::bounded(32 << 20);
        assert!(
            store
                .recover_admission_with_proof(request.clone(), &mut measured)
                .await
                .is_err()
        );
        store.probe().arm_admission_recovery(session, run, false);
        assert!(
            store
                .recover_admission_with_proof(
                    request.clone(),
                    &mut Proof::bounded(measured.used() - 1)
                )
                .await
                .is_err()
        );
        assert!(store.probe().hits().is_empty());
        assert_eq!(store.retained(session, run, false).await, before);
        if lost {
            store.probe().lose_ack();
        }
        let result = if lost {
            store.claim_unstarted_admission(request.clone()).await
        } else {
            store
                .recover_admission_with_proof(request.clone(), &mut Proof::bounded(measured.used()))
                .await
        };
        assert_eq!(result.is_err(), lost);
        assert_eq!(store.probe().hits().len(), 1);
        let updated = store.get_run(run).await.unwrap().unwrap();
        assert_ne!(updated.claim_token, request.expected_fence.claim_token);
        assert_eq!(updated.owner, request.owner);
        assert_eq!(store.retained(session, run, true).await, original);
        let committed = store.retained(session, run, false).await;
        store.probe().arm_admission_recovery(session, run, true);
        assert_eq!(
            store
                .claim_unstarted_admission(request.clone())
                .await
                .unwrap_err(),
            Error::StaleOwner
        );
        assert!(store.probe().hits().is_empty());
        assert_eq!(store.retained(session, run, false).await, committed);
    }
}
