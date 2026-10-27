use super::{State, reads};
use crate::public_session::{budget::Budget, v2::proof::Proof};
use crabber_core::Run;

pub(in super::super) fn accepted_event_count(
    state: &State,
    run: &Run,
    proof: &mut Proof,
) -> Result<usize, crate::AttemptError> {
    let mut safe = Budget::new(usize::MAX, crate::PublicSessionLimit::HeaderBytes);
    let summaries = super::public_controls(state, run, 64, &mut safe, proof)
        .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    let records =
        reads::records_from_summaries(state, &crate::RunControlTarget::of(run), summaries, proof)
            .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?;
    crate::attempt::pristine::accepted_control_events(run, &records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attempt_contract::bound;
    use crate::public_session::v2::proof::Failure;
    use crate::{AttemptError, MemoryStore, Store};
    use crabber_core::{Clock, ManualClock};
    use std::sync::Arc;

    #[tokio::test]
    async fn accepted_control_pristine_reads_share_exact_proof_and_charge_private_request() {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = MemoryStore::with_clock(clock.clone());
        let admitted = bound::admit(&store, clock.now()).await;
        bound::cancel(&store, &admitted, &"p".repeat(1024)).await;
        let state = store.state.lock().unwrap();
        let mut measured = Proof::bounded(usize::MAX);
        assert_eq!(
            accepted_event_count(&state, &admitted.run, &mut measured).unwrap(),
            1
        );
        let n = measured.used();
        assert!(n > 1024);
        let mut exact = Proof::bounded(n);
        assert_eq!(
            accepted_event_count(&state, &admitted.run, &mut exact).unwrap(),
            1
        );
        assert_eq!(exact.used(), n);
        let mut short = Proof::bounded(n - 1);
        assert!(accepted_event_count(&state, &admitted.run, &mut short).is_err());
        assert!(matches!(
            short.charge(0),
            Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
        ));
        let mut shared = Proof::bounded(2 * n - 1);
        assert_eq!(
            accepted_event_count(&state, &admitted.run, &mut shared).unwrap(),
            1
        );
        assert!(accepted_event_count(&state, &admitted.run, &mut shared).is_err());
        assert!(matches!(
            shared.charge(0),
            Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
        ));
    }

    #[tokio::test]
    async fn control_audit_without_original_receipt_cannot_authorize_initial_attempt() {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = MemoryStore::with_clock(clock.clone());
        let admitted = bound::admit(&store, clock.now()).await;
        let receipt = bound::cancel(&store, &admitted, "authorized host cancellation").await;
        store
            .state
            .lock()
            .unwrap()
            .operation_receipts
            .remove(&(admitted.session.id.clone(), receipt.operation_id));
        let before = store.session_revision(&admitted.session.id).await.unwrap();
        let execution = store.execution(admitted.fence).await.unwrap();
        assert_eq!(
            execution
                .mutate_attempt(bound::request(before), None)
                .await
                .unwrap_err(),
            AttemptError::UnknownStoreOutcome
        );
        assert_eq!(
            store.session_revision(&admitted.session.id).await.unwrap(),
            before
        );
        assert!(
            store
                .latest_execution_attempt(&admitted.run.id)
                .await
                .unwrap()
                .is_none()
        );
    }
}
