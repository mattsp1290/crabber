use super::*;
use crate::native_enrollment::caller::Probe;
use crate::native_enrollment::caller::contract::Harness;
impl Harness for crate::SqliteStore {
    fn probe(&self) -> &Probe {
        &self.native_raw_probe
    }
    fn fault(&self, stage: u8) {
        self.attempt_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
    }
    fn phase_fault(&self, stage: u8) {
        self.phase_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
    }
    async fn phase_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::PhaseMutation,
        authority: Option<crate::PhaseWriteAuthority>,
        start: Option<crate::PhaseStart>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        let effect = start.as_ref().map_or(
            crate::phase::PhaseEffect::Metadata,
            crate::phase::PhaseEffect::Start,
        );
        Box::pin(self.phase_transaction_with_proof(
            &fence.run_id,
            request,
            authority,
            effect,
            Some(&fence),
            proof,
        ))
        .await
    }
    async fn attempt_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::AttemptMutation,
        authority: Option<crate::AttemptAuthority>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        let execution = crate::sqlite::SqliteExecution {
            store: self.clone(),
            fence,
        };
        execution
            .change_attempt_with_proof(request, authority, None, proof)
            .await
    }
    async fn legacy_with_proof(
        &self,
        fence: crabber_core::RunFence,
        call: &crabber_core::ToolCallId,
        event: &crabber_core::EventRecord,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::StoreError> {
        let execution = crate::sqlite::SqliteExecution {
            store: self.clone(),
            fence,
        };
        execution.claim_legacy_with_proof(call, event, proof).await
    }
    async fn begin_with_proof(
        &self,
        fence: crabber_core::RunFence,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::AdmissionExecutionError> {
        let execution = crate::sqlite::SqliteExecution {
            store: self.clone(),
            fence,
        };
        crate::sqlite::admission_execution::begin_with_proof(&execution, proof).await
    }
    async fn recover_admission_with_proof(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError> {
        super::super::admission_execution::claim_with_proof(self, request, proof).await
    }
    async fn retained(
        &self,
        session: &SessionId,
        run: &RunId,
        birth: bool,
    ) -> crabber_core::InputFingerprint {
        let mut values: Vec<String> = Vec::new();
        for query in if birth { &BIRTH[..] } else { &NATIVE[..] } {
            let query = query.replace("$2", "(SELECT session_id FROM runs WHERE id=$1)");
            values.extend(
                sqlx::query_scalar::<_, String>(&query)
                    .bind(&run.0)
                    .fetch_all(&self.writer)
                    .await
                    .unwrap(),
            );
        }
        crate::native_enrollment::caller::contract::snapshot_digest(&(session, run, values))
    }
}
const BIRTH: [&str; 3] = [
    "SELECT CAST(execution_enrollment_protocol AS TEXT)||COALESCE(execution_enrollment_origin_id,'') FROM runs WHERE id=$1",
    "SELECT data FROM native_execution_origins WHERE run_id=$1",
    "SELECT data FROM native_execution_unclassified WHERE run_id=$1",
];
const NATIVE: [&str; 17] = [
    "SELECT CAST(public_revision AS TEXT) FROM sessions WHERE id=(SELECT session_id FROM runs WHERE id=$1)",
    "SELECT data FROM runs WHERE id=$1",
    "SELECT CAST(generation AS TEXT)||owner||claim_token||run_claim_token||CAST(lease_until AS TEXT)||state||cleanup_state FROM execution_attempts WHERE run_id=$1 ORDER BY id",
    "SELECT data FROM events WHERE run_id=$1 ORDER BY seq",
    "SELECT data_json FROM operation_receipts WHERE session_id=$2 ORDER BY operation_id",
    "SELECT data_json FROM native_parent_witnesses WHERE run_id=$1 ORDER BY event_seq",
    "SELECT data_json FROM native_run_issuances WHERE run_id=$1 ORDER BY sequence",
    "SELECT CAST(execution_enrollment_protocol AS TEXT)||COALESCE(execution_enrollment_origin_id,'') FROM runs WHERE id=$1",
    "SELECT CAST(sequence AS TEXT)||issuance_digest||COALESCE(previous_digest,'')||chain_digest FROM native_run_sources WHERE run_id=$1 ORDER BY sequence",
    "SELECT CAST(sequence AS TEXT)||origin_digest||chain_digest FROM native_run_source_heads WHERE run_id=$1",
    "SELECT data FROM native_execution_origins WHERE run_id=$1",
    "SELECT data FROM native_execution_unclassified WHERE run_id=$1",
    "SELECT claim_token||outcome_json||state||quiescence_state FROM tool_phases WHERE run_id=$1 ORDER BY id",
    "SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id",
    "SELECT CAST(snapshot_revision AS TEXT) FROM sessions WHERE id=(SELECT session_id FROM runs WHERE id=$1)",
    "SELECT CAST(source_protocol AS TEXT)||start_state||data FROM admission_executions WHERE run_id=$1",
    "SELECT kind||data_json FROM admission_execution_sources WHERE run_id=$1 ORDER BY kind",
];
#[tokio::test]
async fn raw_attempt_fresh_replay_rollback_and_ack_loss() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::run(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_attempt_keeps_caller_proof_through_genuine_barriers() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::budgets(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_phase_fresh_replay_rollback_and_ack_loss() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::phases(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_compatibility_original_inputs_and_atomic_rollback() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::compatibility(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_admission_recovery_original_sources_and_atomic_rollback() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::recoveries(
        &store, &clock,
    ))
    .await;
}
