use super::*;
use crate::native_enrollment::caller::{Probe, contract::Harness};
impl Harness for crate::MemoryStore {
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
    fn phase_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::PhaseMutation,
        authority: Option<crate::PhaseWriteAuthority>,
        start: Option<crate::PhaseStart>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<crate::PhaseMutationOutcome, crate::PhaseError>>
    {
        let execution = crate::memory::MemoryExecution {
            store: self.clone(),
            fence,
        };
        std::future::ready(execution.phase_with_proof(
            &request,
            authority.as_ref(),
            start.as_ref(),
            proof,
        ))
    }
    fn attempt_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::AttemptMutation,
        authority: Option<crate::AttemptAuthority>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<crate::AttemptMutationOutcome, crate::AttemptError>>
    {
        let execution = crate::memory::MemoryExecution {
            store: self.clone(),
            fence,
        };
        std::future::ready(execution.change_attempt_with_proof(
            &request,
            authority.as_ref(),
            None,
            proof,
        ))
    }
    fn legacy_with_proof(
        &self,
        fence: crabber_core::RunFence,
        call: &crabber_core::ToolCallId,
        event: &crabber_core::EventRecord,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<(), crate::StoreError>> {
        let execution = crate::memory::MemoryExecution {
            store: self.clone(),
            fence,
        };
        std::future::ready(execution.claim_legacy_with_proof(call, event, proof))
    }
    fn begin_with_proof(
        &self,
        fence: crabber_core::RunFence,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<(), crate::AdmissionExecutionError>> {
        let execution = crate::memory::MemoryExecution {
            store: self.clone(),
            fence,
        };
        std::future::ready(execution.begin_admission_with_proof(proof))
    }
    fn retained(
        &self,
        session: &SessionId,
        run: &RunId,
        birth: bool,
    ) -> impl std::future::Future<Output = crabber_core::InputFingerprint> {
        let state = self.state.lock().unwrap();
        let scope = (session.clone(), run.clone());
        let original = (
            state.execution_enrollment_protocols.contains(&scope),
            &state.execution_enrollment_origins[&scope],
            &state.execution_enrollment_unclassified[&scope],
        );
        let bytes = if birth {
            serde_json::to_vec(&original).unwrap()
        } else {
            let attempts: Vec<_> = state
                .attempts
                .values()
                .filter(|a| a.snapshot.run_id == *run)
                .map(|a| (&a.snapshot, &a.owner, &a.token, &a.run_token, a.lease_until))
                .collect();
            let receipts: Vec<_> = state
                .operation_receipts
                .iter()
                .filter(|((s, _), _)| s == session)
                .collect();
            let events: Vec<_> = state.events.iter().filter(|e| e.run_id == *run).collect();
            let issuers: Vec<_> = state
                .native_run_issuances
                .iter()
                .filter(|((s, r, _), _)| s == session && r == run)
                .collect();
            let witnesses: Vec<_> = state
                .native_parent_witnesses
                .iter()
                .filter(|(_, w)| w.source.run_id == *run)
                .collect();
            let sources: Vec<_> = state
                .native_run_sources
                .iter()
                .filter(|((s, r, _), _)| s == session && r == run)
                .collect();
            let phases: Vec<_> = state
                .phases
                .values()
                .filter(|p| p.snapshot.run_id == *run)
                .map(|p| (&p.snapshot, &p.token, &p.output))
                .collect();
            let calls: Vec<_> = state.calls.values().filter(|c| c.run_id == *run).collect();
            serde_json::to_vec(&(
                (
                    state.admission_executions.get(run),
                    state
                        .admission_execution_sources
                        .iter()
                        .filter(|((id, _), _)| id == run)
                        .collect::<Vec<_>>(),
                    state.admission_execution_protocols.contains(run),
                ),
                phases,
                calls,
                state.snapshot_revisions.get(session),
                original,
                &state.runs[run],
                attempts,
                receipts,
                events,
                issuers,
                witnesses,
                sources,
                state.native_run_source_heads.get(&scope),
                state.public_revisions.get(session),
            ))
            .unwrap()
        };
        std::future::ready(crate::native_enrollment::caller::contract::snapshot_digest(
            &bytes,
        ))
    }
}
#[tokio::test]
async fn raw_attempt_fresh_replay_rollback_and_ack_loss() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::run(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_attempt_keeps_caller_proof_through_genuine_barriers() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::budgets(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_phase_fresh_replay_rollback_and_ack_loss() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::phases(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_compatibility_original_inputs_and_atomic_rollback() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::compatibility(
        &store, &clock,
    ))
    .await;
}
