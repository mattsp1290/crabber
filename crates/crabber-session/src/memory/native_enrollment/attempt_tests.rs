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
    fn initial_fault(&self, stage: u8) {
        self.binding_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
        self.queue_fault
            .store(stage, std::sync::atomic::Ordering::SeqCst);
    }
    fn legacy_expired_with_proof(
        &self,
        id: &RunId,
        owner: &str,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<crabber_core::RunFence, crate::StoreError>> {
        std::future::ready(self.claim_legacy_expired_with_proof(id, owner, proof))
    }
    fn complete_with_proof(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<
        Output = Result<
            crate::session_owner::preparation::PreparationCompletion,
            crate::session_owner::preparation::PreparationCommitError,
        >,
    > {
        std::future::ready(
            self.complete_preparation_captured_with_proof(&joined, false, proof)
                .map(|value| value.completion),
        )
    }
    fn initial_retained(
        &self,
        session: &SessionId,
    ) -> impl std::future::Future<Output = crabber_core::InputFingerprint> {
        let state = self.state.lock().unwrap();
        let runs = state
            .runs
            .values()
            .filter(|r| r.session_id == *session)
            .collect::<Vec<_>>();
        let bindings = state
            .execution_bindings
            .iter()
            .filter(|(id, _)| runs.iter().any(|r| r.id == **id))
            .collect::<Vec<_>>();
        let admission = (
            runs,
            bindings,
            state
                .messages
                .iter()
                .filter(|m| m.session_id == *session)
                .collect::<Vec<_>>(),
            state
                .epochs
                .values()
                .filter(|e| e.session_id == *session)
                .collect::<Vec<_>>(),
            state
                .receipts
                .iter()
                .filter(|((s, _), _)| s == session)
                .collect::<Vec<_>>(),
            state
                .admission_executions
                .values()
                .filter(|r| r.receipt.session_id == *session)
                .collect::<Vec<_>>(),
            state
                .admission_execution_sources
                .iter()
                .filter(|((id, _), _)| state.runs.get(id).is_some_and(|r| r.session_id == *session))
                .collect::<Vec<_>>(),
        );
        let native = raw_initial_native(&state, session);
        let owners = (
            state.session_owners.raw_initial_snapshot(session),
            state.ownership_origins.get(session),
            state
                .preparation_admissions
                .values()
                .filter(|w| w.session_id() == session)
                .collect::<Vec<_>>(),
        );
        std::future::ready(crate::native_enrollment::caller::contract::snapshot_digest(
            &(
                state.sessions.get(session),
                state.public_revisions.get(session),
                state.snapshot_revisions.get(session),
                state.queue_publications.raw_initial_snapshot(session),
                admission,
                native,
                owners,
                state
                    .queue
                    .iter()
                    .filter(|((s, _), _)| s == session)
                    .collect::<Vec<_>>(),
                state
                    .queue_audits
                    .iter()
                    .filter(|((s, _), _)| s == session)
                    .collect::<Vec<_>>(),
                state
                    .operation_receipts
                    .iter()
                    .filter(|((s, _), _)| s == session)
                    .collect::<Vec<_>>(),
            ),
        ))
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
    fn recover_admission_with_proof(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> impl std::future::Future<Output = Result<crate::ClaimedAdmission, crate::AdmissionExecutionError>>
    {
        std::future::ready(self.claim_admission_with_proof(&request, proof))
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
fn raw_initial_native(state: &State, session: &SessionId) -> crabber_core::InputFingerprint {
    crate::native_enrollment::caller::contract::snapshot_digest(&(
        state
            .native_run_issuances
            .iter()
            .filter(|((s, _, _), _)| s == session)
            .collect::<Vec<_>>(),
        state
            .native_run_sources
            .iter()
            .filter(|((s, _, _), _)| s == session)
            .collect::<Vec<_>>(),
        state
            .native_run_source_heads
            .iter()
            .filter(|((s, _), _)| s == session)
            .collect::<Vec<_>>(),
        state
            .execution_enrollment_protocols
            .iter()
            .filter(|(s, _)| s == session)
            .collect::<Vec<_>>(),
        state
            .execution_enrollment_origins
            .iter()
            .filter(|((s, _), _)| s == session)
            .collect::<Vec<_>>(),
        state
            .execution_enrollment_unclassified
            .iter()
            .filter(|((s, _), _)| s == session)
            .collect::<Vec<_>>(),
    ))
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

#[tokio::test]
async fn raw_admission_recovery_original_sources_and_atomic_rollback() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::recoveries(
        &store, &clock,
    ))
    .await;
}

#[tokio::test]
async fn raw_initial_fresh_replay_rollback_and_ack_loss() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::initials(&store)).await;
}

#[tokio::test]
async fn raw_prepared_recovery_original_sources_and_atomic_rollback() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::prepared_recoveries(&store, &clock)).await;
}

#[tokio::test]
async fn raw_legacy_expired_original_sources_and_atomic_rollback() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    Box::pin(crate::native_enrollment::caller::contract::legacy_expired_claims(&store, &clock))
        .await;
}
