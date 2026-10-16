//! The source-compiled public facade drives a real worker before this probe.
use super::super::super::MemoryStore;
use crabber_core::{ControlId, RunId, SessionId};
impl MemoryStore {
    #[allow(dead_code)] // Invoked by the source-compiled facade integration test.
    pub(crate) fn test_handoff_corruption(&self, session: &SessionId, run: &RunId) {
        let original = self
            .state
            .lock()
            .unwrap()
            .tool_continuations
            .values()
            .filter(|r| r.session_id == *session && r.run_id == *run)
            .max_by_key(|r| r.batch.continuation.generation)
            .unwrap()
            .clone();
        let receipt = self
            .tool_receipt(session, &original.batch.id)
            .unwrap()
            .unwrap();
        let claim = original.claim.clone().unwrap();
        let claimed = self.tool_receipt(session, &claim.id).unwrap().unwrap();
        for case in 0..3 {
            let mut changed = original.clone();
            if case == 0 {
                changed.handoff = None;
            } else {
                changed.handoff.as_mut().unwrap().predecessor.operation_id = if case == 1 {
                    ControlId::new()
                } else {
                    ControlId("x".repeat(1024 * 1024))
                };
            }
            self.state
                .lock()
                .unwrap()
                .tool_continuations
                .insert(original.batch.continuation.id.clone(), changed);
            let revision = self.state.lock().unwrap().public_revisions[session];
            let request = crate::PublicHeaderRequest {
                session_id: session.clone(),
                run_id: Some(run.clone()),
                expected_revision: revision,
                max_bytes: 65536,
                max_proof_bytes: 65536,
                max_interactions: 64,
            };
            let allocation = allocation_counter::measure(|| {
                let outcome = self.read_public_header(&request);
                if case == 2 {
                    assert!(matches!(
                        outcome,
                        Ok(crate::PublicHeaderOutcome::Limited(
                            crate::PublicSessionLimit::ProofBytes
                        ))
                    ));
                } else {
                    assert!(outcome.is_err());
                }
            });
            let approval = self.read_approval(session, run);
            let tool = self.read_tools(session, run);
            let replay = self.tool_receipt(session, &original.batch.id);
            let claim_replay = self.claim_tools(&claim);
            self.state
                .lock()
                .unwrap()
                .tool_continuations
                .insert(original.batch.continuation.id.clone(), original.clone());
            assert!(allocation.bytes_max < 131_072, "{allocation:?}");
            assert!(matches!(
                approval,
                Err(crate::ApprovalError::UnknownStoreOutcome)
            ));
            assert!(matches!(
                tool,
                Err(crate::ToolContinuationError::UnknownStoreOutcome)
            ));
            assert_eq!(replay.unwrap(), Some(receipt.clone()));
            assert!(
                matches!(claim_replay,Ok(crate::ToolContinuationClaimOutcome::Replayed(actual)) if actual==claimed)
            );
        }
        self.read_approval(session, run).unwrap();
        self.read_tools(session, run).unwrap();
    }
}

impl MemoryStore {
    #[allow(dead_code)] // Source-compiled public fixture.
    pub(crate) fn test_terminal_history_corruption(&self, request: &crate::RunControlRequest) {
        let (phase, call) = {
            let mut state = self.state.lock().unwrap();
            let phase = state
                .phases
                .values()
                .filter(|p| {
                    p.snapshot.run_id == request.target.run_id
                        && p.snapshot.state == crate::PhaseState::Completed
                })
                .max_by_key(|p| p.snapshot.target.generation)
                .unwrap()
                .clone();
            let call = state.calls[&phase.snapshot.target.call_id].clone();
            let output =
                crabber_core::ToolOutput::from_json(&serde_json::json!("forged protected output"))
                    .unwrap();
            state
                .phases
                .get_mut(&phase.snapshot.target.id)
                .unwrap()
                .output = Some(output.clone());
            state
                .calls
                .get_mut(&call.id)
                .unwrap()
                .result
                .as_mut()
                .unwrap()
                .content = output.to_content_blocks();
            (phase, call)
        };
        let revision = self.state.lock().unwrap().public_revisions[&request.target.session_id];
        let result = self.control_accept(request.clone());
        let receipt = self.control_lookup(&request.target.session_id, &request.id);
        let after = self.state.lock().unwrap().public_revisions[&request.target.session_id];
        let mut state = self.state.lock().unwrap();
        state.phases.insert(phase.snapshot.target.id.clone(), phase);
        state.calls.insert(call.id.clone(), call);
        drop(state);
        assert!(
            result.is_err(),
            "coherent private output replacement became terminal authority"
        );
        assert_eq!(receipt.unwrap(), None);
        assert_eq!(after, revision);
    }
}
