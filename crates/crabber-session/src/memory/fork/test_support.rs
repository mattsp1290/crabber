use super::MemoryStore;
impl MemoryStore {
    pub(crate) fn corrupt_fork_call_for_test(
        &self,
        target: &crate::PhaseTarget,
        call: crabber_core::ToolCallRecord,
        message: crabber_core::Message,
        output: crabber_core::ToolOutput,
    ) {
        let mut state = self.state.lock().unwrap();
        let original = state.calls.get(&call.id).unwrap();
        let phase = state.phases.get(&target.id).unwrap();
        let run = state.runs.get(&call.run_id).unwrap();
        assert_eq!(
            crate::attempt_control::validate_phase(run, original, phase),
            Err(crate::AttemptError::CleanupBlocked)
        );
        state.phases.get_mut(&target.id).unwrap().output = Some(output);
        state.calls.insert(call.id.clone(), call);
        let current = state
            .messages
            .iter_mut()
            .find(|current| current.id == message.id)
            .unwrap();
        *current = message;
    }
}
