//! Initial reservation needs positive retained-input evidence, checked in its transaction.
use crate::{AttemptError, RetainedExecutionBinding};
use crabber_core::{Run, RunStatus, Usage};

pub(crate) fn input_count(
    run: &Run,
    binding: &RetainedExecutionBinding,
) -> Result<usize, AttemptError> {
    if run.status != RunStatus::Running
        || run.checkpoint.is_some()
        || run.error.is_some()
        || run.usage != Usage::default()
        || binding.run_id != run.id
        || binding.session_id != run.session_id
        || binding.capsule.version != 3
    {
        return Err(AttemptError::InvalidTransition);
    }
    let input = binding
        .capsule
        .input_envelope
        .as_ref()
        .ok_or(AttemptError::InvalidTransition)?;
    Ok(input.messages().len())
}

/// Audit reads authenticate each receipt and source event before calling this.
pub(crate) fn accepted_control_events(
    run: &Run,
    records: &[crate::run_control::Record],
) -> Result<usize, AttemptError> {
    let target = crate::RunControlTarget::of(run);
    if records.iter().any(|record| {
        record.summary.receipt.target != target
            || record.summary != crate::run_control::accepted(&record.summary.receipt)
    }) {
        return Err(AttemptError::InvalidTransition);
    }
    Ok(records.len())
}
