//! Actual original input and terminal domains on the readiness allowance.
//! These authenticate history; no provider declaration becomes resource coverage.
#[path = "run_history/abandonment.rs"]
mod abandonment;
#[path = "run_history/approval.rs"]
mod approval;
#[path = "run_history/claims.rs"]
mod claims;
#[path = "run_history/waiting.rs"]
mod waiting;
use super::super::{OperationReceipt, State};
use crate::deletion::history::RunHistoryReport;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use crabber_core::{EventKind, Run, SessionId};

/// Private read result: the views borrow only the still-held original State.
pub(super) struct CheckedRuns<'state> {
    unproven: bool,
    uncovered_runs: bool,
    sources: Vec<crate::memory::initial_consumer::FixedClosedSource<'state>>,
}
impl<'state> CheckedRuns<'state> {
    pub(super) fn unproven(&self) -> bool {
        self.unproven
    }
    pub(super) fn uncovered_runs(&self) -> bool {
        self.uncovered_runs
    }
    pub(super) fn sources(&self) -> &[crate::memory::initial_consumer::FixedClosedSource<'state>] {
        &self.sources
    }
}
struct CheckedReport<'state> {
    report: RunHistoryReport,
    fixed: Option<crate::memory::initial_consumer::FixedClosedSource<'state>>,
}
/// Complete reports and exact original packages are produced in one fold.
pub(super) fn checked<'state>(
    state: &'state State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<CheckedRuns<'state>, Failure> {
    let count = state
        .runs
        .values()
        .filter(|run| &run.session_id == session)
        .try_fold(0usize, |count, _| {
            count.checked_add(1).ok_or_else(source_limit)
        })?;
    let bytes = count
        .checked_mul(std::mem::size_of::<
            crate::memory::initial_consumer::FixedClosedSource<'_>,
        >())
        .ok_or_else(source_limit)?;
    proof.charge(bytes)?;
    let mut result = CheckedRuns {
        unproven: false,
        uncovered_runs: false,
        sources: Vec::with_capacity(count),
    };
    for run in state.runs.values().filter(|run| &run.session_id == session) {
        let checked = checked_report(state, run, proof)?;
        result.unproven |= !checked.report.complete();
        result.uncovered_runs |= checked.fixed.is_none();
        if let Some(source) = checked.fixed {
            result.sources.push(source);
        }
    }
    Ok(result)
}
#[cfg(test)]
pub(super) fn verify(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    checked(state, session, proof).map(|runs| runs.unproven())
}
fn source_limit() -> Failure {
    Failure::Limited(crate::PublicSessionLimitV2::ProofBytes)
}
#[cfg(test)]
fn report(state: &State, run: &Run, proof: &mut Proof) -> Result<RunHistoryReport, Failure> {
    checked_report(state, run, proof).map(|checked| checked.report)
}
fn checked_report<'state>(
    state: &'state State,
    run: &'state Run,
    proof: &mut Proof,
) -> Result<CheckedReport<'state>, Failure> {
    let protected = super::super::native_parent::source_protocol::history(
        state,
        &run.session_id,
        &run.id,
        proof,
    )?;
    let unkeyed_input = input(state, run, proof)?;
    let (initial_present, claims) = claims::read(state, &run.session_id, &run.id, proof)?;
    let terminal = terminal(state, run, proof)?;
    let approved = approval::verify(state, run, proof)?;
    let cancelled = waiting::verify(state, run, proof)?;
    let abandoned = abandonment::verify(state, run, proof)?;
    let report = RunHistoryReport::sources(
        initial_present
            && state.preparation_admissions.contains_key(&run.id)
            && (state.admission_execution_protocols.contains(&run.id) || unkeyed_input),
        claims.with_membership(protected),
        terminal || approved || abandoned || cancelled,
    )
    .with_initial_consumption(crate::memory::initial_consumer::classify_run(
        state, run, proof,
    )?)
    .with_closed_initial_native_main();
    let fixed = if has_fixed_prerequisites(report) {
        super::super::MemoryStore::fixed_closed_initial_source(state, run, proof)?
    } else {
        None
    };
    proof.charge(0)?;
    let report = report.with_fixed_memory_resource_policy(fixed.is_some());
    Ok(CheckedReport {
        report,
        fixed: if report.complete() { fixed } else { None },
    })
}
fn has_fixed_prerequisites(report: RunHistoryReport) -> bool {
    use crate::deletion::history::Fact;
    report.initial_consumption
        == crate::native_enrollment::initial_consumer::InitialConsumption::CompletedSource
        && [
            report.initial_source,
            report.every_noninitial_claim,
            report.retained_noninitial_sources,
            report.terminal_source,
            report.main_join,
        ]
        .into_iter()
        .all(|fact| fact == Fact::Authenticated)
}

fn input(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let binding =
        super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(ReadError::public)?;
    if let Some(original) = state
        .preparation_admissions
        .get(&run.id)
        .filter(|w| w.is_unkeyed())
    {
        return unkeyed_input(state, run, original, binding.as_ref(), proof);
    }
    let Some(record) = state.admission_executions.get(&run.id) else {
        return Ok(false);
    };
    proof.measure(record)?;
    proof.measure(&(&run.config_hash, &run.plan_fingerprint, &run.owner))?;
    let original = state
        .receipts
        .get(&(run.session_id.clone(), record.key.clone()))
        .ok_or_else(|| unknown("original admission receipt"))?;
    proof.measure(original)?;
    if original != &record.receipt {
        return Err(unknown("original execution receipt"));
    }
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let selected = || {
        state
            .messages
            .iter()
            .filter(|message| {
                message.run_id.as_ref() == Some(&run.id) && message.session_id == run.session_id
            })
            .take(count)
    };
    for message in selected() {
        proof.measure(message)?;
    }
    let messages = selected().cloned().collect::<Vec<_>>();
    record.validate_messages(run, &messages).map_err(unknown)?;
    Ok(false)
}
fn unkeyed_input(
    state: &State,
    run: &Run,
    original: &crate::session_owner::preparation::admission::Witness,
    binding: Option<&crate::RetainedExecutionBinding>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    proof.measure(original)?;
    if original.session_id() != &run.session_id || original.run_id() != &run.id {
        return Err(unknown("original unkeyed input scope"));
    }
    super::owned::original_admission(state, original, proof)?;
    proof.charge(1)?;
    if state.admission_executions.contains_key(&run.id)
        || state.admission_execution_protocols.contains(&run.id)
        || state
            .admission_execution_sources
            .keys()
            .any(|(id, _)| id == &run.id)
        || state
            .receipts
            .values()
            .any(|receipt| receipt.run_id == run.id)
    {
        return Err(unknown("mixed unkeyed/keyed original input"));
    }
    let binding = binding.ok_or_else(|| unknown("original unkeyed binding missing"))?;
    original
        .verify_unkeyed_binding(&binding.capsule)
        .map_err(unknown)?;
    Ok(true)
}
fn terminal(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let Some(record) = state
        .attempts
        .values()
        .filter(|record| record.snapshot.run_id == run.id)
        .max_by_key(|record| record.snapshot.target.generation)
    else {
        return Ok(false);
    };
    proof.measure(&record.snapshot)?;
    let event = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::ExecutionAttemptChanged
                && event
                    .payload
                    .pointer("/attempt/target/id")
                    .and_then(serde_json::Value::as_str)
                    == Some(&record.snapshot.target.id.0)
        })
        .ok_or_else(|| unknown("latest terminal attempt source"))?;
    proof.measure(event)?;
    if !crate::attempt::terminal::candidate(event) {
        return Ok(false);
    }
    let operation = event
        .correlation
        .as_ref()
        .ok_or_else(|| unknown("terminal operation"))?;
    let Some(OperationReceipt::Attempt(receipt)) = state.operation_receipts.get(&(
        run.session_id.clone(),
        crabber_core::ControlId(operation.clone()),
    )) else {
        return Err(unknown("original terminal receipt"));
    };
    proof.measure(receipt)?;
    if receipt.attempt != record.snapshot || event.cursor != Some(receipt.event_cursor) {
        return Err(unknown("current terminal attempt"));
    }
    let original = crate::attempt::terminal::settlement(receipt, event).map_err(unknown)?;
    let source = state
        .events
        .get(original.cursor.ok_or_else(|| unknown("terminal cursor"))?)
        .ok_or_else(|| unknown("original terminal settlement"))?;
    proof.measure(source)?;
    // This verifier compares actual private error/usage and independently latest
    // same-run settlement before the original audit, without fabricating a Run.
    proof.measure(&(&run.error, &run.usage, run.status))?;
    super::super::attempt::verify(state, receipt).map_err(unknown)?;
    Ok(true)
}

#[cfg(test)]
#[path = "run_history_tests.rs"]
mod tests;

native_pending_initial_items! {
    #[cfg(test)]
    #[path="run_history/classification_tests.rs"]
    mod classification_tests;
}

native_pending_initial_items! {
    #[cfg(test)]
    impl crate::memory::MemoryStore {
        pub(crate) fn fixed_initial_fixture_report(&self, session: &SessionId) -> Result<RunHistoryReport, Failure> {
            self.fixed_initial_fixture_report_with_proof(session, &mut Proof::bounded(crate::deletion::coverage::MAX_PROOF_BYTES))
        }
        pub(crate) fn fixed_initial_fixture_report_with_proof(&self, session: &SessionId, proof: &mut Proof) -> Result<RunHistoryReport, Failure> {
            let state = self.state.lock().map_err(unknown)?;
            let run = state.runs.values().find(|run| &run.session_id == session).ok_or_else(|| unknown("fixed fixture missing Run"))?;
            report(&state, run, proof)
        }
    }
}
