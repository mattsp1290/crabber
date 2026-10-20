//! Finite, content-free runtime span fields shared by attempt entry points.
use super::{Orchestrator, RunResult, RuntimeError};
use crate::{TerminalReason, telemetry_targets::targets};
impl Orchestrator {
    pub(super) fn attempt_span(
        &self,
        resume: bool,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
    ) -> tracing::Span {
        // Capture happens at public entry, before its owned preparation task is spawned.
        // Consume the host handle here: Registry retains the parent until this
        // child closes; tracing-opentelemetry measures it to its last exit.
        let parent = self
            .attempt
            .as_ref()
            .and_then(|identity| {
                identity
                    .parent
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            })
            .unwrap_or_else(tracing::Span::none);
        let attempt = self
            .attempt
            .as_ref()
            .map_or_else(|| "none".into(), |identity| identity.attempt.to_string());
        macro_rules! make { ($name:literal) => { tracing::info_span!(target: targets::RUNTIME, parent: &parent, $name, "session.id" = %session, "run.id" = %run, "attempt.id" = %attempt, outcome = tracing::field::Empty, reason = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty, "host.trace_id" = tracing::field::Empty, "host.span_id" = tracing::field::Empty, "host.predecessor_trace_id" = tracing::field::Empty) }; }
        let span = if resume {
            make!("crabber.resume")
        } else {
            make!("crabber.run")
        };
        if let Some(context) = self
            .attempt
            .as_ref()
            .and_then(|identity| identity.context.as_ref())
        {
            span.record("host.trace_id", context.trace_id());
            span.record("host.span_id", context.span_id());
            if let Some(prior) = context.predecessor() {
                span.record("host.predecessor_trace_id", prior.trace_id());
            }
        }
        span
    }
    pub(super) fn lifecycle(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        outcome: Option<&str>,
    ) {
        let attempt = self
            .attempt
            .as_ref()
            .map_or_else(|| "none".into(), |identity| identity.attempt.to_string());
        let context = self
            .attempt
            .as_ref()
            .and_then(|identity| identity.context.as_ref());
        let trace = context.map_or("", crabber_core::TraceContext::trace_id);
        let host_span = context.map_or("", crabber_core::TraceContext::span_id);
        if let Some(outcome) = outcome {
            tracing::info!(target: targets::LIFECYCLE, { "session.id" = %session, "run.id" = %run, "attempt.id" = %attempt, "host.trace_id" = trace, "host.span_id" = host_span, outcome = outcome }, "run settled");
        } else {
            tracing::info!(target: targets::LIFECYCLE, { "session.id" = %session, "run.id" = %run, "attempt.id" = %attempt, "host.trace_id" = trace, "host.span_id" = host_span }, "run started");
        }
    }
}
pub(super) fn reason_label(reason: TerminalReason) -> &'static str {
    match reason {
        TerminalReason::Success => "success",
        TerminalReason::ProviderError => "provider_error",
        TerminalReason::ToolError => "tool_error",
        TerminalReason::Cancelled => "cancelled",
        TerminalReason::LeaseLost => "lease_lost",
        TerminalReason::Paused => "paused",
        TerminalReason::RuntimeError => "runtime_error",
    }
}
pub(super) fn record_result(
    span: &tracing::Span,
    result: &Result<RunResult, RuntimeError>,
) -> &'static str {
    let reason = crate::observation::result_reason(result);
    let outcome = match reason {
        TerminalReason::Success => "completed",
        TerminalReason::Paused => "paused",
        TerminalReason::Cancelled => "interrupted",
        _ => "failed",
    };
    span.record("outcome", outcome);
    span.record("reason", reason_label(reason));
    span.record(
        "otel.status_code",
        if outcome == "failed" { "ERROR" } else { "OK" },
    );
    if let Err(error) = result {
        span.record("error.kind", error.error_kind());
    }
    outcome
}

pub(super) fn record_model_error(span: &tracing::Span, error: &RuntimeError) {
    let (outcome, reason) = match error {
        RuntimeError::Interrupted => ("cancelled", "cancelled"),
        RuntimeError::LeaseLost => ("lease_lost", "lease_lost"),
        RuntimeError::Provider(_) => ("failed", "provider_error"),
        _ => ("failed", "runtime_error"),
    };
    span.record("outcome", outcome);
    span.record("reason", reason);
    span.record("error.kind", error.error_kind());
    span.record(
        "otel.status_code",
        if outcome == "cancelled" {
            "OK"
        } else {
            "ERROR"
        },
    );
}

pub(super) fn provider_error_kind(error: &crabber_providers::ProviderError) -> &'static str {
    use crabber_providers::ProviderErrorKind;
    match error.kind {
        ProviderErrorKind::Auth => "provider_auth",
        ProviderErrorKind::RateLimited => "provider_rate_limited",
        ProviderErrorKind::ContextOverflow => "provider_context_overflow",
        ProviderErrorKind::Transport => "provider_transport",
        ProviderErrorKind::Invalid => "provider_invalid",
        ProviderErrorKind::Server => "provider_server",
        ProviderErrorKind::Canceled => "provider_canceled",
    }
}
