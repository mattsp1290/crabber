//! Typed, read-only context contract for tool result transforms.
use crate::dispatch::Callback;
use crate::plan::ExtensionError;
use crabber_core::{RunId, SessionId, ToolCallId};
use futures::future::BoxFuture;
use serde_json::{Value, json};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tokio_util::task::TaskTracker;

/// Version of the result-transform contract. The untyped contract was version 1.
pub const RESULT_TRANSFORM_CONTRACT_VERSION: u32 = 2;
/// Deadline for final redactors, measured from the instant cancellation was
/// observed. Enforced by the chain driver.
pub const FINAL_REDACTION_DEADLINE: Duration = Duration::from_millis(500);
/// Default bound a mount close waits for plan leases and the cleanup tracker.
pub const DEFAULT_MOUNT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The fixed runtime text of a result-transform failure naming the handler id.
#[must_use]
pub fn result_transform_failed_message(handler: &str) -> String {
    format!("result transform failed: {handler}")
}

/// The input a tool call carried, tagged by how trustworthy its form is.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolInput {
    Normalized(Value),
    Raw(Value),
    Unavailable { reason: InputUnavailable },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputUnavailable {
    /// Preparation failed; there is no normalized input.
    PrepareFailed,
    /// The record holds no raw provider arguments for an unresolved tool.
    Unresolved,
}
impl InputUnavailable {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PrepareFailed => "prepare_failed",
            Self::Unresolved => "unresolved",
        }
    }
}

/// Immutable class of a call's outcome; no handler can change it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolOutcomeClass {
    Succeeded,
    ExecutionFailed,
    PermissionDenied,
    UnknownTool,
    PrepareFailed,
}
impl ToolOutcomeClass {
    /// False only for `Succeeded`.
    #[must_use]
    pub const fn is_error(self) -> bool {
        !matches!(self, Self::Succeeded)
    }
    /// The JSON encoding of the class.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::ExecutionFailed => "execution_failed",
            Self::PermissionDenied => "permission_denied",
            Self::UnknownTool => "unknown_tool",
            Self::PrepareFailed => "prepare_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformPhase {
    Ordinary,
    FinalRedaction,
}
impl TransformPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ordinary => "ordinary",
            Self::FinalRedaction => "final_redaction",
        }
    }
}

/// What a handler returns: the only value it can change, plus the one-way
/// escalation of success to error.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformOutput {
    pub result: Value,
    pub mark_error: bool,
}
impl TransformOutput {
    #[must_use]
    pub fn new(result: Value) -> Self {
        Self {
            result,
            mark_error: false,
        }
    }
    #[must_use]
    pub fn marked_error(result: Value) -> Self {
        Self {
            result,
            mark_error: true,
        }
    }
}

/// What the runtime gets back from the chain driver.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolResultOutcome {
    /// Every handler ran; cancellation was not observed.
    Completed { result: Value, is_error: bool },
    /// A handler failed. Carries only the id of the failing handler.
    Failed { handler: String },
    /// Cancellation was observed. `Some` only when an accepted value passed
    /// every remaining final redactor (settlement precedence).
    Interrupted { redacted: Option<Value> },
}

pub type ResultTransformCallback = Arc<
    dyn Fn(ToolResultContext, Value) -> BoxFuture<'static, Result<TransformOutput, ExtensionError>>
        + Send
        + Sync,
>;

/// Handed to callbacks. Wraps a `TaskTracker` and a close signal.
#[derive(Debug, Clone)]
pub struct CleanupTracker {
    tasks: TaskTracker,
    closing: CancellationToken,
}
impl CleanupTracker {
    /// A tracker with no owner: its close signal never fires and nothing joins it.
    #[must_use]
    pub fn detached() -> Self {
        Self::from_parts(TaskTracker::new(), CancellationToken::new())
    }
    /// Builds a tracker from its owner's task tracker and close token.
    ///
    /// Used by `CleanupOwner` (crabber-2xrl), which keeps the other halves.
    pub(crate) fn from_parts(tasks: TaskTracker, closing: CancellationToken) -> Self {
        Self { tasks, closing }
    }
    /// Spawns on the current Tokio runtime. The task is tracked until it
    /// finishes, whether or not the returned handle or the caller is dropped.
    pub fn spawn<F>(&self, task: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.tasks.spawn(task)
    }
    /// Resolves when the owner starts closing. `'static`, so it can move into a task.
    pub fn closing(&self) -> WaitForCancellationFutureOwned {
        self.closing.clone().cancelled_owned()
    }
    #[must_use]
    pub fn is_closing(&self) -> bool {
        self.closing.is_cancelled()
    }
    /// Tracked tasks that have not finished.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.tasks.len()
    }
}

/// Authoritative, read-only context of one tool call's result transform.
///
/// Anyone can construct a value, for example in tests. Only the instance the
/// runtime builds is authoritative: the runtime never reads a context back from
/// a handler, and the driver replaces phase, effective `is_error` and tracker
/// for every handler it invokes. The cancellation token is never part of any
/// JSON.
#[derive(Debug, Clone)]
pub struct ToolResultContext {
    tool_name: String,
    resolved: bool,
    input: ToolInput,
    call_id: ToolCallId,
    session_id: SessionId,
    run_id: RunId,
    class: ToolOutcomeClass,
    is_error: bool,
    phase: TransformPhase,
    cancellation: CancellationToken,
    cleanup: CleanupTracker,
}
impl ToolResultContext {
    /// Phase is `Ordinary`, the token is never cancelled and the tracker is
    /// detached until replaced.
    #[must_use]
    pub fn new(
        tool_name: String,
        resolved: bool,
        input: ToolInput,
        call_id: ToolCallId,
        session_id: SessionId,
        run_id: RunId,
        class: ToolOutcomeClass,
    ) -> Self {
        Self {
            tool_name,
            resolved,
            input,
            call_id,
            session_id,
            run_id,
            class,
            is_error: class.is_error(),
            phase: TransformPhase::Ordinary,
            cancellation: CancellationToken::new(),
            cleanup: CleanupTracker::detached(),
        }
    }
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }
    #[must_use]
    pub fn with_cleanup(mut self, cleanup: CleanupTracker) -> Self {
        self.cleanup = cleanup;
        self
    }
    /// Provider-requested tool name.
    #[must_use]
    pub fn tool_name(&self) -> &str {
        &self.tool_name
    }
    /// False only for class `UnknownTool`.
    #[must_use]
    pub fn resolved(&self) -> bool {
        self.resolved
    }
    #[must_use]
    pub fn input(&self) -> &ToolInput {
        &self.input
    }
    #[must_use]
    pub fn call_id(&self) -> &ToolCallId {
        &self.call_id
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    #[must_use]
    pub fn class(&self) -> ToolOutcomeClass {
        self.class
    }
    /// `class().is_error()`, or an earlier handler or the pre-stage set `mark_error`.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.is_error
    }
    #[must_use]
    pub fn phase(&self) -> TransformPhase {
        self.phase
    }
    #[must_use]
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
    #[must_use]
    pub fn cleanup(&self) -> &CleanupTracker {
        &self.cleanup
    }
    /// Driver-only: the phase of the handler about to run.
    pub(crate) fn set_phase(&mut self, phase: TransformPhase) {
        self.phase = phase;
    }
    /// Driver-only: the effective `is_error` before the handler about to run.
    pub(crate) fn set_is_error(&mut self, is_error: bool) {
        self.is_error = is_error;
    }
    fn context_json(&self) -> Value {
        let input = match &self.input {
            ToolInput::Normalized(value) => json!({"kind": "normalized", "value": value}),
            ToolInput::Raw(value) => json!({"kind": "raw", "value": value}),
            ToolInput::Unavailable { reason } => {
                json!({"kind": "unavailable", "reason": reason.as_str()})
            }
        };
        json!({
            "tool_name": self.tool_name,
            "resolved": self.resolved,
            "input": input,
            "call_id": self.call_id.to_string(),
            "session_id": self.session_id.to_string(),
            "run_id": self.run_id.to_string(),
            "class": self.class.as_str(),
            "is_error": self.is_error,
            "phase": self.phase.as_str(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    NotAnObject,
    MissingField(&'static str),
    UnknownField,
    ContextChanged,
    MarkErrorNotBool,
}

/// The JSON handler input: context, `result` and `mark_error: false`.
#[must_use]
#[allow(clippy::needless_pass_by_value)] // signature fixed by the design
pub fn result_envelope(context: &ToolResultContext, result: Value) -> Value {
    json!({
        "context": context.context_json(),
        "result": result,
        "mark_error": false,
    })
}

/// Verifies a JSON handler's reply against the context that was sent.
///
/// # Errors
/// Returns the first envelope violation: not an object, a missing or unknown
/// top-level key, a context that differs from `context`, or a non-boolean
/// `mark_error`.
pub fn parse_result_envelope(
    context: &ToolResultContext,
    reply: Value,
) -> Result<TransformOutput, EnvelopeError> {
    let Value::Object(mut object) = reply else {
        return Err(EnvelopeError::NotAnObject);
    };
    let context_value = object
        .remove("context")
        .ok_or(EnvelopeError::MissingField("context"))?;
    let result = object
        .remove("result")
        .ok_or(EnvelopeError::MissingField("result"))?;
    let mark_error = object
        .remove("mark_error")
        .ok_or(EnvelopeError::MissingField("mark_error"))?;
    if !object.is_empty() {
        return Err(EnvelopeError::UnknownField);
    }
    if context_value != context.context_json() {
        return Err(EnvelopeError::ContextChanged);
    }
    let Value::Bool(mark_error) = mark_error else {
        return Err(EnvelopeError::MarkErrorNotBool);
    };
    Ok(TransformOutput { result, mark_error })
}

/// The envelope adapter both JSON registrations use. Any envelope violation is
/// an `Err`; its text is discarded by the driver.
#[must_use]
pub fn json_result_transform(cb: Callback) -> ResultTransformCallback {
    Arc::new(move |context, result| {
        let reply = cb(result_envelope(&context, result));
        Box::pin(async move {
            let reply = reply.await?;
            parse_result_envelope(&context, reply).map_err(|error| {
                ExtensionError::Tool(format!("invalid result envelope: {error:?}"))
            })
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn context(input: ToolInput, class: ToolOutcomeClass) -> ToolResultContext {
        ToolResultContext::new(
            "read_file".into(),
            true,
            input,
            ToolCallId::from("call-1"),
            SessionId::from("session-1"),
            RunId::from("run-1"),
            class,
        )
    }
    fn base() -> ToolResultContext {
        context(
            ToolInput::Normalized(json!({"path": "a.txt"})),
            ToolOutcomeClass::Succeeded,
        )
    }
    const CLASSES: [ToolOutcomeClass; 5] = [
        ToolOutcomeClass::Succeeded,
        ToolOutcomeClass::ExecutionFailed,
        ToolOutcomeClass::PermissionDenied,
        ToolOutcomeClass::UnknownTool,
        ToolOutcomeClass::PrepareFailed,
    ];
    fn inputs() -> Vec<ToolInput> {
        vec![
            ToolInput::Normalized(json!({"a": 1})),
            ToolInput::Raw(json!([1, 2])),
            ToolInput::Unavailable {
                reason: InputUnavailable::PrepareFailed,
            },
            ToolInput::Unavailable {
                reason: InputUnavailable::Unresolved,
            },
        ]
    }

    #[test]
    fn accessors_expose_construction_values() {
        let ctx = base();
        assert_eq!(ctx.tool_name(), "read_file");
        assert!(ctx.resolved());
        assert_eq!(
            ctx.input(),
            &ToolInput::Normalized(json!({"path": "a.txt"}))
        );
        assert_eq!(ctx.call_id().to_string(), "call-1");
        assert_eq!(ctx.session_id().to_string(), "session-1");
        assert_eq!(ctx.run_id().to_string(), "run-1");
        assert_eq!(ctx.class(), ToolOutcomeClass::Succeeded);
        assert!(!ctx.is_error());
        assert_eq!(ctx.phase(), TransformPhase::Ordinary);
        assert!(!ctx.cancellation().is_cancelled());
        assert!(!ctx.cleanup().is_closing());
        let failed = context(ToolInput::Raw(json!(null)), ToolOutcomeClass::UnknownTool);
        assert!(failed.is_error());
    }

    #[test]
    fn builders_replace_token_and_tracker() {
        let token = CancellationToken::new();
        let owner_closing = CancellationToken::new();
        let tracker = CleanupTracker::from_parts(TaskTracker::new(), owner_closing.clone());
        let ctx = base()
            .with_cancellation(token.clone())
            .with_cleanup(tracker);
        token.cancel();
        assert!(ctx.cancellation().is_cancelled());
        assert!(!ctx.cleanup().is_closing());
        owner_closing.cancel();
        assert!(ctx.cleanup().is_closing());
    }

    #[test]
    fn envelope_shape_is_exact() {
        let envelope = result_envelope(&base(), json!("out"));
        assert_eq!(
            envelope,
            json!({
                "context": {
                    "tool_name": "read_file",
                    "resolved": true,
                    "input": {"kind": "normalized", "value": {"path": "a.txt"}},
                    "call_id": "call-1",
                    "session_id": "session-1",
                    "run_id": "run-1",
                    "class": "succeeded",
                    "is_error": false,
                    "phase": "ordinary"
                },
                "result": "out",
                "mark_error": false
            })
        );
        let unavailable = context(
            ToolInput::Unavailable {
                reason: InputUnavailable::Unresolved,
            },
            ToolOutcomeClass::PrepareFailed,
        );
        let envelope = result_envelope(&unavailable, json!(null));
        assert_eq!(
            envelope["context"]["input"],
            json!({"kind": "unavailable", "reason": "unresolved"})
        );
        assert_eq!(envelope["context"]["class"], "prepare_failed");
        assert_eq!(envelope["context"]["is_error"], true);
        let mut redaction = base();
        redaction.set_phase(TransformPhase::FinalRedaction);
        assert_eq!(
            result_envelope(&redaction, json!(1))["context"]["phase"],
            "final_redaction"
        );
    }

    #[test]
    fn envelope_round_trips_every_input_and_class() {
        for input in inputs() {
            for class in CLASSES {
                let ctx = context(input.clone(), class);
                let value = json!({"k": [1, null]});
                let parsed = parse_result_envelope(&ctx, result_envelope(&ctx, value.clone()));
                assert_eq!(
                    parsed,
                    Ok(TransformOutput {
                        result: value,
                        mark_error: false
                    })
                );
            }
        }
    }

    #[test]
    fn mark_error_true_and_null_result_are_accepted() {
        let ctx = base();
        let mut reply = result_envelope(&ctx, json!(null));
        reply["mark_error"] = json!(true);
        assert_eq!(
            parse_result_envelope(&ctx, reply),
            Ok(TransformOutput::marked_error(json!(null)))
        );
    }

    #[test]
    fn every_tampered_context_field_is_rejected() {
        let ctx = base();
        let tampers: Vec<(&str, Value)> = vec![
            ("tool_name", json!("other")),
            ("resolved", json!(false)),
            ("input", json!({"kind": "raw", "value": {"path": "a.txt"}})),
            (
                "input",
                json!({"kind": "normalized", "value": {"path": "b.txt"}}),
            ),
            (
                "input",
                json!({"kind": "unavailable", "reason": "prepare_failed"}),
            ),
            ("call_id", json!("call-2")),
            ("session_id", json!("session-2")),
            ("run_id", json!("run-2")),
            ("class", json!("execution_failed")),
            ("is_error", json!(true)),
            ("phase", json!("final_redaction")),
        ];
        for (field, value) in tampers {
            let mut reply = result_envelope(&ctx, json!("x"));
            reply["context"][field] = value;
            assert_eq!(
                parse_result_envelope(&ctx, reply),
                Err(EnvelopeError::ContextChanged),
                "{field}"
            );
        }
        let unavailable = context(
            ToolInput::Unavailable {
                reason: InputUnavailable::PrepareFailed,
            },
            ToolOutcomeClass::PrepareFailed,
        );
        let mut reply = result_envelope(&unavailable, json!("x"));
        reply["context"]["input"]["reason"] = json!("unresolved");
        assert_eq!(
            parse_result_envelope(&unavailable, reply),
            Err(EnvelopeError::ContextChanged)
        );
        let mut reply = result_envelope(&ctx, json!("x"));
        reply["context"]["extra"] = json!(1);
        assert_eq!(
            parse_result_envelope(&ctx, reply),
            Err(EnvelopeError::ContextChanged)
        );
    }

    #[test]
    fn malformed_envelopes_are_rejected() {
        let ctx = base();
        for field in ["result", "context", "mark_error"] {
            let mut reply = result_envelope(&ctx, json!("x"));
            reply.as_object_mut().unwrap().remove(field);
            assert_eq!(
                parse_result_envelope(&ctx, reply),
                Err(EnvelopeError::MissingField(field))
            );
        }
        assert_eq!(
            parse_result_envelope(&ctx, json!("text")),
            Err(EnvelopeError::NotAnObject)
        );
        let mut reply = result_envelope(&ctx, json!("x"));
        reply["is_error"] = json!(true);
        assert_eq!(
            parse_result_envelope(&ctx, reply),
            Err(EnvelopeError::UnknownField)
        );
        let mut reply = result_envelope(&ctx, json!("x"));
        reply["mark_error"] = json!("true");
        assert_eq!(
            parse_result_envelope(&ctx, reply),
            Err(EnvelopeError::MarkErrorNotBool)
        );
    }

    #[test]
    fn class_encoding() {
        let expected = [
            "succeeded",
            "execution_failed",
            "permission_denied",
            "unknown_tool",
            "prepare_failed",
        ];
        for (class, text) in CLASSES.into_iter().zip(expected) {
            assert_eq!(class.as_str(), text);
            assert_eq!(class.is_error(), class != ToolOutcomeClass::Succeeded);
        }
    }

    #[test]
    fn constants_and_fixed_text() {
        assert_eq!(RESULT_TRANSFORM_CONTRACT_VERSION, 2);
        assert_eq!(FINAL_REDACTION_DEADLINE, Duration::from_millis(500));
        assert_eq!(DEFAULT_MOUNT_CLOSE_TIMEOUT, Duration::from_secs(5));
        assert_eq!(
            result_transform_failed_message("h"),
            "result transform failed: h"
        );
    }

    #[test]
    fn transform_output_constructors() {
        assert!(!TransformOutput::new(json!(1)).mark_error);
        assert!(TransformOutput::marked_error(json!(1)).mark_error);
    }

    #[tokio::test]
    async fn json_adapter_accepts_well_behaved_callback() {
        let cb: Callback = Arc::new(|mut envelope| {
            async move {
                envelope["result"] = json!("redacted");
                envelope["mark_error"] = json!(true);
                Ok(envelope)
            }
            .boxed()
        });
        let adapter = json_result_transform(cb);
        let output = adapter(base(), json!("secret")).await;
        assert_eq!(output, Ok(TransformOutput::marked_error(json!("redacted"))));
    }

    #[tokio::test]
    async fn json_adapter_rejects_tampering_and_errors() {
        let tamper: Callback = Arc::new(|mut envelope| {
            async move {
                envelope["context"]["class"] = json!("succeeded2");
                Ok(envelope)
            }
            .boxed()
        });
        assert!(
            json_result_transform(tamper)(base(), json!(1))
                .await
                .is_err()
        );
        let legacy: Callback =
            Arc::new(|_| async { Ok(json!({"result": 1, "is_error": false})) }.boxed());
        assert!(
            json_result_transform(legacy)(base(), json!(1))
                .await
                .is_err()
        );
        let failing: Callback =
            Arc::new(|_| async { Err(ExtensionError::Tool("boom".into())) }.boxed());
        assert!(
            json_result_transform(failing)(base(), json!(1))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn detached_tracker_runs_tasks_and_never_closes() {
        let tracker = CleanupTracker::detached();
        assert!(!tracker.is_closing());
        assert!(tracker.closing().now_or_never().is_none());
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<u32>();
        let spawner = tracker.clone();
        let caller = async move {
            spawner.spawn(async move {
                let _ = gate.await;
                let _ = done_tx.send(7);
            });
            std::future::pending::<()>().await;
        };
        // Poll the spawning future once, then drop it.
        assert!(caller.now_or_never().is_none());
        tokio::task::yield_now().await;
        assert_eq!(tracker.pending(), 1);
        release.send(()).unwrap();
        assert_eq!(done_rx.await, Ok(7));
        tokio::task::yield_now().await;
        assert_eq!(tracker.pending(), 0);
    }
}
