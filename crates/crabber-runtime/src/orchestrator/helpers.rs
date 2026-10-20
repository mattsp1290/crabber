use super::{
    AtomicBool, ContentBlock, InputUnavailable, Message, MessageId, ModelRequestError,
    OffsetDateTime, Ordering, PREPARE_ERROR_KEY, Part, PartId, PartKind, ProviderError,
    RESERVED_ARGUMENT_KEY_TEXT, RESERVED_KEY_PREFIX, Role, RunId, RunPlan, RuntimeError, SessionId,
    ToolCallId, ToolCallRecord, ToolCallStatus, ToolInfo, ToolInput, ToolOutcomeClass,
    TransformOutput, UNKNOWN_TOOL_KEY, Value, json,
};

pub(super) fn runtime_error_output(error: &str) -> crabber_core::ToolOutput {
    crabber_core::ToolOutput::from_json(&Value::String(error.into())).unwrap_or_else(|_| {
        crabber_core::ToolOutput::from_json(&Value::String("tool result unavailable".into()))
            .expect("fixed runtime error is valid")
    })
}

pub(super) fn storable_error_text(error: &RuntimeError) -> String {
    error.to_string().replace('\0', "\u{fffd}")
}

pub(super) fn storable_call_arguments(parsed: Value, raw: &str) -> Value {
    let message_id = MessageId::new();
    let probe = Message {
        id: message_id.clone(),
        session_id: SessionId::new(),
        run_id: Some(RunId::new()),
        role: Role::Assistant,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id,
            ordinal: 0,
            kind: PartKind::FunctionToolCall,
            content: ContentBlock::ToolCall {
                call_id: ToolCallId::new(),
                name: String::new(),
                arguments: parsed.clone(),
            },
        }],
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    if crabber_session::ensure_storable(&probe).is_ok() {
        parsed
    } else {
        Value::String(raw.to_owned())
    }
}

/// The record arguments for an unknown tool. Stores write records without a depth limit but decode
/// them with `serde_json::from_str` (128 levels), and both the sentinel and the enclosing
/// `ToolCallRecord` add levels. When the whole record would not decode back unchanged, `raw` holds
/// the arguments' JSON text instead, as for unparseable provider text. That form is a fixed depth
/// (a string is a scalar), so it always decodes.
pub(crate) fn unknown_tool_arguments(raw: &Value) -> Value {
    let arguments = json!({UNKNOWN_TOOL_KEY: {"raw": raw}});
    let probe = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: RunId::new(),
        name: String::new(),
        arguments: arguments.clone(),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    if crabber_session::ensure_storable(&probe).is_ok() {
        return arguments;
    }
    let text = serde_json::to_string(raw).unwrap_or_default();
    json!({UNKNOWN_TOOL_KEY: {"raw": text}})
}

/// Typed result of running a resolved call, so the class never depends on error text.
pub(super) enum ToolRun {
    Suspended(crabber_core::ToolSuspension),
    SuspensionInvalid,
    Output(TransformOutput),
    PipelineFailed,
    ProtectionInterrupted,
    PermissionDenied,
    ExecutionFailed(String),
}

/// What the stored `record.arguments` say about a call (Recorded answer 1).
#[derive(Debug, PartialEq)]
pub(crate) enum RecordedCall {
    /// A resolved call with real arguments: class comes from execution.
    Execute(Value),
    /// The call does not execute; class, input and the chain's seed text come from the record.
    Settled {
        class: ToolOutcomeClass,
        input: ToolInput,
        resolved: bool,
        seed: String,
    },
}

/// An object whose only key is reserved for the runtime.
fn sole_reserved_key(arguments: &Value) -> Option<(&str, &Value)> {
    let object = arguments.as_object().filter(|object| object.len() == 1)?;
    let (key, value) = object.iter().next()?;
    key.starts_with(RESERVED_KEY_PREFIX)
        .then_some((key.as_str(), value))
}

pub(super) fn is_reserved_arguments(arguments: &Value) -> bool {
    sole_reserved_key(arguments).is_some()
}

pub(crate) fn read_recorded_call(arguments: Value, name: &str, resolves: bool) -> RecordedCall {
    let unknown_seed = || format!("unknown tool: {name}");
    let prepare_failed = |seed: String| RecordedCall::Settled {
        class: ToolOutcomeClass::PrepareFailed,
        input: ToolInput::Unavailable {
            reason: InputUnavailable::PrepareFailed,
        },
        resolved: true,
        seed,
    };
    match sole_reserved_key(&arguments) {
        Some((UNKNOWN_TOOL_KEY, value)) => RecordedCall::Settled {
            class: ToolOutcomeClass::UnknownTool,
            input: match value.as_object().and_then(|object| object.get("raw")) {
                Some(raw) => ToolInput::Raw(raw.clone()),
                None => ToolInput::Unavailable {
                    reason: InputUnavailable::Unresolved,
                },
            },
            resolved: false,
            seed: unknown_seed(),
        },
        Some((PREPARE_ERROR_KEY, value)) => prepare_failed(
            value
                .as_str()
                .unwrap_or(RESERVED_ARGUMENT_KEY_TEXT)
                .to_owned(),
        ),
        Some(_) => prepare_failed(RESERVED_ARGUMENT_KEY_TEXT.to_owned()),
        None if resolves => RecordedCall::Execute(arguments),
        None => RecordedCall::Settled {
            class: ToolOutcomeClass::UnknownTool,
            input: ToolInput::Unavailable {
                reason: InputUnavailable::Unresolved,
            },
            resolved: false,
            seed: unknown_seed(),
        },
    }
}

pub(super) fn push_part(
    parts: &mut Vec<Part>,
    message_id: &MessageId,
    kind: PartKind,
    content: ContentBlock,
) {
    parts.push(Part {
        id: PartId::new(),
        message_id: message_id.clone(),
        ordinal: u32::try_from(parts.len()).expect("assistant part count fits u32"),
        kind,
        content,
    });
}

pub(super) fn invalid_provider(message: &str) -> ProviderError {
    ProviderError {
        kind: crabber_providers::ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

pub(super) fn ensure_lease(lost: &AtomicBool) -> Result<(), RuntimeError> {
    if lost.load(Ordering::SeqCst) {
        Err(RuntimeError::LeaseLost)
    } else {
        Ok(())
    }
}

pub(super) fn validate_arguments(tool: &ToolInfo, arguments: &Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(&tool.parameters)
        .map_err(|error| format!("invalid tool schema: {error}"))?;
    validator
        .validate(arguments)
        .map_err(|error| format!("invalid tool arguments: {error}"))
}

pub(super) struct RetryDecision {
    pub(super) retry: bool,
    pub(super) delay_ms: u64,
    pub(super) compaction_requested: bool,
}

pub(super) async fn request_error(
    plan: &RunPlan,
    error: &ProviderError,
    attempt: u32,
    retry: bool,
    delay_ms: u64,
) -> Result<RetryDecision, RuntimeError> {
    let class = format!("{:?}", error.kind);
    let seed = json!({"class":class,"attempt":attempt,"retry":retry,"delay_ms":delay_ms,"compaction_requested":error.kind == crabber_providers::ProviderErrorKind::ContextOverflow});
    match plan.dispatcher.transform::<ModelRequestError>(seed).await {
        Ok(decision)
            if decision.get("class").and_then(Value::as_str) != Some(class.as_str())
                || decision.get("attempt").and_then(Value::as_u64) != Some(u64::from(attempt)) =>
        {
            Err(RuntimeError::Extension(
                "request-error handler changed immutable metadata".into(),
            ))
        }
        Ok(decision) if decision.get("retry").and_then(Value::as_bool) == Some(true) && !retry => {
            Err(RuntimeError::Extension(
                "request-error handler cannot enable retry".into(),
            ))
        }
        Ok(decision)
            if decision
                .get("delay_ms")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > delay_ms =>
        {
            Err(RuntimeError::Extension(
                "request-error handler cannot lengthen delay".into(),
            ))
        }
        Ok(decision)
            if decision
                .get("compaction_requested")
                .and_then(Value::as_bool)
                == Some(true)
                && error.kind != crabber_providers::ProviderErrorKind::ContextOverflow =>
        {
            Err(RuntimeError::Extension(
                "compaction is only valid for context overflow".into(),
            ))
        }
        Ok(decision) => Ok(RetryDecision {
            retry: decision["retry"].as_bool().unwrap_or(false),
            delay_ms: decision["delay_ms"].as_u64().unwrap_or(0),
            compaction_requested: decision["compaction_requested"].as_bool().unwrap_or(false),
        }),
        Err(handler) => Err(RuntimeError::Extension(handler.to_string())),
    }
}
