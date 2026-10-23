//! Native question state machine; only recorded host responses reach resume.
use crate::{Options, Surface, parse_args, validate_answer};
use async_trait::async_trait;
use crabber::{
    ExtensionError,
    core::{
        AcceptedToolResponse, InputFingerprint, InteractionDescription, InteractionKind,
        InteractionState, ResumableToolIdentity, ToolCheckpoint, ToolPhaseOutcome, ToolSuspension,
    },
    extension::{ResumableTool, ToolContext},
};
use crabber_tools_core::{
    CONFIG_IDENTITY_VERSION, category, check_cancelled, config_hash, failed, to_output,
};
use serde_json::{Value, json};
use std::sync::Arc;
const SCHEMA: &str = "crabber-tools.user-interact.checkpoint";
pub(crate) struct DurableTool {
    pub options: Arc<Options>,
}
fn completed(value: Value) -> ToolPhaseOutcome {
    ToolPhaseOutcome::Completed(to_output(value, &[]))
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Owned error callbacks match the Result::map_err interface."
)]
fn invalid(error: crabber::core::CoreError) -> ExtensionError {
    ExtensionError::Tool(error.to_string())
}
#[async_trait]
impl ResumableTool for DurableTool {
    fn identity(&self) -> ResumableToolIdentity {
        ResumableToolIdentity {
            component_id: "crabber-tools.user-interact".into(),
            component_version: env!("CARGO_PKG_VERSION").into(),
            config_fingerprint: InputFingerprint::new(config_hash(&(
                CONFIG_IDENTITY_VERSION,
                &self.options.policy,
            )))
            .expect("SHA-256 identity"),
            checkpoint_schema_id: SCHEMA.into(),
            checkpoint_schema_version: "1".into(),
        }
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "Retain the timing ABI; u64 milliseconds overflow only after 584 million years."
    )]
    async fn start(
        &self,
        ctx: ToolContext,
        arguments: Value,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let args = match parse_args(arguments, &self.options.policy) {
            Ok(args) => args,
            Err(failure) => return Ok(completed(failure)),
        };
        if !args.answer.is_empty() {
            return Ok(completed(failed(
                category::VALIDATION,
                "answer is reserved for the pending surface",
            )));
        }
        let Surface::Durable {
            authority,
            expires_after,
        } = &self.options.policy.surface
        else {
            return Err(ExtensionError::Tool("durable policy required".into()));
        };
        let checkpoint =
            ToolCheckpoint::new(SCHEMA, "1", json!({"stage":"asked"})).map_err(invalid)?;
        let question = InteractionDescription::new(
            "answer",
            InteractionKind::UserInput,
            args.question,
            json!({"type":"string", "maxLength":self.options.policy.max_answer_bytes}),
            authority.clone(),
            expires_after.map(|d| d.as_millis() as u64),
        )
        .map_err(invalid)?;
        Ok(ToolPhaseOutcome::Suspended(
            ToolSuspension::new(checkpoint, vec![question]).map_err(invalid)?,
        ))
    }
    async fn resume(
        &self,
        ctx: ToolContext,
        _: Value,
        checkpoint: ToolCheckpoint,
        responses: Vec<AcceptedToolResponse>,
    ) -> Result<ToolPhaseOutcome, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        if !self.identity().accepts(&checkpoint)
            || checkpoint.value()["stage"] != "asked"
            || responses.len() != 1
            || responses[0].name != "answer"
        {
            return Err(ExtensionError::Tool(
                "unexpected user_interact continuation".into(),
            ));
        }
        let response = &responses[0];
        let value = match response.state {
            InteractionState::Answered => match response.value.as_ref().and_then(Value::as_str) {
                Some(answer) => {
                    let answer = answer.trim_end_matches('\n');
                    if let Some(failure) =
                        validate_answer(answer, self.options.policy.max_answer_bytes)
                    {
                        failure
                    } else if answer.is_empty() {
                        json!({"outcome":"succeeded"})
                    } else {
                        json!({"outcome":"succeeded", "answer":answer})
                    }
                }
                None => failed(category::UNKNOWN, "invalid recorded answer"),
            },
            InteractionState::Denied => failed(category::DENIED, "user denied the question"),
            InteractionState::Cancelled => {
                failed(category::CANCELLED, "question was cancelled by the host")
            }
            InteractionState::Expired => failed(category::TIMEOUT, "user response expired"),
            InteractionState::Pending => failed(category::UNKNOWN, "invalid recorded answer"),
        };
        Ok(completed(value))
    }
}
