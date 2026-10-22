//! Host-owned durable questions and a pending relay.
//! Questions and answers cross the host boundary unredacted. Hosts should mount
//! a tool-result redactor; this tool must not be used to collect secrets.
#![warn(missing_docs)]
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::{category, check_cancelled, failed};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
mod durable;
mod info;
mod policy;
pub use info::info;
pub use policy::{
    MAX_ANSWER_BYTES, MAX_DURABLE_ANSWER_BYTES, MAX_QUESTION_BYTES, Surface, UserInteractPolicy,
};
/// Explicit host policy; interactions do not consume workspace capacity.
pub struct Options {
    /// Validated interaction surface and bounds.
    pub policy: UserInteractPolicy,
}
/// Validate policy and construct a relay or resumable durable tool.
///
/// # Errors
/// Returns a plan error if the interaction policy is invalid.
pub fn definition(options: Arc<Options>) -> Result<Arc<ToolDefinition>, ExtensionError> {
    options.policy.validate()?;
    match &options.policy.surface {
        Surface::Pending => Ok(Arc::new(ToolDefinition {
            info: info(),
            executor: Arc::new(UserTool { options }),
        })),
        Surface::Durable { .. } => Ok(Arc::new(ToolDefinition::resumable(
            info(),
            Arc::new(durable::DurableTool { options }),
        )?)),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    question: String,
    #[serde(default)]
    answer: String,
}
struct UserTool {
    options: Arc<Options>,
}
#[async_trait]
impl ToolExecutor for UserTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        value: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        self.invoke(&ctx, value)
            .map(|value| crabber_tools_core::to_output(value, &[]))
    }
}
impl UserTool {
    fn invoke(&self, ctx: &ToolContext, value: Value) -> Result<Value, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let args = match parse_args(value, &self.options.policy) {
            Ok(args) => args,
            Err(failure) => return Ok(failure),
        };
        let policy = &self.options.policy;
        if let Some(failure) = validate_answer(&args.answer, policy.max_answer_bytes) {
            return Ok(failure);
        }
        match &policy.surface {
            Surface::Pending if !args.answer.is_empty() => {
                Ok(json!({"outcome":"succeeded","answer":args.answer}))
            }
            Surface::Pending => Ok(json!({"outcome":"pending","question":args.question})),
            Surface::Durable { .. } => Ok(failed(
                category::UNKNOWN,
                "durable surface requires resumable registration",
            )),
        }
    }
}

pub(crate) fn validate_answer(answer: &str, max_bytes: usize) -> Option<Value> {
    if answer.contains('\0') {
        Some(failed(category::VALIDATION, "answer contains NUL"))
    } else if answer.len() > max_bytes {
        Some(failed(
            category::TOO_LARGE,
            format!("answer exceeds {max_bytes} bytes"),
        ))
    } else {
        None
    }
}

fn parse_args(value: Value, policy: &UserInteractPolicy) -> Result<Args, Value> {
    let args: Args = serde_json::from_value(value)
        .map_err(|_| failed(category::VALIDATION, "invalid arguments"))?;
    if args.question.trim().is_empty() {
        return Err(failed(category::VALIDATION, "question is required"));
    }
    if args
        .question
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
    {
        return Err(failed(
            category::VALIDATION,
            "question contains control characters",
        ));
    }
    if args.question.len() > policy.max_question_bytes {
        return Err(failed(
            category::TOO_LARGE,
            format!("question exceeds {} bytes", policy.max_question_bytes),
        ));
    }
    Ok(args)
}
