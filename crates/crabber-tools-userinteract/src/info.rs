//! Deterministic model-facing metadata.
use crabber::core::ToolInfo;
use crabber_tools_core::permission;
/// Tool metadata; the durable surface rejects model-supplied answers.
#[must_use]
pub fn info() -> ToolInfo {
    ToolInfo {
        name: "user_interact".into(),
        description: "Ask the user a question and return their answer as a string. Depending on host configuration, either pauses until the host records the user's answer or returns immediately with a pending result — provide the user's answer in a follow-up call by populating the answer field. Does not format questions, validate answers, or offer multiple-choice options.".into(),
        parameters: serde_json::json!({"type":"object","additionalProperties":false,"required":["question"],"properties":{
            "question":{"type":"string","minLength":1,"description":"The question to present to the user."},
            "answer":{"type":"string","description":"Reserved for relaying an answer the host already collected for a pending question; when non-empty, the tool returns it immediately. Rejected on the durable surface. Leave empty when asking."}
        }}),
        retry_safe: false,
        required_permissions: vec![permission::INTERACTION_ASK.into()],
    }
}
