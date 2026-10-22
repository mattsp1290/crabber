#![allow(dead_code)]
use crabber::core::{AdmissionKey, AdmissionOptions, InputFingerprint};
use crabber::{Admission, Agent, AgentBuilder, ExecutionContext, RunHandle, RuntimeError};
use sha2::{Digest, Sha256};

pub fn builder() -> AgentBuilder {
    Agent::builder().behavior_version("crabber-extensions-tests.v1")
}

pub async fn first_prompt(
    agent: &Agent,
    execution: &ExecutionContext,
    text: &str,
) -> Result<RunHandle, RuntimeError> {
    let admission = agent
        .prompt_keyed_in(
            execution,
            text,
            AdmissionOptions {
                key: AdmissionKey::new("first-prompt").unwrap(),
                fingerprint: InputFingerprint::new(format!(
                    "{:x}",
                    Sha256::digest(text.as_bytes())
                ))
                .unwrap(),
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            },
            None,
        )
        .await?;
    match admission {
        Admission::Started { handle, .. } => Ok(handle),
        Admission::Replayed(_) => panic!("first prompt must start a new session"),
    }
}

pub async fn next_prompt(
    agent: &Agent,
    execution: &ExecutionContext,
    text: &str,
) -> Result<RunHandle, RuntimeError> {
    agent.prompt_in(execution, text, None).await
}
