//! Explicit host interaction surface and finite prompt bounds.
use crabber::ExtensionError;
use crabber::core::AuthorityRequirement;
use serde::{Serialize, Serializer};
use std::time::Duration;
/// Maximum question size in bytes.
pub const MAX_QUESTION_BYTES: usize = 8192;
/// Maximum answer size in bytes.
pub const MAX_ANSWER_BYTES: usize = 1024 * 1024;
/// Maximum durable answer bytes.
pub const MAX_DURABLE_ANSWER_BYTES: usize = 8192;
/// Host-owned answer collection mode.
#[derive(Clone, Debug)]
pub enum Surface {
    /// Relay a question immediately; this does not enforce or establish consent.
    Pending,
    /// Suspend durably until an authenticated host resolution is recorded.
    Durable {
        /// Host policy identity required by Crabber for recorded resolutions.
        authority: AuthorityRequirement,
        /// Store-clock expiry; hosts must explicitly record Expire.
        expires_after: Option<Duration>,
    },
}
impl Serialize for Surface {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Pending => serializer.serialize_str("pending"),
            Self::Durable { authority, expires_after } => {
                serde_json::json!({"durable":{"policy_id":authority.policy_id(), "policy_version":authority.policy_version(), "expires_after_ms":expires_after.map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))}}).serialize(serializer)
            }
        }
    }
}
/// Host interaction policy; every bound participates in configuration identity.
#[derive(Clone, Debug, Serialize)]
pub struct UserInteractPolicy {
    /// Answer collection mode.
    pub surface: Surface,
    /// Maximum question bytes.
    pub max_question_bytes: usize,
    /// Maximum answer bytes.
    pub max_answer_bytes: usize,
}
impl UserInteractPolicy {
    /// Default relay policy.
    pub fn pending() -> Self {
        Self {
            surface: Surface::Pending,
            max_question_bytes: MAX_QUESTION_BYTES,
            max_answer_bytes: MAX_ANSWER_BYTES,
        }
    }
    /// Default durable policy with an 8 KiB answer limit and no expiry.
    pub fn durable(authority: AuthorityRequirement) -> Self {
        Self {
            surface: Surface::Durable {
                authority,
                expires_after: None,
            },
            max_question_bytes: MAX_QUESTION_BYTES,
            max_answer_bytes: MAX_DURABLE_ANSWER_BYTES,
        }
    }
    /// Validate finite question and answer bounds.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        if !(1..=MAX_QUESTION_BYTES).contains(&self.max_question_bytes)
            || !(1..=MAX_ANSWER_BYTES).contains(&self.max_answer_bytes)
        {
            return Err(ExtensionError::Plan("user_interact policy".into()));
        }
        if let Surface::Durable { expires_after, .. } = &self.surface
            && (self.max_answer_bytes > MAX_DURABLE_ANSWER_BYTES
                || expires_after
                    .is_some_and(|d| d.as_millis() == 0 || d.as_millis() > u128::from(u64::MAX)))
        {
            return Err(ExtensionError::Plan("user_interact policy".into()));
        }
        Ok(())
    }
}
