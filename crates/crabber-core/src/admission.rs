//! Safe, bounded host admission identifiers and immutable reconciliation metadata.
use crate::{CoreError, MessageId, RunId, SessionId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Opaque host key, unique within a retained session. Never put secrets in keys.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AdmissionKey(String);

impl AdmissionKey {
    /// Accepts 1–128 ASCII alphanumeric, dash, underscore, or dot characters.
    /// # Errors
    /// Returns a redacted validation error for invalid keys.
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        Self::try_from(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AdmissionKey {
    type Error = CoreError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(CoreError::Validation("invalid admission key".into()));
        }
        Ok(Self(value))
    }
}
impl From<AdmissionKey> for String {
    fn from(value: AdmissionKey) -> Self {
        value.0
    }
}
impl fmt::Debug for AdmissionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdmissionKey([redacted])")
    }
}

/// SHA-256 hex metadata; construction and deserialization both validate its bound.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InputFingerprint(String);
impl InputFingerprint {
    /// Accepts exactly 64 lowercase hexadecimal characters.
    /// # Errors
    /// Returns a redacted validation error for invalid fingerprints.
    pub fn new(value: impl Into<String>) -> Result<Self, CoreError> {
        Self::try_from(value.into())
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for InputFingerprint {
    type Error = CoreError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(CoreError::Validation("invalid input fingerprint".into()));
        }
        Ok(Self(value))
    }
}
impl From<InputFingerprint> for String {
    fn from(value: InputFingerprint) -> Self {
        value.0
    }
}
impl fmt::Debug for InputFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InputFingerprint([redacted])")
    }
}

/// Host identity for one admission. `behavior_fingerprint` must version opaque
/// behavior/configuration (native callbacks, policies, provider routing) whose
/// implementation cannot be inspected by the runtime. It is not the claimed
/// input fingerprint; changing any reflected input is independently detected.
#[derive(Debug, Clone)]
pub struct AdmissionOptions {
    pub key: AdmissionKey,
    pub fingerprint: InputFingerprint,
    pub behavior_fingerprint: InputFingerprint,
}

/// Immutable safe metadata. The run ID also identifies the admission receipt.
/// Contains no execution authority, raw request, mutable status, or session paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionReceipt {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub user_message_id: MessageId,
    pub fingerprint: InputFingerprint,
    pub semantic_digest_version: u32,
    pub semantic_digest: InputFingerprint,
}
