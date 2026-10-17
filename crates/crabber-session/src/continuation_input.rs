//! Original continuation payloads anchored only at their actual acceptance birth.
#[path = "continuation_input/input.rs"]
mod input;
#[path = "continuation_input/ledger.rs"]
mod ledger;
#[path = "continuation_input/receipt.rs"]
mod receipt;
use crabber_core::{ControlId, EventCursor, EventRecord, InputFingerprint, RunId, SessionId};
pub(crate) use input::Input;
pub(crate) use ledger::Ledger;
pub(crate) use receipt::Receipt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub(crate) const MAX_SOURCE_BYTES: usize = 4096;
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Domain {
    Approval,
    Tool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    Suspension,
    Answer,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct Error;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scope {
    pub domain: Domain,
    pub session: SessionId,
    pub run: RunId,
    pub continuation: crate::ContinuationTarget,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub version: u32,
    pub scope: Scope,
    pub operation: ControlId,
    pub kind: Kind,
    pub subject: String,
    pub cursor: EventCursor,
    pub digest: InputFingerprint,
}
impl Source {
    pub(crate) fn new(
        input: Input<'_>,
        receipt: Receipt<'_>,
        event: &EventRecord,
    ) -> Result<Self, Error> {
        receipt.verify(event)?;
        let (scope, operation, kind, subject, semantic) = input.facts(receipt)?;
        if operation != *receipt.operation() || semantic != *receipt.semantic() {
            return Err(Error);
        }
        let mut source = Self {
            version: 1,
            scope,
            operation,
            kind,
            subject,
            cursor: receipt.cursor(),
            digest: InputFingerprint::new("0".repeat(64)).map_err(|_| Error)?,
        };
        source.validate()?;
        source.digest = digest(&(
            "crabber.continuation-input.birth.v1",
            &source.scope,
            &source.operation,
            source.kind,
            &source.subject,
            source.cursor,
            input,
            receipt,
            event,
        ))?;
        Ok(source)
    }
    pub(crate) fn validate(&self) -> Result<(), Error> {
        self.scope.validate()?;
        crate::attempt::validate_identifier(&self.operation.0).map_err(|_| Error)?;
        if self.version != 1
            || self.cursor.0 == 0
            || self.cursor.0 > i64::MAX as u64
            || (self.kind == Kind::Suspension && self.subject != "suspension")
            || (self.kind == Kind::Answer
                && crate::attempt::validate_identifier(&self.subject).is_err())
            || crate::snapshot::encoded_bytes(self, MAX_SOURCE_BYTES).is_none()
        {
            return Err(Error);
        }
        Ok(())
    }
}
impl Scope {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        crate::continuation_origin::validate_scope(&self.session, &self.run, &self.continuation)
            .map_err(|()| Error)
    }
}
fn digest(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    // Finite canonical temporary; arbitrary JSON object order is not provenance.
    if crate::snapshot::encoded_bytes(value, 5 << 20).is_none() {
        return Err(Error);
    }
    let mut value = serde_json::to_value(value).map_err(|_| Error)?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(|_| Error)?)
    ))
    .map_err(|_| Error)
}

impl Ord for Scope {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.domain,
            &self.session.0,
            &self.run.0,
            &self.continuation.id.0,
            self.continuation.generation.get(),
        )
            .cmp(&(
                other.domain,
                &other.session.0,
                &other.run.0,
                &other.continuation.id.0,
                other.continuation.generation.get(),
            ))
    }
}
impl PartialOrd for Scope {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
#[path = "continuation_input/tests.rs"]
pub(crate) mod tests;
