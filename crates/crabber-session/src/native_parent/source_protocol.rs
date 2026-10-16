//! Independent append-only membership metadata, never execution authority.
use super::{Error, RunIssuance};
use crabber_core::{InputFingerprint, RunId, SessionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const SOURCE_BYTES: usize = 2048;
pub(crate) const HEAD_BYTES: usize = 1536;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalSource {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub sequence: u64,
    pub issuance_digest: InputFingerprint,
    pub previous_digest: Option<InputFingerprint>,
    pub chain_digest: InputFingerprint,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct JournalHead {
    pub version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub sequence: u64,
    pub origin_digest: InputFingerprint,
    pub chain_digest: InputFingerprint,
}

impl JournalSource {
    pub fn new(previous: Option<&JournalHead>, issuer: &RunIssuance) -> Result<Self, Error> {
        // Count the actual borrowed carrier before canonical serialization.
        if crate::snapshot::encoded_bytes(issuer, 2048).is_none() {
            return Err(Error);
        }
        issuer.validate()?;
        let previous_digest = match previous {
            Some(head) => {
                head.validate()?;
                if head.session_id != issuer.state.session_id
                    || head.run_id != issuer.state.run_id
                    || issuer.sequence != head.sequence.checked_add(1).ok_or(Error)?
                {
                    return Err(Error);
                }
                Some(head.chain_digest.clone())
            }
            None if issuer.sequence == 1 => None,
            None => return Err(Error),
        };
        let issuance_digest = digest(&("crabber.native-run.source-issuer.v1", issuer))?;
        let chain_digest = chain(
            &issuer.state.session_id,
            &issuer.state.run_id,
            issuer.sequence,
            &issuance_digest,
            previous_digest.as_ref(),
        )?;
        Ok(Self {
            version: 1,
            session_id: issuer.state.session_id.clone(),
            run_id: issuer.state.run_id.clone(),
            sequence: issuer.sequence,
            issuance_digest,
            previous_digest,
            chain_digest,
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        scope(self.version, &self.session_id, &self.run_id, self.sequence)?;
        if crate::snapshot::encoded_bytes(self, SOURCE_BYTES).is_none()
            || (self.sequence == 1) != self.previous_digest.is_none()
            || self.chain_digest
                != chain(
                    &self.session_id,
                    &self.run_id,
                    self.sequence,
                    &self.issuance_digest,
                    self.previous_digest.as_ref(),
                )?
        {
            return Err(Error);
        }
        Ok(())
    }

    pub fn verify(&self, issuer: &RunIssuance) -> Result<(), Error> {
        self.validate()?;
        if crate::snapshot::encoded_bytes(issuer, 2048).is_none() {
            return Err(Error);
        }
        issuer.validate()?;
        if self.session_id != issuer.state.session_id
            || self.run_id != issuer.state.run_id
            || self.sequence != issuer.sequence
            || self.issuance_digest != digest(&("crabber.native-run.source-issuer.v1", issuer))?
        {
            return Err(Error);
        }
        Ok(())
    }
}

impl JournalHead {
    pub fn advance(previous: Option<&Self>, source: &JournalSource) -> Result<Self, Error> {
        source.validate()?;
        let origin_digest = match previous {
            Some(head) => {
                head.validate()?;
                if head.session_id != source.session_id
                    || head.run_id != source.run_id
                    || source.sequence != head.sequence.checked_add(1).ok_or(Error)?
                    || source.previous_digest.as_ref() != Some(&head.chain_digest)
                {
                    return Err(Error);
                }
                head.origin_digest.clone()
            }
            None if source.sequence == 1 => source.chain_digest.clone(),
            None => return Err(Error),
        };
        Ok(Self {
            version: 1,
            session_id: source.session_id.clone(),
            run_id: source.run_id.clone(),
            sequence: source.sequence,
            origin_digest,
            chain_digest: source.chain_digest.clone(),
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        scope(self.version, &self.session_id, &self.run_id, self.sequence)?;
        if crate::snapshot::encoded_bytes(self, HEAD_BYTES).is_none()
            || (self.sequence == 1 && self.origin_digest != self.chain_digest)
        {
            return Err(Error);
        }
        Ok(())
    }
}

/// A complete reader compares the final folded head with the independent head.
/// Counting present rows or selecting their maximum sequence is insufficient.
#[derive(Default)]
pub(crate) struct Stream(Option<JournalHead>);
impl Stream {
    pub fn observe(&mut self, source: &JournalSource, issuer: &RunIssuance) -> Result<(), Error> {
        source.verify(issuer)?;
        self.0 = Some(JournalHead::advance(self.0.as_ref(), source)?);
        Ok(())
    }
    pub fn finish(self, expected: &JournalHead) -> Result<(), Error> {
        expected.validate()?;
        if self.0.as_ref() != Some(expected) {
            return Err(Error);
        }
        Ok(())
    }
}

fn scope(version: u32, session: &SessionId, run: &RunId, sequence: u64) -> Result<(), Error> {
    if version != 1 || sequence == 0 || sequence > i64::MAX as u64 {
        return Err(Error);
    }
    validate_scope(session, run)
}
pub(crate) fn validate_scope(session: &SessionId, run: &RunId) -> Result<(), Error> {
    super::run::scope(session, run)
}
fn chain(
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    issuer: &InputFingerprint,
    previous: Option<&InputFingerprint>,
) -> Result<InputFingerprint, Error> {
    digest(&(
        "crabber.native-run.source-chain.v1",
        session,
        run,
        sequence,
        issuer,
        previous,
    ))
}
fn digest(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| Error)?)
    ))
    .map_err(|_| Error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maximal_scope_is_bounded_and_chain_requires_exact_origin_and_tail() {
        let state = super::super::RunState::new(
            &SessionId("\\".repeat(512)),
            &RunId("r".repeat(128)),
            crabber_core::RunStatus::Running,
            "actual-initial-fence",
        )
        .unwrap();
        let first = super::super::advance(None, state, None).unwrap().unwrap();
        let original = JournalSource::new(None, &first).unwrap();
        let initial = JournalHead::advance(None, &original).unwrap();
        assert!(crate::snapshot::encoded_bytes(&original, SOURCE_BYTES).is_some());
        assert!(crate::snapshot::encoded_bytes(&initial, HEAD_BYTES).is_some());
        let mut next = first.clone();
        next.sequence = 2;
        next.state.status = crabber_core::RunStatus::Completed;
        let tail = JournalSource::new(Some(&initial), &next).unwrap();
        let head = JournalHead::advance(Some(&initial), &tail).unwrap();
        let mut complete = Stream::default();
        complete.observe(&original, &first).unwrap();
        complete.observe(&tail, &next).unwrap();
        complete.finish(&head).unwrap();
        let mut truncated = Stream::default();
        truncated.observe(&original, &first).unwrap();
        assert!(truncated.finish(&head).is_err());
        assert!(Stream::default().observe(&tail, &next).is_err());
        let mut forged = first.clone();
        forged.state.token = InputFingerprint::new("e".repeat(64)).unwrap();
        assert!(original.verify(&forged).is_err());
    }
}
