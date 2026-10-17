//! Protected negative enrollment facts. No value in this module proves a main
//! join, an owned grant holder, or resource ownership.
use crate::native_parent::{RunIssuance, source_protocol::JournalSource};
use crate::public_session::v2::proof::{Proof, ReadError};
use crabber_core::{InputFingerprint, RunId, SessionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const SOURCE_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    Legacy,
    Unclassified,
}

#[derive(Debug)]
pub(crate) struct Error;
pub(crate) type Result<T> = std::result::Result<T, ReadError<Error>>;

/// This first protocol has only raw births. A later driver cannot adopt one.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Birth {
    Raw,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Origin {
    version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub first_issuer_digest: InputFingerprint,
    birth: Birth,
}

/// Every birth supported here already issues its first raw grant. Later raw
/// calls preserve that original poison. Pending births and other first-raw
/// families require the separate complete writer/enrolled-call protocol.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RawFamily {
    Initial,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RawAdmission {
    family: RawFamily,
    source_digest: InputFingerprint,
    targets_digest: InputFingerprint,
}

/// The first raw admission is permanent. A fresh later raw call cannot replace
/// its original source; metadata replay never inserts one.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Unclassified {
    version: u32,
    pub session_id: SessionId,
    pub run_id: RunId,
    pub origin_digest: InputFingerprint,
    first_raw: RawAdmission,
}

impl Origin {
    pub(crate) fn raw_birth(
        issuer: &RunIssuance,
        source: &JournalSource,
        proof: &mut Proof,
    ) -> Result<(Self, Unclassified)> {
        proof.measure(issuer).map_err(ReadError::Budget)?;
        proof.measure(source).map_err(ReadError::Budget)?;
        source.verify(issuer).map_err(|_| Error)?;
        if issuer.sequence != 1 {
            return Err(Error.into());
        }
        let origin = Self {
            version: 1,
            session_id: issuer.state.session_id.clone(),
            run_id: issuer.state.run_id.clone(),
            first_issuer_digest: source.issuance_digest.clone(),
            birth: Birth::Raw,
        };
        let first_raw = RawAdmission::new(RawFamily::Initial, issuer, &issuer.state, proof)?;
        let unclassified = Unclassified::new(&origin, first_raw, proof)?;
        Ok((origin, unclassified))
    }

    pub(crate) fn digest(&self, proof: &mut Proof) -> Result<InputFingerprint> {
        proof.measure(self).map_err(ReadError::Budget)?;
        self.validate()?;
        digest(&("crabber.native-execution.origin.v1", self)).map_err(Into::into)
    }

    fn validate(&self) -> std::result::Result<(), Error> {
        if self.version != 1 || crate::snapshot::encoded_bytes(self, SOURCE_BYTES).is_none() {
            return Err(Error);
        }
        crate::native_parent::source_protocol::validate_scope(&self.session_id, &self.run_id)
            .map_err(|_| Error)
    }

    /// Exact original metadata only. The caller separately authenticates the
    /// immutable Run cohort and SQL routing fields before decoding these bytes.
    pub(crate) fn verify_original(
        &self,
        unclassified: &Unclassified,
        first: &JournalSource,
        issuer: &RunIssuance,
        proof: &mut Proof,
    ) -> Result<()> {
        proof.measure(first).map_err(ReadError::Budget)?;
        proof.measure(issuer).map_err(ReadError::Budget)?;
        first.verify(issuer).map_err(|_| Error)?;
        if first.sequence != 1
            || self.session_id != first.session_id
            || self.run_id != first.run_id
            || self.first_issuer_digest != first.issuance_digest
        {
            return Err(Error.into());
        }
        unclassified.verify(self, proof)?;
        // Every birth supported by this protocol is already raw. Its first
        // poison must name that actual original grant, never a later caller's
        // replacement operation or a guessed first source.
        let initial = RawAdmission::new(RawFamily::Initial, issuer, &issuer.state, proof)?;
        if unclassified.first_raw != initial {
            return Err(Error.into());
        }
        Ok(())
    }
}

impl RawAdmission {
    fn new(
        family: RawFamily,
        source: &impl Serialize,
        targets: &impl Serialize,
        proof: &mut Proof,
    ) -> Result<Self> {
        proof.measure(source).map_err(ReadError::Budget)?;
        proof.measure(targets).map_err(ReadError::Budget)?;
        Ok(Self {
            family,
            source_digest: digest(&("crabber.native-execution.raw-source.v1", family, source))?,
            targets_digest: digest(&("crabber.native-execution.raw-targets.v1", family, targets))?,
        })
    }
}

impl Unclassified {
    pub(crate) fn new(origin: &Origin, first_raw: RawAdmission, proof: &mut Proof) -> Result<Self> {
        let origin_digest = origin.digest(proof)?;
        Ok(Self {
            version: 1,
            session_id: origin.session_id.clone(),
            run_id: origin.run_id.clone(),
            origin_digest,
            first_raw,
        })
    }

    pub(crate) fn verify(&self, origin: &Origin, proof: &mut Proof) -> Result<()> {
        proof.measure(self).map_err(ReadError::Budget)?;
        if self.version != 1
            || crate::snapshot::encoded_bytes(self, SOURCE_BYTES).is_none()
            || self.session_id != origin.session_id
            || self.run_id != origin.run_id
            || self.origin_digest != origin.digest(proof)?
        {
            return Err(Error.into());
        }
        Ok(())
    }
}

fn digest(value: &impl Serialize) -> std::result::Result<InputFingerprint, Error> {
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| Error)?)
    ))
    .map_err(|_| Error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native_parent::{RunState, advance};
    use crabber_core::RunStatus;

    fn first() -> (RunIssuance, JournalSource) {
        let state = RunState::new(
            &SessionId("\\".repeat(512)),
            &RunId("r".repeat(128)),
            RunStatus::Running,
            "original-native-fence",
        )
        .unwrap();
        let issuer = advance(None, state, None).unwrap().unwrap();
        let source = JournalSource::new(None, &issuer).unwrap();
        (issuer, source)
    }

    #[test]
    fn original_raw_grant_is_permanent_and_cannot_be_rebound() {
        let (issuer, source) = first();
        let mut proof = Proof::bounded(32 << 20);
        let (origin, poison) = Origin::raw_birth(&issuer, &source, &mut proof).unwrap();
        assert!(crate::snapshot::encoded_bytes(&origin, SOURCE_BYTES).is_some());
        assert!(crate::snapshot::encoded_bytes(&poison, SOURCE_BYTES).is_some());
        origin
            .verify_original(&poison, &source, &issuer, &mut proof)
            .unwrap();

        let mut replaced = poison.clone();
        replaced.first_raw.source_digest = InputFingerprint::new("f".repeat(64)).unwrap();
        assert!(
            origin
                .verify_original(&replaced, &source, &issuer, &mut proof)
                .is_err()
        );
        let mut unsupported = serde_json::to_value(&poison).unwrap();
        unsupported["first_raw"]["family"] = serde_json::json!("attempt_begin");
        assert!(serde_json::from_value::<Unclassified>(unsupported).is_err());
        let mut other = issuer.clone();
        other.state.run_id = RunId::new();
        let other_source = JournalSource::new(None, &other).unwrap();
        assert!(
            origin
                .verify_original(&poison, &other_source, &other, &mut proof)
                .is_err()
        );
        let mut later = issuer.clone();
        later.sequence = 2;
        assert!(Origin::raw_birth(&later, &source, &mut proof).is_err());
    }

    #[test]
    fn original_verification_uses_the_callers_remaining_proof() {
        let (issuer, source) = first();
        let (origin, poison) =
            Origin::raw_birth(&issuer, &source, &mut Proof::bounded(32 << 20)).unwrap();
        let mut measured = Proof::bounded(32 << 20);
        origin
            .verify_original(&poison, &source, &issuer, &mut measured)
            .unwrap();
        let mut limited = Proof::bounded(measured.used() + 7);
        limited.charge(8).unwrap();
        assert!(matches!(
            origin.verify_original(&poison, &source, &issuer, &mut limited),
            Err(ReadError::Budget(_))
        ));
    }
}
