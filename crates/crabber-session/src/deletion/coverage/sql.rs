//! Shared decoding after SQL has bounded every carrier before transfer.
use super::{CoverageRead, MAX_PROOF_BYTES};
use crate::DeletionError as Error;
use crate::deletion::origin::{OriginKind, OwnershipOrigin};
use crabber_core::{Session, SessionId, SessionRevision};

pub(crate) struct OriginEvidence {
    origin: OwnershipOrigin,
    session: Session,
    revision: SessionRevision,
    bytes: usize,
}

impl OriginEvidence {
    pub(crate) fn decode(
        id: &SessionId,
        session: &str,
        origin: &str,
        revision: i64,
        protocol: i64,
    ) -> Result<Self, Error> {
        let bytes = session.len() + origin.len();
        let origin: OwnershipOrigin =
            serde_json::from_str(origin).map_err(|_| Error::UnknownStoreOutcome)?;
        let session: Session =
            serde_json::from_str(session).map_err(|_| Error::UnknownStoreOutcome)?;
        let revision =
            SessionRevision::new(u64::try_from(revision).map_err(|_| Error::UnknownStoreOutcome)?)
                .map_err(|_| Error::UnknownStoreOutcome)?;
        if protocol != 1 || session.id != *id {
            return Err(Error::UnknownStoreOutcome);
        }
        origin.validate(id, revision)?;
        Ok(Self {
            origin,
            session,
            revision,
            bytes,
        })
    }
    pub(crate) fn kind(&self) -> OriginKind {
        self.origin.kind
    }
    pub(crate) fn operation(&self) -> &str {
        &self.origin.operation_id.0
    }
    pub(crate) fn prepared(
        self,
        witness: &crate::session_owner::preparation::admission::Witness,
        initial: &crate::native_parent::RunIssuance,
        audits: &[crate::session_owner::audit::Audit; 5],
        bytes: usize,
    ) -> Result<CoverageRead, Error> {
        let bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(Error::UnknownStoreOutcome)?;
        if bytes > MAX_PROOF_BYTES {
            return Ok(CoverageRead::Limited);
        }
        super::prepared(
            &self.origin,
            &self.session,
            self.revision,
            witness,
            initial,
            &crate::session_owner::preparation::admission::SourceAudits::from_array(audits),
            bytes,
        )
        .map(CoverageRead::Verified)
    }
    pub(crate) fn verify(
        self,
        kind: &str,
        digest: &str,
        event_seq: Option<i64>,
        receipt: &str,
        audit: &str,
    ) -> Result<CoverageRead, Error> {
        let bytes = self.bytes + kind.len() + digest.len() + receipt.len() + audit.len();
        if bytes > MAX_PROOF_BYTES {
            return Ok(CoverageRead::Limited);
        }
        if event_seq.is_some() || digest != self.origin.semantic_digest.as_str() {
            return Err(Error::UnknownStoreOutcome);
        }
        let proof = match self.origin.kind {
            OriginKind::ManagedCreate if kind == "session_management" => super::managed(
                &self.origin,
                &self.session,
                self.revision,
                &serde_json::from_str(audit).map_err(|_| Error::UnknownStoreOutcome)?,
                &serde_json::from_str(receipt).map_err(|_| Error::UnknownStoreOutcome)?,
                bytes,
            )?,
            OriginKind::Fork if kind == "session_fork" => super::fork(
                &self.origin,
                &self.session,
                self.revision,
                &serde_json::from_str(audit).map_err(|_| Error::UnknownStoreOutcome)?,
                &serde_json::from_str(receipt).map_err(|_| Error::UnknownStoreOutcome)?,
                bytes,
            )?,
            _ => return Err(Error::UnknownStoreOutcome),
        };
        Ok(CoverageRead::Verified(proof))
    }
}
