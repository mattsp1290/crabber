//! A new library scope is an actual creation source, never an empty roster.
use crate::session_owner::{SessionOwnerError as Error, audit::Audit};
use crabber_core::{
    ControlId, InputFingerprint, SessionId, SessionOwnerId, SessionOwnerOperationId,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Origin {
    pub session: SessionId,
    pub workspace: String,
    pub directory: String,
    pub source: Source,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Source {
    Managed {
        operation: ControlId,
        digest: InputFingerprint,
        receipt_digest: InputFingerprint,
        audit_digest: InputFingerprint,
    },
    Fork {
        operation: ControlId,
        digest: InputFingerprint,
        receipt_digest: InputFingerprint,
        audit_digest: InputFingerprint,
    },
    Absent {
        operation: SessionOwnerOperationId,
        owner: SessionOwnerId,
        digest: InputFingerprint,
    },
}
impl Origin {
    pub fn absent(audit: &Audit) -> Result<Self, Error> {
        let binding = &audit.record.snapshot.binding;
        Ok(Self {
            session: binding.session_id.clone(),
            workspace: binding.workspace_id.clone(),
            directory: binding.directory.clone(),
            source: Source::Absent {
                operation: audit.receipt.operation_id.clone(),
                owner: binding.id.clone(),
                digest: super::digest(audit)?,
            },
        })
    }
    pub fn created(
        session: &crabber_core::Session,
        origin: &crate::deletion::origin::OwnershipOrigin,
        receipt: &str,
        audit: &str,
    ) -> Result<Self, Error> {
        use crate::deletion::origin::OriginKind;
        let operation = origin.operation_id.clone();
        let digest = origin.semantic_digest.clone();
        let receipt_digest = body_digest(receipt)?;
        let audit_digest = body_digest(audit)?;
        let source = match origin.kind {
            OriginKind::ManagedCreate => Source::Managed {
                operation,
                digest,
                receipt_digest,
                audit_digest,
            },
            OriginKind::Fork => Source::Fork {
                operation,
                digest,
                receipt_digest,
                audit_digest,
            },
            OriginKind::PreparedAdmission => return Err(Error::Unsupported),
        };
        Ok(Self {
            session: session.id.clone(),
            workspace: session.workspace_id.clone(),
            directory: session.directory.clone(),
            source,
        })
    }
    pub fn encoded(&self) -> Result<String, Error> {
        if crate::snapshot::encoded_bytes(self, 32768).is_none() {
            return Err(Error::Capacity);
        }
        serde_json::to_string(self).map_err(|_| Error::UnknownStoreOutcome)
    }
    pub fn verify_absent(&self, audit: &Audit) -> Result<(), Error> {
        let Source::Absent {
            operation,
            owner,
            digest,
        } = &self.source
        else {
            return Err(Error::UnknownStoreOutcome);
        };
        let binding = &audit.record.snapshot.binding;
        if audit.version != 3
            || audit.record.sequence != 1
            || audit.receipt.operation != crate::session_owner::SessionOwnerOperation::Reserve
            || audit.receipt.operation_id != *operation
            || binding.id != *owner
            || binding.session_id != self.session
            || binding.workspace_id != self.workspace
            || binding.directory != self.directory
            || super::digest(audit)? != *digest
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
    pub fn verify_created(
        &self,
        kind: &str,
        digest: &str,
        event: Option<i64>,
        receipt: &str,
        audit: &str,
    ) -> Result<(), Error> {
        if event.is_some() {
            return Err(Error::UnknownStoreOutcome);
        }
        let (operation, expected, summary) = match &self.source {
            Source::Managed {
                operation,
                digest,
                receipt_digest,
                audit_digest,
            } if kind == "session_management" => {
                if body_digest(receipt)? != *receipt_digest || body_digest(audit)? != *audit_digest
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                let receipt: crate::ManagementReceipt =
                    serde_json::from_str(receipt).map_err(|_| Error::UnknownStoreOutcome)?;
                let audit: crate::management::Audit =
                    serde_json::from_str(audit).map_err(|_| Error::UnknownStoreOutcome)?;
                crate::management::verify_audit(&audit, &receipt)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
                if !matches!(audit.request.change, crate::SessionChange::Create { .. })
                    || receipt.operation_id != *operation
                    || receipt.semantic_digest != *digest
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                (operation, digest, receipt.summary)
            }
            Source::Fork {
                operation,
                digest,
                receipt_digest,
                audit_digest,
            } if kind == "session_fork" => {
                if body_digest(receipt)? != *receipt_digest || body_digest(audit)? != *audit_digest
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                let receipt: crate::ForkReceipt =
                    serde_json::from_str(receipt).map_err(|_| Error::UnknownStoreOutcome)?;
                let audit: crate::fork::Audit =
                    serde_json::from_str(audit).map_err(|_| Error::UnknownStoreOutcome)?;
                audit.validate().map_err(|_| Error::UnknownStoreOutcome)?;
                if audit.receipt != receipt
                    || receipt.operation_id != *operation
                    || receipt.semantic_digest != *digest
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                (operation, digest, receipt.target)
            }
            _ => return Err(Error::UnknownStoreOutcome),
        };
        if operation.0.is_empty()
            || digest != expected.as_str()
            || summary.session_id != self.session
            || summary.workspace_id != self.workspace
            || summary.directory != self.directory
            || summary.revision.get() != 1
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

fn body_digest(raw: &str) -> Result<InputFingerprint, Error> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| Error::UnknownStoreOutcome)?;
    let body = crate::session_owner::digest(&value)?;
    crate::session_owner::digest(&("crabber.owner-scope-creation-body.v1", body))
}
