use super::{Failure, Proof, bounded, digest, unknown};
use crate::session_owner::preparation::{JoinedPreparation, PreparationCommitError};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionId};
use serde::{Deserialize, Serialize};

/// Routing commitment to original identities only; never source authentication.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceKey {
    pub session_id: SessionId,
    pub identity: InputFingerprint,
}
impl Ord for SourceKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (&self.session_id, self.identity.as_str())
            .cmp(&(&other.session_id, other.identity.as_str()))
    }
}
impl PartialOrd for SourceKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl SourceKey {
    /// The shared original identity recipe. All values are borrowed originals;
    /// this hash supplies routing metadata and never native authentication.
    pub(crate) fn from_original_identities(
        session: &SessionId,
        root_reserve: &crabber_core::SessionOwnerOperationId,
        root_begin: &crabber_core::SessionOwnerOperationId,
        root_target: &crate::session_owner::SessionOwnerTarget,
        registration: &crate::session_owner::owned_scope::OwnedScopeActivation,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        let identities = (
            session,
            root_reserve,
            root_begin,
            root_target,
            &registration.source.reserve,
            &registration.source.begin,
            &registration.source.target,
            &registration.id,
            &registration.nonce,
        );
        proof.measure(&identities)?;
        let identity = digest(&("crabber.pending-initial.source-key.v1", identities))?;
        proof.measure(session)?;
        Ok(Self {
            session_id: session.clone(),
            identity,
        })
    }
    pub(crate) fn expected(
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        use crate::session_owner::SessionOwnerError as Error;
        let grant = original.observed_grant();
        let registration = original.observed_registration().ok_or(Error::Unsupported)?;
        let reserve = grant.original_reservation();
        let target = grant.owner_authority().target();
        // Reconciliation derives root operation IDs. Charge its actual complete
        // input before its existing canonical fingerprint/hash/allocation work.
        let authority = grant.owner_authority();
        proof.measure(&(reserve, registration)).map_err(|error| {
            proof.admission_failure(&error, PreparationCommitError::Owner(Error::Capacity))
        })?;
        crate::native_enrollment::caller::measure_owner_authority(authority, proof).map_err(
            |error| proof.admission_failure(&error, PreparationCommitError::Owner(Error::Capacity)),
        )?;
        let root = reserve.reconciliation()?;
        Self::from_original_identities(
            &reserve.owner.binding.session_id,
            &root.reserve_id,
            &root.begin_id,
            target,
            registration,
            proof,
        )
        .map_err(|error| match error {
            Failure::Limited(limit) => proof.admission_failure(
                &Failure::Limited(limit),
                PreparationCommitError::Owner(Error::Capacity),
            ),
            Failure::Error(error) => proof.admission_failure(
                &Failure::Error(error),
                PreparationCommitError::Owner(Error::UnknownStoreOutcome),
            ),
        })
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub epoch: ControlId,
    pub complete: ControlId,
    pub seal: ControlId,
}
impl Identity {
    pub(crate) fn validate(&self) -> Result<(), Failure> {
        for id in [&self.epoch, &self.complete, &self.seal] {
            crate::session_owner::validation::identifier(&id.0, 128, false).map_err(unknown)?;
            uuid::Uuid::parse_str(&id.0).map_err(unknown)?;
        }
        if self.epoch == self.complete || self.epoch == self.seal || self.complete == self.seal {
            return Err(unknown("Pending Initial call identities"));
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Correlation {
    pub identity: Identity,
    pub key: SourceKey,
    pub prefix_digest: InputFingerprint,
    pub intent_digest: InputFingerprint,
}
impl Correlation {
    pub(crate) fn validate(&self, proof: &mut Proof) -> Result<(), Failure> {
        bounded(self, proof)?;
        self.identity.validate()?;
        crate::session_owner::validation::identifier(&self.key.session_id.0, 512, false)
            .map_err(unknown)
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EpochKind {
    Opened,
    SealedBeforeOpen,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Epoch {
    pub version: u32,
    pub correlation: Correlation,
    pub kind: EpochKind,
}
impl Epoch {
    pub(crate) fn digest(&self) -> Result<InputFingerprint, Failure> {
        digest(&("crabber.pending-initial.epoch.v1", self))
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSlot {
    pub version: u32,
    pub key: SourceKey,
    pub epoch: ControlId,
    pub epoch_digest: InputFingerprint,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredComplete {
    pub version: u32,
    pub key: SourceKey,
    pub epoch: ControlId,
    pub call: ControlId,
    pub epoch_digest: InputFingerprint,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Assignment {
    pub version: u32,
    pub correlation: Correlation,
    pub call: ControlId,
    pub run_id: RunId,
    pub epoch_digest: InputFingerprint,
    pub first_issuer_digest: InputFingerprint,
    pub first_source_digest: InputFingerprint,
    pub result_digest: InputFingerprint,
    pub keyed_receipt_digest: Option<InputFingerprint>,
}
impl Assignment {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.correlation
    }
    pub(crate) fn digest(&self) -> Result<InputFingerprint, Failure> {
        digest(&("crabber.pending-initial.assignment.v1", self))
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CloseOutcome {
    NoPublication,
    Assigned { assignment_digest: InputFingerprint },
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloseRecord {
    pub version: u32,
    pub correlation: Correlation,
    pub epoch_digest: InputFingerprint,
    pub outcome: CloseOutcome,
}
impl CloseRecord {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.correlation
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicationKind {
    Opened,
    Assigned,
    Closed,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    pub version: u32,
    pub key: SourceKey,
    pub epoch: ControlId,
    pub sequence: u64,
    pub kind: PublicationKind,
    pub record_digest: InputFingerprint,
    pub previous_digest: Option<InputFingerprint>,
    pub digest: InputFingerprint,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    pub version: u32,
    pub key: SourceKey,
    pub epoch: ControlId,
    pub sequence: u64,
    pub origin_digest: InputFingerprint,
    pub digest: InputFingerprint,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OpenRecord {
    pub epoch: Epoch,
    pub slot: SourceSlot,
    pub registered: RegisteredComplete,
}
impl OpenRecord {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.epoch.correlation
    }
}
/// Safe authenticated Raw completion; it never creates a Pending assignment.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActualRawMetadata {
    pub correlation: Correlation,
    pub run_id: RunId,
    pub first_issuer_digest: InputFingerprint,
    pub first_source_digest: InputFingerprint,
    pub receipt_digest: InputFingerprint,
}
impl ActualRawMetadata {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.correlation
    }
}
/// Original native result, kept private by the actual task and its supervisor.
pub(crate) struct EnrolledInitialData {
    pub completion: crate::session_owner::preparation::PreparationCompletion,
    pub first: crate::native_enrollment::caller::initial::FirstPublication,
    pub assignment: Assignment,
}
impl EnrolledInitialData {
    pub(crate) fn correlation(&self) -> &Correlation {
        self.assignment.correlation()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawInitialReceipt<'a> {
    Unkeyed(&'a crate::session_owner::preparation::PreparationAdmissionReceipt),
    Keyed(&'a crabber_core::AdmissionReceipt),
}
impl ActualRawMetadata {
    /// Backend separately authenticates exact historical Raw Join/Witness/Init.
    pub(crate) fn from_original(
        correlation: &Correlation,
        first: &crate::native_enrollment::caller::initial::FirstPublication,
        receipt: RawInitialReceipt<'_>,
        proof: &mut Proof,
    ) -> Result<Self, Failure> {
        proof.measure(&(correlation, &first.issuer, &first.source))?;
        first.source.verify(&first.issuer).map_err(unknown)?;
        if first.issuer.sequence != 1
            || first.source.sequence != 1
            || first.issuer.state.session_id != correlation.key.session_id
        {
            return Err(unknown("original Raw Initial source"));
        }
        let (session, run, receipt_digest) = match receipt {
            RawInitialReceipt::Unkeyed(value) => {
                let data = (&value.session_id, &value.run_id, &value.operation_id);
                proof.measure(&data)?;
                (
                    &value.session_id,
                    &value.run_id,
                    digest(&("crabber.pending-initial.raw-receipt.v1", "unkeyed", data))?,
                )
            }
            RawInitialReceipt::Keyed(value) => {
                proof.measure(value)?;
                (
                    &value.session_id,
                    &value.run_id,
                    digest(&("crabber.pending-initial.raw-receipt.v1", "keyed", value))?,
                )
            }
        };
        if session != &first.issuer.state.session_id || run != &first.issuer.state.run_id {
            return Err(unknown("Raw Initial receipt routing"));
        }
        proof.measure(&(
            correlation,
            run,
            &first.source.issuance_digest,
            &first.source.chain_digest,
        ))?;
        Ok(Self {
            correlation: correlation.clone(),
            run_id: run.clone(),
            first_issuer_digest: first.source.issuance_digest.clone(),
            first_source_digest: first.source.chain_digest.clone(),
            receipt_digest,
        })
    }
}
