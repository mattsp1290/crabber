//! Closed object-safe transport; native authentication still happens under its gate.
use super::{LocalLease, RetainedInitialSource};
use crate::{
    native_enrollment::{
        caller::initial::SelectedOriginal,
        pending_initial::{
            ActualRawMetadata, Assignment, CloseRecord, Correlation, EnrolledInitialData,
            OpenRecord,
        },
    },
    public_session::v2::proof::Proof,
    session_owner::{
        SessionOwnerError as Error,
        preparation::{JoinedPreparation, PreparationCommitError},
    },
};
use crabber_core::SessionContext;

/// One original private call. Adapters may forward it but cannot construct,
/// duplicate, inspect or extract an execution capability from it.
pub struct PendingInitialNativeCall<'call> {
    pub(super) source: &'call RetainedInitialSource,
    pub(super) expected: &'call Correlation,
    pub(super) operation: NativeInitialOperation,
    pub(super) opened: Option<&'call OpenRecord>,
    pub(super) selected: &'call SelectedOriginal<'call>,
    pub(super) proof: &'call mut Proof,
    pub(super) lease: &'call LocalLease,
    pub(super) consumer: Option<&'call super::InitialResourceAssociation<'call>>,
    pub(super) retirement: Option<&'call super::InitialRetirementWitness<'call>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeInitialOperation {
    Open,
    Complete,
    Seal,
}

/// Only a consumed genuine call can construct these borrowed native inputs.
/// Its lease remains alive through native I/O and actual result disposal.
pub(crate) struct NativeInitialParts<'a> {
    pub(crate) operation: NativeInitialOperation,
    pub(crate) original: &'a JoinedPreparation,
    pub(crate) context: &'a SessionContext,
    pub(crate) expected: &'a Correlation,
    pub(crate) opened: Option<&'a OpenRecord>,
    pub(crate) selected: &'a SelectedOriginal<'a>,
    pub(crate) proof: &'a mut Proof,
    pub(crate) consumer: Option<&'a super::InitialResourceAssociation<'a>>,
    pub(crate) retirement: Option<&'a super::InitialRetirementWitness<'a>>,
    _lease: &'a LocalLease,
}
impl<'a> PendingInitialNativeCall<'a> {
    #[cfg(test)]
    pub(crate) fn is_seal(&self) -> bool {
        self.operation == NativeInitialOperation::Seal
    }

    /// Local issuer admission only; native selected-source/current verification
    /// is mandatory afterwards and is not replaced by a Tokio task identity.
    pub(crate) fn into_native(self) -> Result<NativeInitialParts<'a>, PreparationCommitError> {
        self.lease
            .verify(self.operation, self.source, self.expected)?;
        let needs_retirement =
            self.operation == NativeInitialOperation::Seal && self.consumer.is_some();
        if self.retirement.is_some() != needs_retirement {
            return Err(Error::InvalidTransition.into());
        }
        Ok(NativeInitialParts {
            operation: self.operation,
            original: &self.source.original,
            context: &self.source.context,
            expected: self.expected,
            opened: self.opened,
            selected: self.selected,
            proof: self.proof,
            consumer: self.consumer,
            retirement: self.retirement,
            _lease: self.lease,
        })
    }
}

/// Actual native output with no public capability accessors or reconstruction.
pub struct PendingInitialNativeReply {
    kind: Reply,
}
pub(super) enum Reply {
    Opened(OpenRecord),
    AlreadyOpened(Correlation),
    Completed(Box<EnrolledInitialData>),
    AlreadyPublished(Assignment),
    Closed(CloseRecord),
    RawCompleted(ActualRawMetadata),
}
impl PendingInitialNativeReply {
    pub(crate) fn already_opened(
        correlation: Correlation,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        proof.measure(&correlation).map_err(|_| Error::Capacity)?;
        Ok(Self {
            kind: Reply::AlreadyOpened(correlation),
        })
    }
    pub(crate) fn opened(
        record: OpenRecord,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        proof.measure(&record).map_err(|_| Error::Capacity)?;
        Ok(Self {
            kind: Reply::Opened(record),
        })
    }
    // Actual publisher/result verification accounts for full native capabilities
    // before this ownership transfer. No metadata lookup calls this constructor.
    pub(crate) fn completed(data: EnrolledInitialData) -> Self {
        Self {
            kind: Reply::Completed(Box::new(data)),
        }
    }
    pub(crate) fn already_published(
        record: Assignment,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        proof.measure(&record).map_err(|_| Error::Capacity)?;
        Ok(Self {
            kind: Reply::AlreadyPublished(record),
        })
    }
    pub(crate) fn closed(
        record: CloseRecord,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        proof.measure(&record).map_err(|_| Error::Capacity)?;
        Ok(Self {
            kind: Reply::Closed(record),
        })
    }
    pub(crate) fn raw_completed(
        record: ActualRawMetadata,
        proof: &mut Proof,
    ) -> Result<Self, PreparationCommitError> {
        proof.measure(&record).map_err(|_| Error::Capacity)?;
        Ok(Self {
            kind: Reply::RawCompleted(record),
        })
    }
    pub(super) fn correlated(
        self,
        expected: &Correlation,
        selected: &SelectedOriginal<'_>,
        proof: &mut Proof,
    ) -> Result<Reply, PreparationCommitError> {
        // A forwarding adapter can return genuine output charged by an older
        // operation's Proof. Account for the actual returned ACK on THIS Proof
        // after the literal future releases its mutable borrow, before even
        // accepting or refusing its correlation. No receipt echo is permission.
        match &self.kind {
            Reply::Opened(value) => proof.measure(value).map_err(|_| Error::Capacity)?,
            Reply::AlreadyOpened(value) => proof.measure(value).map_err(|_| Error::Capacity)?,
            Reply::Completed(value) => value.verify_returned(selected, proof)?,
            Reply::AlreadyPublished(value) => proof.measure(value).map_err(|_| Error::Capacity)?,
            Reply::Closed(value) => proof.measure(value).map_err(|_| Error::Capacity)?,
            Reply::RawCompleted(value) => proof.measure(value).map_err(|_| Error::Capacity)?,
        }
        let actual = match &self.kind {
            Reply::Opened(value) => value.correlation(),
            Reply::AlreadyOpened(value) => value,
            Reply::Completed(value) => value.correlation(),
            Reply::AlreadyPublished(value) => value.correlation(),
            Reply::Closed(value) => value.correlation(),
            Reply::RawCompleted(value) => value.correlation(),
        };
        if actual != expected {
            return Err(Error::SemanticConflict.into());
        }
        Ok(self.kind)
    }
}
