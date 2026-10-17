//! Selected original completion inspection; no current worker permission.
use crate::session_owner::driver::{NativeClaimLocator, RegisterSuccessorExecutionDriver};
use crabber_core::{ControlId, InputFingerprint};

/// The locator must come from actual completion. All fields are independently
/// corroborated against protected original sources; this request grants nothing.
#[derive(Clone, Debug)]
pub struct InspectPreparedToolDriver {
    pub request: RegisterSuccessorExecutionDriver,
    pub origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    pub operation_id: ControlId,
    pub semantic_digest: InputFingerprint,
}
/// Original safe metadata only, independent of later heads/continuations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedToolDriverMetadata {
    pub claim: NativeClaimLocator,
    pub receipt: crate::ToolContinuationReceipt,
}

mod verify;
pub(crate) use verify::*;
