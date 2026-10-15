//! Compound effects share the native phase transaction and receipt namespace.
use crate::{PhaseError, PhaseMutation, PhaseReceipt, PhaseSettlement, PhaseStart};
use crabber_core::{InputFingerprint, RunId, SessionId, ToolCallRecord};

#[derive(Clone, Copy)]
pub(crate) enum PhaseEffect<'a> {
    Metadata,
    Start(&'a PhaseStart),
    Settle(&'a PhaseSettlement),
    Resolve(&'a crate::PhaseResolution),
}
impl<'a> PhaseEffect<'a> {
    pub(crate) fn resolution(self) -> Option<&'a crate::PhaseResolution> {
        if let Self::Resolve(value) = self {
            Some(value)
        } else {
            None
        }
    }
    pub(crate) fn start(self) -> Option<&'a PhaseStart> {
        if let Self::Start(value) = self {
            Some(value)
        } else {
            None
        }
    }
    pub(crate) fn settlement(self) -> Option<&'a PhaseSettlement> {
        if let Self::Settle(value) = self {
            Some(value)
        } else {
            None
        }
    }
    pub(crate) fn fingerprint(
        self,
        request: &PhaseMutation,
        session: &SessionId,
        run: &RunId,
    ) -> Result<InputFingerprint, PhaseError> {
        match self {
            Self::Metadata => request.fingerprint(session, run),
            Self::Start(value) => value.fingerprint(session, run),
            Self::Settle(value) => value.fingerprint(session, run),
            Self::Resolve(value) => {
                if value.session_id != *session || value.run_id != *run {
                    return Err(PhaseError::NotFound);
                }
                value.fingerprint()
            }
        }
    }
    pub(crate) fn verify_replay(
        self,
        receipt: &PhaseReceipt,
        call: &ToolCallRecord,
    ) -> Result<(), PhaseError> {
        match self {
            Self::Metadata => Ok(()),
            Self::Start(value) => value.verify_replay(receipt),
            Self::Settle(value) => value.verify_replay(receipt, call),
            Self::Resolve(value) => value.verify_replay(receipt, call),
        }
    }
}
