//! Compound effects share the native phase transaction and receipt namespace.
use crate::{PhaseError, PhaseMutation, PhaseReceipt, PhaseSettlement, PhaseStart};
use crabber_core::{InputFingerprint, RunId, SessionId, ToolCallRecord};

#[derive(Clone, Copy)]
pub(crate) enum PhaseEffect<'a> {
    Metadata,
    Uncertainty(&'a crate::PhaseUncertainty),
    Cleanup(&'a crate::PhaseCleanup),
    Start(&'a PhaseStart),
    Settle(&'a PhaseSettlement),
    Resolve(&'a crate::PhaseResolution),
}
impl<'a> PhaseEffect<'a> {
    pub(crate) fn is_host(self) -> bool {
        matches!(
            self,
            Self::Resolve(_) | Self::Uncertainty(_) | Self::Cleanup(_)
        )
    }
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
            Self::Uncertainty(value) => {
                if value.session_id != *session || value.run_id != *run {
                    return Err(PhaseError::NotFound);
                }
                value.fingerprint()
            }
            Self::Cleanup(value) => {
                if value.session_id != *session || value.run_id != *run {
                    return Err(PhaseError::NotFound);
                }
                value.fingerprint()
            }
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
            Self::Uncertainty(value) => verify_control(
                receipt,
                call,
                &value.session_id,
                &value.run_id,
                &value.mutation(),
                &value.fingerprint()?,
            ),
            Self::Cleanup(value) => verify_control(
                receipt,
                call,
                &value.session_id,
                &value.run_id,
                &value.mutation(),
                &value.fingerprint()?,
            ),
            Self::Start(value) => value.verify_replay(receipt),
            Self::Settle(value) => value.verify_replay(receipt, call),
            Self::Resolve(value) => value.verify_replay(receipt, call),
        }
    }
}

fn verify_control(
    receipt: &PhaseReceipt,
    call: &ToolCallRecord,
    session: &SessionId,
    run: &RunId,
    request: &PhaseMutation,
    digest: &InputFingerprint,
) -> Result<(), PhaseError> {
    if receipt.operation_id != request.id
        || receipt.phase.session_id != *session
        || receipt.phase.run_id != *run
        || receipt.phase.target != *request.command.target()
        || receipt.operation != super::operation(&request.command)
        || receipt.resolver != super::resolver(&request.command)
        || &receipt.semantic_digest != digest
        || call.run_id != *run
        || call.id != receipt.phase.target.call_id
        || super::call_binding(call)? != receipt.phase.call_binding
    {
        return Err(PhaseError::UnknownStoreOutcome);
    }
    Ok(())
}
