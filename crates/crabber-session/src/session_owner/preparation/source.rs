//! Transient exact native source evidence; private grants are never serialized.

use crate::{ApprovalContinuationClaim, ContinuationTarget, ToolContinuationClaim};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ExistingWorkTarget {
    ExpiredRun(Box<super::ExpiredRunTarget>),
    AdmissionRecovery {
        key: crabber_core::AdmissionKey,
        issuance: u64,
    },
    ApprovalClaim(ContinuationTarget),
    ToolClaim(ContinuationTarget),
    FrontendClaim(ContinuationTarget),
    ApprovalRecovery {
        continuation: ContinuationTarget,
        attempt: crate::AttemptTarget,
    },
    FrontendRecovery {
        continuation: ContinuationTarget,
        attempt: crate::AttemptTarget,
    },
    ToolRecovery {
        continuation: ContinuationTarget,
        attempt: crate::AttemptTarget,
    },
}
/// Fresh reservation preflight uses the actual typed original native control.
/// Only its target and semantic digest enter the immutable preparation origin.
#[derive(Clone)]
pub enum PreparationSource {
    ExpiredRun(super::ExpiredRunPreparation),
    AdmissionRecovery(super::AdmissionRecoveryPreparation),
    ApprovalClaim(ApprovalContinuationClaim),
    ToolClaim(ToolContinuationClaim),
    FrontendClaim(crate::FrontendContinuationClaim),
    FrontendRecovery(crate::FrontendContinuationRecovery),
    ApprovalRecovery(crate::ApprovalContinuationRecovery),
    ToolRecovery(crate::ToolContinuationRecovery),
}

impl std::fmt::Debug for PreparationSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparationSource([redacted])")
    }
}
