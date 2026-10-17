//! Borrow every public field without allocating reference collections.
use super::{Decision, Frontend, Invocation, Serialize};
use serde::ser::{SerializeSeq, SerializeStruct};
pub(super) struct View<'a>(pub(super) &'a crate::ToolContinuationGrant);
impl Serialize for View<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let crate::ToolContinuationGrant {
            fence: _,
            authority: _,
            phases,
            frontend,
            calls,
            source_turn_id,
            usage,
            turns_used,
        } = self.0;
        let mut view = serializer.serialize_struct("ToolGrantPayload", 6)?;
        view.serialize_field("phases", &Phases(phases))?;
        view.serialize_field("frontend", &Frontends(frontend))?;
        view.serialize_field("calls", &Calls(calls))?;
        view.serialize_field("source_turn_id", source_turn_id)?;
        view.serialize_field("usage", usage)?;
        view.serialize_field("turns_used", turns_used)?;
        view.end()
    }
}
struct Phases<'a>(&'a [crate::ToolPhaseResume]);
#[derive(Serialize)]
struct PhaseView<'a> {
    target: &'a crate::PhaseTarget,
    identity: &'a crabber_core::ResumableToolIdentity,
    checkpoint: &'a crabber_core::ToolCheckpoint,
    responses: &'a [crabber_core::AcceptedToolResponse],
}
impl Serialize for Phases<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for phase in self.0 {
            let crate::ToolPhaseResume {
                target,
                authority: _,
                identity,
                checkpoint,
                responses,
            } = phase;
            sequence.serialize_element(&PhaseView {
                target,
                identity,
                checkpoint,
                responses,
            })?;
        }
        sequence.end()
    }
}
struct Frontends<'a>(&'a [crate::FrontendPhaseResume]);
impl Serialize for Frontends<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for phase in self.0 {
            let crate::FrontendPhaseResume {
                target,
                authority: _,
                result,
                frozen,
            } = phase;
            sequence.serialize_element(&Frontend {
                target,
                result,
                frozen,
            })?;
        }
        sequence.end()
    }
}
struct Calls<'a>(&'a [crate::GrantedApproval]);
impl Serialize for Calls<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for call in self.0 {
            let crate::GrantedApproval {
                target,
                call_binding,
                decision,
            } = call;
            sequence.serialize_element(&Invocation {
                target,
                call_binding,
                decision: match decision {
                    crate::GrantedDecision::Allow => Decision::Allow,
                    crate::GrantedDecision::Deny => Decision::Deny,
                    crate::GrantedDecision::NotRequired => Decision::NotRequired,
                },
            })?;
        }
        sequence.end()
    }
}
