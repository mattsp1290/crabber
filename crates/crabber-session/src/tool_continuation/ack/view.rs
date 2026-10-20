//! Borrow concrete private identities; never serialize a seal into its own hash.
use super::{Error, Serialize, ToolContinuationGrant, ToolContinuationReceipt};
use crate::session_owner::preparation::tool_driver::payload;
use serde::ser::SerializeSeq;
pub(super) fn ack<'a>(
    grant: &'a ToolContinuationGrant,
    receipt: &'a ToolContinuationReceipt,
    meter: bool,
) -> Result<impl Serialize + 'a, Error> {
    Ok((
        "crabber.native-tool-grant-ack.v1",
        receipt,
        &grant.fence,
        payload::view(grant).map_err(|_| Error::InvalidRequest)?,
        Parent {
            authority: &grant.authority,
            meter,
        },
        ToolPhases {
            phases: &grant.phases,
            meter,
        },
        FrontendPhases {
            phases: &grant.frontend,
            meter,
        },
    ))
}
struct Parent<'a> {
    authority: &'a crate::AttemptAuthority,
    meter: bool,
}
impl Serialize for Parent<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.meter {
            crate::attempt::grant_ack_meter(self.authority).serialize(serializer)
        } else {
            crate::attempt::grant_ack_identity(self.authority).serialize(serializer)
        }
    }
}
struct ToolPhases<'a> {
    phases: &'a [crate::ToolPhaseResume],
    meter: bool,
}
impl Serialize for ToolPhases<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        phases(
            serializer,
            self.phases.iter().map(|phase| &phase.authority),
            self.meter,
        )
    }
}
struct FrontendPhases<'a> {
    phases: &'a [crate::FrontendPhaseResume],
    meter: bool,
}
impl Serialize for FrontendPhases<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        phases(
            serializer,
            self.phases.iter().map(|phase| &phase.authority),
            self.meter,
        )
    }
}
fn phases<'a, S: serde::Serializer>(
    serializer: S,
    authorities: impl ExactSizeIterator<Item = &'a crate::PhaseAuthority>,
    meter: bool,
) -> Result<S::Ok, S::Error> {
    let mut sequence = serializer.serialize_seq(Some(authorities.len()))?;
    for authority in authorities {
        if meter {
            sequence.serialize_element(&crate::phase::grant_ack_meter(authority))?;
        } else {
            sequence.serialize_element(&crate::phase::grant_ack_identity(authority))?;
        }
    }
    sequence.end()
}
