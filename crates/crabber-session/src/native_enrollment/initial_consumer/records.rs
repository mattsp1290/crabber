use super::{Correlation, Failure, InputFingerprint, Original, Proof, digest, unknown};
use crabber_core::{ControlId, RunId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Anchor {
    pub version: u32,
    pub correlation: Correlation,
    pub birth_open_digest: InputFingerprint,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoOpenedMember {
    pub version: u32,
    pub correlation: Correlation,
    pub birth_close_digest: InputFingerprint,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(
    tag = "origin",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum Origin {
    Opened(Anchor),
    NoOpenedMember(NoOpenedMember),
}
impl Origin {
    pub(crate) fn correlation(&self) -> &Correlation {
        match self {
            Self::Opened(a) => &a.correlation,
            Self::NoOpenedMember(n) => &n.correlation,
        }
    }
    pub(crate) fn digest(&self) -> Result<InputFingerprint, Failure> {
        digest(&("crabber.initial-consumer.origin.v1", self))
    }
    pub(crate) fn validate(&self, proof: &mut Proof) -> Result<(), Failure> {
        proof.measure(self)?;
        self.correlation().validate(proof)?;
        let version = match self {
            Self::Opened(a) => a.version,
            Self::NoOpenedMember(n) => n.version,
        };
        if version != 1 {
            return Err(unknown("consumer origin version"));
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AssignmentLink {
    pub version: u32,
    pub correlation: Correlation,
    pub anchor_digest: InputFingerprint,
    pub assignment: crate::native_enrollment::pending_initial::Assignment,
}
impl AssignmentLink {
    pub(crate) fn run_id(&self) -> &RunId {
        &self.assignment.run_id
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CallKind {
    Register,
    Activate,
    KeyedBegin,
    ReserveInitial,
    BeginInitial,
    AssistantMessage,
    RunAdmitted,
    RunStarted,
    TurnStarted,
    MessageCommitted,
    TurnCompleted,
    Conclude,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RegisteredCall {
    pub version: u32,
    pub correlation: Correlation,
    pub call: ControlId,
    pub anchor_digest: InputFingerprint,
    pub kind: CallKind,
    pub input_digest: InputFingerprint,
    pub prerequisites: Vec<InputFingerprint>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CallResult {
    pub version: u32,
    pub correlation: Correlation,
    pub call: ControlId,
    pub registration_digest: InputFingerprint,
    pub original: Original,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RosterEntry {
    pub version: u32,
    pub correlation: Correlation,
    pub call: ControlId,
    pub registration_digest: InputFingerprint,
    pub result_digest: InputFingerprint,
    pub reverse_digests: Vec<InputFingerprint>,
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReverseKind {
    Driver,
    Activation,
    KeyedBegin,
    InitialAttempt,
    AttemptBegin,
    Message,
    Event,
    Conclusion,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReverseMembership {
    pub version: u32,
    pub correlation: Correlation,
    pub call: ControlId,
    pub kind: ReverseKind,
    pub member_key: InputFingerprint,
    pub result_digest: InputFingerprint,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsumerClose {
    pub version: u32,
    pub correlation: Correlation,
    pub anchor_digest: InputFingerprint,
    pub birth_close_digest: InputFingerprint,
    pub prior_head_digest: InputFingerprint,
    pub roster_digest: InputFingerprint,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PublicationKind {
    Opened,
    Assigned,
    CommittedCall,
    Closed,
    NoOpenedMember,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publication {
    pub version: u32,
    pub correlation: Correlation,
    pub sequence: u64,
    pub kind: PublicationKind,
    pub original_digest: InputFingerprint,
    pub previous_digest: Option<InputFingerprint>,
    pub digest: InputFingerprint,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    pub version: u32,
    pub correlation: Correlation,
    pub origin_digest: InputFingerprint,
    pub sequence: u64,
    pub digest: InputFingerprint,
}
macro_rules! hashed {
    ($($name:ident => $domain:literal),* $(,)?) => {$(impl $name {
        pub(crate) fn digest(&self) -> Result<InputFingerprint, Failure> {
            digest(&($domain, self))
        }
    })*};
}
hashed! {
    AssignmentLink => "crabber.initial-consumer.assignment.v1",
    RegisteredCall => "crabber.initial-consumer.call.v1",
    CallResult => "crabber.initial-consumer.result.v1",
    ReverseMembership => "crabber.initial-consumer.reverse.v1",
    ConsumerClose => "crabber.initial-consumer.close.v1",
}
// Routing accessors and the individual roster digest serve SQL indexed rows.
// Canonical relation/hash verification below remains available in every build.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
hashed! { RosterEntry => "crabber.initial-consumer.roster.v1" }
macro_rules! sql_correlated {
    ($($name:ident),* $(,)?) => {$(
        #[cfg(any(feature = "postgres", feature = "sqlite"))]
        impl $name {
            pub(crate) fn correlation(&self)->&Correlation { &self.correlation }
        }
    )*};
}
sql_correlated! { AssignmentLink, RegisteredCall, RosterEntry, ReverseMembership, ConsumerClose, Publication, Head }
impl CallResult {
    pub(crate) fn correlation(&self) -> &Correlation {
        &self.correlation
    }
}
impl Publication {
    pub(crate) fn canonical_digest(&self) -> Result<InputFingerprint, Failure> {
        digest(&(
            "crabber.initial-consumer.publication.v1",
            self.version,
            &self.correlation,
            self.sequence,
            self.kind,
            &self.original_digest,
            &self.previous_digest,
        ))
    }
}
