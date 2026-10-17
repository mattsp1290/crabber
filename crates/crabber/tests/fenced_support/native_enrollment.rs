//! Reuse the unchanged origin domain and actual borrowed caller verifier.
#[path = "../../../crabber-session/src/native_enrollment/origin.rs"]
mod origin;
pub(crate) use origin::{Origin, Status, Unclassified};
#[path = "native_raw_caller.rs"]
pub(crate) mod caller;
