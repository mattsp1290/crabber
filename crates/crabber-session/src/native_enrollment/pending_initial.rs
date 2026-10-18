//! Closed Pending Initial source and publication relations. No business or `MainJoin`.
#[path = "pending_initial/ledger.rs"]
mod ledger;
#[path = "pending_initial/records.rs"]
mod records;
#[path = "pending_initial/relation.rs"]
mod relation;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::InputFingerprint;
pub(crate) use ledger::Ledger;
pub(crate) use records::*;
pub(crate) use relation::*;
use serde::Serialize;
const RECORD_BYTES: usize = 8192;
pub(crate) fn digest(value: &impl Serialize) -> Result<InputFingerprint, Failure> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value).map_err(unknown)?;
    InputFingerprint::new(format!("{:x}", Sha256::digest(bytes))).map_err(unknown)
}
fn bounded(value: &impl Serialize, proof: &mut Proof) -> Result<(), Failure> {
    proof.measure(value)?;
    if crate::snapshot::encoded_bytes(value, RECORD_BYTES).is_none() {
        return Err(unknown("Pending Initial record limit"));
    }
    Ok(())
}

native_pending_initial_items! {
    #[path = "pending_initial/prefix.rs"]
    pub(crate) mod prefix;
    #[path = "pending_initial/publication.rs"]
    mod publication;
    pub(crate) use publication::*;
}
