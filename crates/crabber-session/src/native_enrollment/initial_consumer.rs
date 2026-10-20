//! Protected originals for the closed, original Initial consumer.
//! Stored records describe actual native publications. They never recreate a
//! task, a resource source, a grant, or permission to dispatch.
#[path = "initial_consumer/classification.rs"]
mod classification;
#[path = "initial_consumer/ledger.rs"]
mod ledger;
pub(crate) use classification::InitialConsumption;
#[path = "initial_consumer/original.rs"]
mod original;
#[path = "initial_consumer/policy.rs"]
mod policy;
pub(crate) use policy::*;
#[path = "initial_consumer/digests.rs"]
mod digests;
#[path = "initial_consumer/records.rs"]
mod records;
#[path = "initial_consumer/relation.rs"]
mod relation;
pub(crate) use digests::ResultIndex;
#[path = "initial_consumer/retirement.rs"]
mod retirement;
pub(crate) use super::pending_initial::{Correlation, SourceKey};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::InputFingerprint;
pub(crate) use ledger::Ledger;
pub(crate) use original::*;
pub(crate) use records::*;
pub(crate) use relation::*;
pub(crate) use retirement::*;
use serde::Serialize;

pub(crate) fn digest(value: &impl Serialize) -> Result<InputFingerprint, Failure> {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    struct Writer(Sha256);
    impl std::io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer(Sha256::new());
    serde_json::to_writer(&mut writer, value).map_err(unknown)?;
    let mut encoded = String::with_capacity(64);
    write!(&mut encoded, "{:x}", writer.0.finalize()).map_err(unknown)?;
    InputFingerprint::new(encoded).map_err(unknown)
}
fn copied<T: Serialize + Clone>(value: &T, proof: &mut Proof) -> Result<T, Failure> {
    proof.measure(value)?;
    Ok(value.clone())
}
native_pending_initial_items! {
    pub(crate) use classification::classify_closed;
    #[cfg(test)]
    pub(crate) use ledger::Projection;
    #[path = "initial_consumer/publication.rs"]
    mod publication;
    pub(crate) use publication::*;
    #[path = "initial_consumer/call.rs"]
    mod call;
    pub(crate) use call::*;
}
