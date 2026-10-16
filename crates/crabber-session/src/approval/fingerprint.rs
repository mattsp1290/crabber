//! Canonical bounded approval semantics shared by native and original-source checks.
use crate::ApprovalError as Error;
use crabber_core::InputFingerprint;
use serde::Serialize;
use sha2::{Digest, Sha256};
pub(crate) fn fingerprint(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    if crate::snapshot::encoded_bytes(value, 4 * 1024 * 1024).is_none() {
        return Err(Error::InvalidRequest);
    }
    let mut value = serde_json::to_value(value).map_err(|_| Error::InvalidRequest)?;
    value.sort_all_objects();
    let bytes = serde_json::to_vec(&value).map_err(|_| Error::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", Sha256::digest(bytes))).map_err(|_| Error::InvalidRequest)
}
