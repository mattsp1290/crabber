//! Bounded digest of the complete actual frozen unkeyed execution capsule.
use crate::session_owner::SessionOwnerError as Error;
use crabber_core::InputFingerprint;
use sha2::{Digest, Sha256};
use std::io::Write;
struct Writer {
    digest: Sha256,
    bytes: usize,
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|count| *count <= 16 << 20)
            .ok_or_else(|| std::io::Error::other("unkeyed binding exceeds bound"))?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn unkeyed_binding(
    binding: &crate::ExecutionBindingCapsule,
) -> Result<InputFingerprint, Error> {
    let mut writer = Writer {
        digest: Sha256::new(),
        bytes: 0,
    };
    serde_json::to_writer(
        &mut writer,
        &("crabber.preparation.unkeyed-binding.v1", binding),
    )
    .map_err(|_| Error::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", writer.digest.finalize()))
        .map_err(|_| Error::InvalidRequest)
}
