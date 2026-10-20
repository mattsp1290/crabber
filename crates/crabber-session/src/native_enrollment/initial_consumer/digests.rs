//! Ephemeral correspondence for one verification of an actual selected cohort.
//! This is a lookup optimization, never independent source or dispatch evidence.
use super::{CallResult, Failure, InputFingerprint, Proof, unknown};

pub(crate) struct ResultIndex<'a>(Vec<(InputFingerprint, &'a CallResult)>);
impl<'a> ResultIndex<'a> {
    pub(crate) fn new(results: &[&'a CallResult], proof: &mut Proof) -> Result<Self, Failure> {
        let bytes = results
            .len()
            .checked_mul(std::mem::size_of::<(InputFingerprint, &CallResult)>() + 64)
            .ok_or_else(|| unknown("consumer digest index size"))?;
        // Precharge the actual table and each owned 64-byte digest before any
        // allocation. Full originals remain borrowed and stream into SHA256.
        proof.charge(bytes)?;
        let mut entries: Vec<(InputFingerprint, &'a CallResult)> =
            Vec::with_capacity(results.len());
        for result in results {
            let digest = result.digest()?;
            if entries.iter().any(|(actual, _)| *actual == digest) {
                return Err(unknown("duplicate consumer result digest"));
            }
            entries.push((digest, *result));
        }
        Ok(Self(entries))
    }
    pub(crate) fn get(
        &self,
        digest: &InputFingerprint,
    ) -> Option<(&InputFingerprint, &'a CallResult)> {
        self.0
            .iter()
            .find(|(actual, _)| actual == digest)
            .map(|(actual, result)| (actual, *result))
    }
}
