//! Actual writer accounting before independent execution carriers are copied.
use super::{Init, Source, invalid};
use crate::native_parent::RunIssuance;
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::{
    AdmissionExecutionCapsule, AdmissionExecutionRecord, AdmissionExecutionState, StoreError,
};
use crabber_core::{AdmissionKey, AdmissionReceipt};
use serde::Serialize;
#[derive(Serialize)]
struct RecordView<'a> {
    receipt: &'a AdmissionReceipt,
    key: &'a AdmissionKey,
    capsule: &'a AdmissionExecutionCapsule,
    state: AdmissionExecutionState,
}
/// The exact stored record carrier, before cloning its receipt/key/capsule.
/// This is separate from validation of a borrowed original prepared input.
pub(crate) fn measure_record(
    receipt: &AdmissionReceipt,
    key: &AdmissionKey,
    capsule: &AdmissionExecutionCapsule,
    state: AdmissionExecutionState,
    proof: &mut Proof,
) -> Result<(), ReadError<StoreError>> {
    proof
        .measure(&RecordView {
            receipt,
            key,
            capsule,
            state,
        })
        .map_err(ReadError::Budget)
}
#[derive(Serialize)]
struct InitView<'a> {
    version: u32,
    receipt: &'a AdmissionReceipt,
    key: &'a AdmissionKey,
    capsule_digest: &'a str,
    issuance: &'a RunIssuance,
}
#[derive(Serialize)]
struct SourceView<'a> {
    kind: &'static str,
    source: InitView<'a>,
}
/// The actual writer has already measured this record before constructing it.
/// Borrowed hashing reuses that carrier. The independent Init source, including
/// its real digest and original issuer, is charged before any owned source copy.
pub(crate) fn init_with_proof(
    record: &AdmissionExecutionRecord,
    issuance: &RunIssuance,
    proof: &mut Proof,
) -> Result<Source, ReadError<StoreError>> {
    let capsule_digest = record
        .capsule
        .digest()
        .map_err(|_| ReadError::Domain(invalid()))?;
    let source = SourceView {
        kind: "init",
        source: InitView {
            version: 1,
            receipt: &record.receipt,
            key: &record.key,
            capsule_digest: &capsule_digest,
            issuance,
        },
    };
    proof.measure(&source).map_err(ReadError::Budget)?;
    let init = Init {
        version: 1,
        receipt: record.receipt.clone(),
        key: record.key.clone(),
        capsule_digest,
        issuance: issuance.clone(),
    };
    init.verify(record).map_err(ReadError::Domain)?;
    Ok(Source::Init(init))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn record_view_matches_actual_stored_wire_shape() {
        let mut keyed = crate::execution_binding_contract::fixture(
            &crabber_core::SessionId::new(),
            time::OffsetDateTime::UNIX_EPOCH,
            "backend",
            'b',
        )
        .keyed;
        let capsule = keyed.execution.take().unwrap();
        let receipt = AdmissionReceipt {
            session_id: capsule.request.session_id.clone(),
            run_id: crabber_core::RunId::new(),
            user_message_id: keyed.request.user_message.id.clone(),
            fingerprint: keyed.options.fingerprint.clone(),
            semantic_digest_version: 1,
            semantic_digest: keyed.semantic_digest().unwrap(),
        };
        let record = AdmissionExecutionRecord {
            receipt,
            key: keyed.options.key.clone(),
            capsule,
            state: AdmissionExecutionState::Unstarted,
        };
        let mut proof = Proof::bounded(32 << 20);
        measure_record(
            &record.receipt,
            &record.key,
            &record.capsule,
            record.state,
            &mut proof,
        )
        .unwrap();
        assert_eq!(proof.used(), serde_json::to_vec(&record).unwrap().len());
    }
}
