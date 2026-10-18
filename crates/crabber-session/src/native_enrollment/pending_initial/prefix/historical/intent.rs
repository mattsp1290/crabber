//! Recover only semantic preparation intent; do not hash a normalized request.
use super::{Failure, HistoricalInitialSources, PreparationPurpose, Proof, unknown};
use crate::session_owner::preparation::{PreparationKey, admission_intent};
use crabber_core::AdmissionOptions;

pub(super) fn verify(
    source: &HistoricalInitialSources<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let root = &source.audits.reserve.record;
    let origin = root
        .preparation
        .as_ref()
        .ok_or_else(|| unknown("missing preparation Origin"))?;
    let binding = &source.binding.capsule;
    if !matches!(origin.purpose, PreparationPurpose::OrdinaryAdmission)
        || origin.resource_contract.is_none()
        || origin.resource_contract
            != crate::session_owner::preparation::resource_contract(&binding.runtime_semantics)
                .map_err(unknown)?
        || binding.request.session_id != source.session.id
        || binding.request.workspace_id != source.session.workspace_id
        || binding.request.directory != source.session.directory
        || binding.behavior_fingerprint != root.snapshot.binding.behavior_fingerprint
    {
        return Err(unknown("historical Initial semantic binding scope"));
    }
    let input = binding
        .input_envelope
        .as_ref()
        .ok_or_else(|| unknown("missing original typed input"))?;
    if input.expected_revision() != origin.expected_revision {
        return Err(unknown("historical original admission revision"));
    }
    let options = match (&origin.key, source.keyed) {
        (PreparationKey::Unkeyed(_), None) if source.witness.is_unkeyed() => {
            source
                .witness
                .verify_unkeyed_binding(binding)
                .map_err(unknown)?;
            None
        }
        (PreparationKey::Admission(key), Some((record, init))) if !source.witness.is_unkeyed() => {
            init.verify(record).map_err(unknown)?;
            let capsule = &record.capsule;
            if key != &record.key
                || init.driver_key() != key
                || record.receipt.session_id != source.session.id
                || record.receipt.run_id != source.initial.state.run_id
                || record.receipt.user_message_id != source.binding.user_message_id
                || record.receipt.fingerprint != capsule.fingerprint
                || capsule.request != binding.request
                || capsule.input_envelope != binding.input_envelope
                || capsule.runtime_semantics != binding.runtime_semantics
                || capsule.behavior_fingerprint != binding.behavior_fingerprint
                || capsule.config_hash != binding.config_hash.as_str()
                || capsule.plan_fingerprint != binding.plan_fingerprint.as_str()
            {
                return Err(unknown("historical original keyed options/binding"));
            }
            proof.measure(&(key, &capsule.fingerprint, &capsule.behavior_fingerprint))?;
            Some(AdmissionOptions {
                key: key.clone(),
                fingerprint: capsule.fingerprint.clone(),
                behavior_fingerprint: capsule.behavior_fingerprint.clone(),
            })
        }
        _ => return Err(unknown("historical Initial keyed/unkeyed source kind")),
    };
    let intent = admission_intent(&binding.request, input, options.as_ref()).map_err(unknown)?;
    if intent != origin.intent {
        return Err(unknown("historical original preparation semantic intent"));
    }
    Ok(())
}
