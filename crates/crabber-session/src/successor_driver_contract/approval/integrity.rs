//! Decoder-valid mutations must still disagree with protected original sources.
use super::*;
pub(crate) async fn advance(store: &dyn Store, f: &Fixture) -> crabber_core::SessionRevision {
    let mut request = crate::session_owner_contract::reservation(f.receipt.session_id.clone());
    request.binding.workspace_id = f.request.context.workspace_id().into();
    request.binding.directory = f.request.context.directory().into();
    request.binding.kind = crate::session_owner::SessionOwnerKind::RetainedCapacity;
    store.reserve_session_owner(request, None).await.unwrap();
    store.session_revision(&f.receipt.session_id).await.unwrap()
}
pub(crate) fn rewrite_binding(binding: &mut crate::RetainedExecutionBinding) {
    let capsule = &mut binding.capsule;
    let old = &capsule.provider;
    capsule.provider = crabber_core::ProviderBindingDescriptor::new(
        old.provider_id(),
        old.model_id(),
        "changed-successor-backend",
        old.behavior_version(),
        old.config_fingerprint().clone(),
        old.limits(),
    )
    .unwrap();
    let components = capsule.runtime_semantics[12].as_array_mut().unwrap();
    let component = components
        .iter_mut()
        .find(|c| c["id"] == crate::PROVIDER_BINDING_COMPONENT)
        .unwrap();
    component["version"] =
        serde_json::Value::String(serde_json::to_string(&capsule.provider).unwrap());
    capsule.config_hash = crabber_core::InputFingerprint::new(crate::admission_config_hash(
        &capsule.runtime_semantics,
    ))
    .unwrap();
    capsule.input_fingerprint = capsule.computed_input_fingerprint().unwrap();
    crate::RetainedExecutionBinding::decode(&serde_json::to_string(binding).unwrap()).unwrap();
}
pub(crate) fn refreshed_event(
    event: &mut crabber_core::EventRecord,
    receipt: &crate::ApprovalReceipt,
) {
    event.payload["revision"] = serde_json::to_value(receipt.revision).unwrap();
    event.cursor = Some(receipt.event_cursor);
    crate::approval::verify_receipt(receipt, event).unwrap();
}

pub(crate) async fn begin(store: &dyn Store, f: &Fixture) -> crate::AttemptReceipt {
    let execution = store.execution(f.native.fence.clone()).await.unwrap();
    let crate::AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: store.session_revision(&f.receipt.session_id).await.unwrap(),
                command: crate::AttemptCommand::Begin {
                    target: f.batch.next_attempt.clone(),
                },
            },
            Some(f.native.authority.clone()),
        )
        .await
        .unwrap()
    else {
        panic!("actual Begin")
    };
    receipt
}
pub(crate) fn replace_parent(
    original: &mut crate::ApprovalReceipt,
    recovery: &mut crate::ApprovalReceipt,
) {
    let new_id = crabber_core::ControlId::new();
    original.operation_id = new_id.clone();
    let crate::ApprovalOutcome::ReclaimedUnstarted {
        original_claim_id, ..
    } = &mut recovery.outcome
    else {
        panic!()
    };
    *original_claim_id = new_id;
}
pub(crate) fn synchronize(event: &mut crabber_core::EventRecord, receipt: &crate::ApprovalReceipt) {
    event.cursor = Some(receipt.event_cursor);
    event.correlation = Some(receipt.operation_id.0.clone());
    event.payload["operation_id"] = serde_json::to_value(&receipt.operation_id).unwrap();
    event.payload["outcome"] = serde_json::to_value(&receipt.outcome).unwrap();
    crate::approval::verify_receipt(receipt, event).unwrap();
}
