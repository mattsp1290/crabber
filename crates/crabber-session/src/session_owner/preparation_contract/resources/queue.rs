//! A real protected Queue preparation A, restored factory projection B, Root and child.
use super::*;
use crate::session_owner::preparation::{PreparationKey, queue_admission_intent};
use crate::{QueueAction, QueueMutation};
use crabber_core::{MessageId, SessionContext};
pub(crate) async fn prepared(store: &impl Store) -> (SessionContext, JoinedPreparation, MessageId) {
    let mut f = crate::queue_contract::fixture(store).await;
    let contract = InputFingerprint::new("d".repeat(64)).unwrap();
    declaration::bind(&mut f.prepared, &contract);
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = crate::queue_contract::claim(store, &f).await;
    let prepare = crate::queue_contract::mutation(
        store,
        &f,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    store.mutate_queue(prepare).await.unwrap();
    let actual_id = f.prepared.keyed.request.user_message.id.clone();
    let revision = store.session_revision(&f.session).await.unwrap();
    let id = ControlId::new();
    let mut request = reservation(&f.prepared);
    request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
    request.expected_revision = revision;
    request.resource_contract = Some(contract);
    request.purpose = PreparationPurpose::Queue {
        authority: authority.clone(),
    };
    request.intent = queue_admission_intent(&f.prepared).unwrap();
    let request = request.bind_key(PreparationKey::Queue(id.clone())).unwrap();
    let root = begin(store, &request).await;
    let input = f.prepared.binding.input_envelope.clone().unwrap();
    let restored =
        crate::typed_admission_contract::prepare(f.prepared, input, OffsetDateTime::UNIX_EPOCH);
    assert_ne!(restored.keyed.request.user_message.id, actual_id);
    let operation = PreparedOperation::Queue {
        request: QueueMutation {
            session_id: f.session.clone(),
            id,
            expected_revision: revision,
            action: QueueAction::Admit {
                authority,
                owner: "queue-admission-owner".into(),
                lease: std::time::Duration::from_secs(30),
            },
        },
        binding: Some(Box::new(restored)),
    };
    let joined = initial_driver_observation(store, root, operation).await;
    (
        SessionContext::new(f.session, "bound-workspace", "/bound/workspace"),
        joined,
        actual_id,
    )
}
