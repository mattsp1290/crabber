use super::ToolResourceOwner;
use crate::{CallbackInvocationOwner, with_callback_invocations};

#[tokio::test]
async fn actual_callback_parent_identity_survives_spawn_and_seal() {
    let owner = ToolResourceOwner::new();
    let other = ToolResourceOwner::new();
    let callbacks = CallbackInvocationOwner::attach(owner.resources()).unwrap();
    assert!(!owner.is_current_callback_parent());
    with_callback_invocations(callbacks.capability(), async {
        assert!(owner.is_current_callback_parent());
        assert!(!other.is_current_callback_parent());
        let context = crate::dispatch::CallbackTaskContext::capture_existing();
        let child_owner = owner.clone();
        let child_other = other.clone();
        tokio::spawn(context.scope(async move {
            child_owner.close();
            assert!(child_owner.is_current_callback_parent());
            assert!(!child_other.is_current_callback_parent());
        }))
        .await
        .unwrap();
    })
    .await;
    assert!(!owner.is_current_callback_parent());
    drop(callbacks);
    let drained = owner.drain(std::time::Duration::from_secs(1)).await;
    assert_eq!(drained.pending, 0);
    assert_eq!(drained.failures, []);
}
