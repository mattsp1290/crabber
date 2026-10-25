//! Genuine foreign private carriers cannot acquire this prelude's fixed policy.
use super::lifetime::native_closed;
use super::*;
use crate::session_owner::owned_scope::FixedInitialTestHooks;

#[tokio::test]
async fn another_real_fixed_core_is_refused_before_inner_registration() {
    let host = FixedMemoryInitialHost::new();
    let other = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let hooks = FixedInitialTestHooks::new();
    hooks.substitute_core(other.control.clone());
    let actual_owner = refused(&host, context, revision, hooks.clone()).await;
    assert!(!actual_owner.resources_pending());
    native_closed(&host, &session, &hooks);
    assert_eq!(
        other.store().list_unfinished_runs().await.unwrap(),
        [] as [crabber_core::Run; 0]
    );
    assert!(other.store().get_session(&session).await.unwrap().is_none());
}

#[tokio::test]
async fn genuine_opaque_preparation_from_another_joined_prelude_is_refused() {
    let host = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let donor = FixedInitialTestHooks::new();
    donor.capture_original();
    let mut task = host
        .register_scripted_test(
            values(context.clone(), revision, false),
            source(),
            donor.clone(),
        )
        .unwrap();
    task.start().unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observation.status().is_none());
    assert!(observation.error().is_none());
    let donor_owner = donor.owner();
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert!(!donor_owner.resources_pending());
    native_closed(&host, &session, &donor);
    let original = donor
        .take_original()
        .expect("actual original was moved from supervised prelude");
    let current = host.store().session_revision(&session).await.unwrap();
    let recipient = FixedInitialTestHooks::new();
    recipient.substitute_original(original);
    let actual_owner = refused(&host, context, current, recipient.clone()).await;
    assert!(!actual_owner.resources_pending());
    native_closed(&host, &session, &recipient);
}

#[tokio::test]
async fn another_pristine_concrete_recipe_after_actual_binding_is_refused() {
    let host = FixedMemoryInitialHost::new();
    let (context, revision) = managed(&host).await;
    let session = context.session_id().clone();
    let hooks = FixedInitialTestHooks::new();
    hooks.substitute_source(
        LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "library".into(),
                model_id: "finite-text".into(),
            },
            vec!["genuine other recipe".into()],
        )
        .unwrap(),
    );
    let actual_owner = refused(&host, context, revision, hooks.clone()).await;
    assert!(!actual_owner.resources_pending());
    native_closed(&host, &session, &hooks);
}

async fn refused(
    host: &FixedMemoryInitialHost,
    context: SessionContext,
    revision: SessionRevision,
    hooks: Arc<FixedInitialTestHooks>,
) -> Arc<super::super::super::fixed_preparation::FixedPreparationOwner> {
    let mut task = host
        .register_scripted_test(values(context, revision, false), source(), hooks.clone())
        .unwrap();
    task.start().unwrap();
    let observed = tokio::time::timeout(Duration::from_secs(5), task.join())
        .await
        .unwrap()
        .unwrap();
    assert!(observed.error().is_some());
    assert!(
        observed.status().is_none(),
        "no replacement original can register an inner producer"
    );
    let actual_owner = hooks.owner();
    drop(task);
    tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
        .await
        .unwrap();
    assert_eq!(
        host.store().list_unfinished_runs().await.unwrap(),
        [] as [crabber_core::Run; 0]
    );
    assert_eq!(host.control.tasks().available_capacity(), 64);
    actual_owner
}
