//! Publication of an already constructed concrete product registration.
use crate::contextual::{Slot, cleanup::RecordMount};
use crate::{ContextualPlanProvider, Extension, ExtensionError, FactoryScope, Scope};
use std::sync::Arc;

pub(super) fn scope(slot: &Slot, scope: FactoryScope) -> Scope {
    let context = slot.context.as_ref().expect("factory context");
    match scope {
        FactoryScope::Workspace => Scope::Workspace(context.workspace_id().into()),
        FactoryScope::Session => Scope::SessionIn {
            session_id: context.session_id().clone(),
            workspace_id: context.workspace_id().into(),
        },
    }
}

pub(super) async fn publish(
    provider: &ContextualPlanProvider,
    slot: &Arc<Slot>,
    scope: Scope,
    index: usize,
    extension: Arc<dyn Extension>,
    host_expectation: &'static str,
) -> Result<(), ExtensionError> {
    let mount = provider
        .inner
        .registry
        .mount_owned_with_failure_signal(extension, scope, || {
            ContextualPlanProvider::signal_records(slot);
        })
        .await?;
    let mut state = slot.state.lock().unwrap();
    state
        .producer
        .as_ref()
        .expect(host_expectation)
        .record_mounted(index, RecordMount::Owned(mount.clone()));
    state.records[index].mount = Some(RecordMount::Owned(mount));
    Ok(())
}
