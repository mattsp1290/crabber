use super::{Node, ToolRegistration, construction};
use crate::contextual::{FactoryBody, FactoryRegistration, Slot, cleanup::RecordMount};
use crate::{ContextualPlanProvider, ExtensionError, FactoryContext, FactoryScope, Scope};
use std::sync::Arc;
pub(in crate::contextual) async fn install(
    provider: &ContextualPlanProvider,
    slot: &Arc<Slot>,
    scope: FactoryScope,
    index: usize,
    registration: &FactoryRegistration,
    context: FactoryContext,
) -> Result<(), ExtensionError> {
    let FactoryBody::ToolProduct(factory) = &registration.factory else {
        unreachable!("typed Tool product registration")
    };
    let node = Node::new(slot.owner.clone());
    slot.state
        .lock()
        .unwrap()
        .producer
        .as_ref()
        .expect("tool product has host")
        .record_tool(index, node.clone());
    construction::create(factory, context, &node).await?;
    provider.check_slot(slot)?;
    let context = slot.context.as_ref().expect("factory context");
    let scope = match scope {
        FactoryScope::Workspace => Scope::Workspace(context.workspace_id().into()),
        FactoryScope::Session => Scope::SessionIn {
            session_id: context.session_id().clone(),
            workspace_id: context.workspace_id().into(),
        },
    };
    let extension = Arc::new(ToolRegistration {
        node: Arc::downgrade(&node),
        identity: registration.identity.clone(),
        config: registration.config_hash.clone(),
    });
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
        .expect("tool product has host")
        .record_mounted(index, RecordMount::Owned(mount.clone()));
    state.records[index].mount = Some(RecordMount::Owned(mount));
    Ok(())
}
