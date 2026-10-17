//! Acquisitions observe service ownership before initialization or exposure.
use super::{ContextualPlanProvider, ExtensionError, FactoryScope, INITIALIZING, Owner, RunPlan};
use crabber_core::SessionContext;

impl ContextualPlanProvider {
    pub(super) async fn acquire_context(
        &self,
        context: &SessionContext,
        binding: Option<super::native_dependency::Binding>,
    ) -> Result<RunPlan, ExtensionError> {
        if INITIALIZING
            .try_with(|owner| match owner {
                Owner::Shared => true,
                Owner::Workspace(id) => id == context.workspace_id(),
                Owner::Session(id) => id == context.session_id(),
            })
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        {
            let mut owners = self.inner.owners.lock().unwrap();
            self.check_open()?;
            owners.validate(context)?;
            self.inner.dependencies.bind_context(context)?;
            if binding.is_none() {
                self.inner.dependencies.check_legacy(context)?;
            }
        }
        self.wait_initialization(async {
            self.ensure_owner_registered(Owner::Shared, None, binding.clone())
                .await?;
            if self
                .inner
                .factories
                .iter()
                .any(|f| f.scope == FactoryScope::Workspace)
            {
                if context.workspace_id().is_empty() {
                    return Err(ExtensionError::WorkspaceUnavailable);
                }
                self.ensure_owner_registered(
                    Owner::Workspace(context.workspace_id().into()),
                    Some(context),
                    binding.clone(),
                )
                .await?;
            }
            if self
                .inner
                .factories
                .iter()
                .any(|f| f.scope == FactoryScope::Session)
            {
                self.ensure_owner_registered(
                    Owner::Session(context.session_id().clone()),
                    Some(context),
                    binding.clone(),
                )
                .await?;
            }
            let mut owners = self.inner.owners.lock().unwrap();
            self.check_open()?;
            owners.validate(context)?;
            let generations = owners
                .slots
                .values()
                .filter(|slot| match &slot.owner {
                    Owner::Shared => true,
                    Owner::Workspace(id) => id == context.workspace_id(),
                    Owner::Session(id) => id == context.session_id(),
                })
                .filter_map(|slot| slot.state.lock().unwrap().generation.clone())
                .collect();
            let mut plan = self.inner.dependencies.track(context, generations, || {
                self.inner.registry.try_acquire_in(context)
            })?;
            self.append_root_components(&mut plan);
            Ok(plan)
        })
        .await
    }
}
