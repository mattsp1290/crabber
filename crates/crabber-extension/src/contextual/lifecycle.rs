use super::{ContextualPlanProvider, INITIALIZING, Owner, Slot};
use crate::{ExtensionError, MountHandle};
use crabber_core::SessionContext;
use futures::future::join_all;
use std::{sync::atomic::Ordering, time::Duration};

impl ContextualPlanProvider {
    /// Close only this session's owned instances. Other sessions and workspace
    /// services remain available. Timeout retains cleanup and keeps ingress shut.
    /// # Errors
    /// Rejects drift/self-close, or reports incomplete cleanup under the bound.
    pub async fn close_session(
        &self,
        context: &SessionContext,
        bound: Duration,
    ) -> Result<(), ExtensionError> {
        self.close_context(context, false, bound).await
    }

    /// Close a workspace's instances and its session instances, never its files.
    /// # Errors
    /// Rejects drift, missing workspace identity, self-close or incomplete cleanup.
    pub async fn close_workspace(
        &self,
        context: &SessionContext,
        bound: Duration,
    ) -> Result<(), ExtensionError> {
        if context.workspace_id().is_empty() {
            return Err(ExtensionError::WorkspaceUnavailable);
        }
        self.close_context(context, true, bound).await
    }

    async fn close_context(
        &self,
        context: &SessionContext,
        workspace: bool,
        bound: Duration,
    ) -> Result<(), ExtensionError> {
        let provider = self.clone();
        let task = {
            let mut owners = self.inner.owners.lock().unwrap();
            if !workspace {
                self.inner.dependencies.check_context(context)?;
            }
            if owners
                .roots
                .get(context.workspace_id())
                .is_some_and(|root| root != context.directory())
            {
                return Err(ExtensionError::ContextIdentityMismatch);
            }
            let slots: Vec<_> = owners
                .slots
                .values()
                .filter(|slot| {
                    slot.context.as_ref().is_some_and(|stored| {
                        if workspace {
                            stored.workspace_id() == context.workspace_id()
                        } else {
                            stored.session_id() == context.session_id()
                                && matches!(slot.owner, Owner::Session(_))
                        }
                    })
                })
                .cloned()
                .collect();
            for slot in &slots {
                if !workspace && slot.context.as_ref() != Some(context) {
                    return Err(ExtensionError::ContextIdentityMismatch);
                }
                Self::check_self_close(slot)?;
            }
            if workspace {
                owners
                    .closed_workspaces
                    .insert(context.workspace_id().into());
            } else {
                owners.closed_sessions.insert(context.session_id().clone());
            }
            for slot in &slots {
                Self::signal_slot(slot);
            }
            let dependencies = self.inner.dependencies.seal_context(context, workspace);
            self.inner.reapers.tracker().spawn(async move {
                let (dependency_result, results) = tokio::join!(
                    super::dependencies::join(&dependencies, bound),
                    join_all(slots.iter().map(|slot| async {
                        super::initialization::join_slot(slot).await?;
                        let _gate = slot.gate.lock().await;
                        provider.clean_records(slot).await
                    }))
                );
                dependency_result?;
                results
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map(|_| ())
            })
        };
        Self::wait_cleanup(task, bound).await
    }

    /// Make the whole profile terminal and start all cleanup. No caller drop or
    /// timeout relinquishes reaper ownership. A later call can establish completion.
    /// # Errors
    /// Rejects self-close or reports incomplete initialization/cleanup.
    /// # Panics
    /// Panics if a previous internal panic poisoned an ownership lock.
    pub async fn close_all(&self, bound: Duration) -> Result<(), ExtensionError> {
        let provider = self.clone();
        let task = {
            let owners = self.inner.owners.lock().unwrap();
            if INITIALIZING.try_with(|_| ()).is_ok() {
                return Err(ExtensionError::SelfClose);
            }
            let mounts = self.inner.registry.begin_close_all()?;
            self.inner.closed.store(true, Ordering::SeqCst);
            self.inner.initializers.close();
            let slots: Vec<_> = owners.slots.values().cloned().collect();
            for slot in &slots {
                Self::signal_slot(slot);
            }
            let dependencies = self.inner.dependencies.seal_all();
            self.inner.reapers.tracker().spawn(async move {
                provider.inner.initializers.wait_drained().await;
                for slot in &slots {
                    super::initialization::join_slot(slot).await?;
                }
                let (dependency_result, results) = tokio::join!(
                    super::dependencies::join(&dependencies, bound),
                    join_all(mounts.iter().rev().map(MountHandle::close))
                );
                dependency_result?;
                let cleanup: Vec<_> = slots
                    .iter()
                    .flat_map(|slot| {
                        slot.state
                            .lock()
                            .unwrap()
                            .records
                            .iter()
                            .map(|record| record.cleanup.clone())
                            .collect::<Vec<_>>()
                    })
                    .collect();
                let cleanup = join_all(cleanup.iter().map(|owner| owner.wait_drained())).await;
                results.into_iter().collect::<Result<Vec<_>, _>>()?;
                cleanup.into_iter().collect::<Result<Vec<_>, _>>()?;
                for slot in slots {
                    slot.state.lock().unwrap().records.clear();
                }
                Ok(())
            })
        };
        Self::wait_cleanup(task, bound).await
    }

    fn check_self_close(slot: &Slot) -> Result<(), ExtensionError> {
        if INITIALIZING
            .try_with(|owner| owner == &slot.owner)
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        for record in &slot.state.lock().unwrap().records {
            if let Some(mount) = &record.mount {
                mount.check_close_allowed()?;
            }
        }
        Ok(())
    }

    fn signal_slot(slot: &Slot) {
        let mut state = slot.state.lock().unwrap();
        state.closed = true;
        for record in &state.records {
            record.cleanup.close();
            if let Some(mount) = &record.mount {
                mount.deactivate();
            }
        }
    }

    pub(super) fn signal_records(slot: &Slot) {
        for record in &slot.state.lock().unwrap().records {
            record.cleanup.close();
            if let Some(mount) = &record.mount {
                mount.deactivate();
            }
        }
    }

    pub(super) async fn clean_records(&self, slot: &Slot) -> Result<(), ExtensionError> {
        let records: Vec<_> = slot
            .state
            .lock()
            .unwrap()
            .records
            .iter()
            .map(|record| (record.cleanup.clone(), record.mount.clone()))
            .collect();
        for (cleanup, mount) in &records {
            cleanup.close();
            if let Some(mount) = mount {
                mount.deactivate();
            }
        }
        let results = join_all(
            records
                .into_iter()
                .rev()
                .map(|(cleanup, mount)| async move {
                    let result = if let Some(mount) = mount {
                        mount.close().await
                    } else {
                        Ok(())
                    };
                    cleanup.wait_drained().await?;
                    result
                }),
        )
        .await;
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
        slot.state.lock().unwrap().records.clear();
        Ok(())
    }

    async fn wait_cleanup(
        task: tokio::task::JoinHandle<Result<(), ExtensionError>>,
        bound: Duration,
    ) -> Result<(), ExtensionError> {
        match tokio::time::timeout(bound, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) | Err(_) => Err(ExtensionError::ContextCleanupIncomplete),
        }
    }
}
