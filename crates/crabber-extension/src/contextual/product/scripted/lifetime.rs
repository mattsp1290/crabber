use super::{
    Arc, CancellationToken, ExtensionError, LibraryScriptedActive, Mutex, NEXT, Node, Ordering,
    Owner, ScriptedProvider, SessionContext, State,
};
use crate::{Extension, Registrar, Registry, Scope};
use async_trait::async_trait;
struct Empty {
    #[cfg(test)]
    installation: Option<Arc<super::probe::Installation>>,
}
#[async_trait]
impl Extension for Empty {
    fn id(&self) -> &'static str {
        "crabber.library-scripted-source"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed-empty-registration-v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        #[cfg(test)]
        if let Some(installation) = &self.installation {
            installation.entered.notify_one();
            installation.release.notified().await;
        }
        Ok(())
    }
}
struct Activation(Arc<State>, bool);
impl Drop for Activation {
    fn drop(&mut self) {
        if !self.1 {
            self.0.signal_close();
        }
        self.0.root.done(std::thread::panicking());
        self.0.installed.cancel();
    }
}
pub(super) async fn activate(
    provider: ScriptedProvider,
    session: SessionContext,
    #[cfg(test)] installation: Option<Arc<super::probe::Installation>>,
) -> Result<LibraryScriptedActive, ExtensionError> {
    let creator = crate::registry::current_callback_invocations()
        .ok_or(crate::ResourceRegistrationError::Unsupported)?;
    if !creator.matches_current() {
        return Err(ExtensionError::ContextOwnerClosed);
    }
    let brand = NEXT
        .try_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
        .map_err(|_| crate::ResourceRegistrationError::Capacity)?;
    let owner = Owner::LibraryScripted(brand);
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| crate::ResourceRegistrationError::Unsupported)?;
    let root = Node::new(owner.clone());
    root.install(Mutex::new(provider));
    root.captured()?;
    let state = Arc::new(State {
        owner,
        root,
        mount: Mutex::new(None),
        streams: Mutex::new(Vec::new()),
        admission: Mutex::new(true),
        closing: CancellationToken::new(),
        installed: CancellationToken::new(),
        creator,
        runtime,
        failed: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        probe: Mutex::new(None),
    });
    let mut guard = Activation(state.clone(), false);
    // Attach before the original installation can yield, so caller close owns
    // both the incomplete install and every handle it may actually publish.
    state.creator.retain_scripted_source(state.clone())?;
    #[cfg(test)]
    if let Some(installation) = &installation {
        *installation.state.lock().unwrap() = Some(Arc::downgrade(&state));
    }
    let registry = Registry::new();
    let mounted = registry
        .mount_owned(
            Arc::new(Empty {
                #[cfg(test)]
                installation,
            }),
            Scope::SessionIn {
                session_id: session.session_id().clone(),
                workspace_id: session.workspace_id().into(),
            },
        )
        .await?;
    // Publish the original handle before any bind/attachment can refuse.
    *state.mount.lock().unwrap() = Some(mounted.clone());
    let (_, roster) = mounted.scripted_source()?;
    state.root.bind(roster)?;
    state.check_current()?;
    guard.1 = true;
    Ok(LibraryScriptedActive { state })
}
