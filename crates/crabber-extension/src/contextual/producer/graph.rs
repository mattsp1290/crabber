//! Exact registered graph observations, never physical-object disposal proof.
use super::{ContextualGenerationMember, Registration};
use crate::contextual::{
    Initialization, Owner, Slot, cleanup::RecordCleanup, cleanup::RecordMount,
};
use crate::{ExtensionError, OwnedMountJoin, ServiceGenerationDescriptor};
use futures::future::join_all;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

pub(super) struct Graph {
    initializer: Arc<Initialization>,
    slot: Weak<Slot>,
    owner: Owner,
    members: Mutex<Vec<Option<Member>>>,
    cleanup_gate: AsyncMutex<()>,
}
#[derive(Clone)]
enum Product {
    Root(Arc<crate::contextual::product::Cell>),
    Tool(Arc<crate::contextual::product::tool::Node>),
    WorkspaceFs(Arc<crate::contextual::product::workspace_fs::Node>),
    WorkspaceReaderResolver(Arc<crate::contextual::product::workspace_reader::Node>),
    Subprocess(Arc<crate::contextual::product::subprocess::Node>),
    Provider(Arc<crate::contextual::product::provider::Node>),
}
impl Product {
    fn close(&self) {
        match self {
            Self::Root(root) => root.close(),
            Self::Tool(node) => node.close(),
            Self::WorkspaceFs(node) => node.close(),
            Self::WorkspaceReaderResolver(node) => node.close(),
            Self::Subprocess(node) => node.close(),
            Self::Provider(node) => node.close(),
        }
    }
    async fn join(&self) -> Result<(), ExtensionError> {
        match self {
            Self::Root(root) => root.join().await,
            Self::Tool(node) => node.join().await,
            Self::WorkspaceFs(node) => node.join().await,
            Self::WorkspaceReaderResolver(node) => node.join().await,
            Self::Subprocess(node) => node.join().await,
            Self::Provider(node) => node.join().await,
        }
    }
    fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Root(left), Self::Root(right)) => Arc::ptr_eq(left, right),
            (Self::Tool(left), Self::Tool(right)) => Arc::ptr_eq(left, right),
            (Self::WorkspaceFs(left), Self::WorkspaceFs(right)) => Arc::ptr_eq(left, right),
            (Self::WorkspaceReaderResolver(left), Self::WorkspaceReaderResolver(right)) => {
                Arc::ptr_eq(left, right)
            }
            (Self::Subprocess(left), Self::Subprocess(right)) => Arc::ptr_eq(left, right),
            (Self::Provider(left), Self::Provider(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}
struct Member {
    cleanup: Arc<RecordCleanup>,
    root: Option<Product>,
    mount: Option<RecordMount>,
    mount_identity: Option<u64>,
    failed: bool,
    joined: Option<MemberJoin>,
}
#[derive(Clone)]
enum MemberJoin {
    Cleanup,
    Mounted(OwnedMountJoin),
}
/// Observation of the exact original generation; no acquisition lease is pinned.
#[derive(Clone)]
pub struct ContextualGenerationObservation {
    pub(super) registration: Arc<Registration>,
}
/// Original constructor roster status, determined after actual constructor join.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextualGenerationMemberDisposition {
    NotEntered,
    CleanupJoined,
    MountedJoined,
}
/// Constructor-private original registered-graph evidence. This is not object
/// disposal, physical generation closure, native authority or resource-policy proof.
#[derive(Clone)]
pub struct JoinedContextualGenerationGraph {
    registration: Arc<Registration>,
    dispositions: Vec<ContextualGenerationMemberDisposition>,
    mount_joins: Vec<Option<OwnedMountJoin>>,
}
impl JoinedContextualGenerationGraph {
    #[must_use]
    pub fn generation(&self) -> &ServiceGenerationDescriptor {
        &self.registration.source.generation
    }
    #[must_use]
    pub fn members(&self) -> &[ContextualGenerationMember] {
        &self.registration.source.members
    }
    #[must_use]
    pub fn dispositions(&self) -> &[ContextualGenerationMemberDisposition] {
        &self.dispositions
    }
    /// Original owned-mount join for the frozen member ordinal, when mounted.
    #[must_use]
    pub fn mount_join(&self, member: usize) -> Option<&OwnedMountJoin> {
        self.mount_joins.get(member).and_then(Option::as_ref)
    }
}
impl ContextualGenerationObservation {
    #[must_use]
    pub fn generation(&self) -> &ServiceGenerationDescriptor {
        &self.registration.source.generation
    }
    /// Inspect the original entered root-only member, never a replacement.
    /// # Errors
    /// Refuses a non-root or not-yet-entered member.
    /// # Panics
    /// Panics if an internal graph lock is poisoned.
    pub fn product_root(
        &self,
        member: usize,
    ) -> Result<crate::ContextualProductRootObservation, ExtensionError> {
        let cell = self
            .registration
            .graph
            .members
            .lock()
            .unwrap()
            .get(member)
            .and_then(Option::as_ref)
            .and_then(|member| match &member.root {
                Some(Product::Root(root)) => Some(root.clone()),
                _ => None,
            })
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(crate::ContextualProductRootObservation {
            graph: self.clone(),
            cell,
            member,
        })
    }
    /// Inspect the exact original entered Box Tool node.
    /// # Errors
    /// Refuses a different or not-yet-entered member.
    /// # Panics
    /// Panics if an internal ownership lock is poisoned.
    pub fn tool_product(
        &self,
        member: usize,
    ) -> Result<crate::ContextualToolProductObservation, ExtensionError> {
        let node = self
            .registration
            .graph
            .members
            .lock()
            .unwrap()
            .get(member)
            .and_then(Option::as_ref)
            .and_then(|member| match &member.root {
                Some(Product::Tool(node)) => Some(node.clone()),
                _ => None,
            })
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(crate::ContextualToolProductObservation {
            graph: self.clone(),
            node,
            member,
        })
    }
    /// Inspect the exact original entered Box filesystem node.
    /// # Errors
    /// Refuses a different or not-yet-entered member.
    /// # Panics
    /// Panics if an internal ownership lock is poisoned.
    pub fn workspace_fs_product(
        &self,
        member: usize,
    ) -> Result<crate::ContextualWorkspaceFsProductObservation, ExtensionError> {
        let node = self
            .registration
            .graph
            .members
            .lock()
            .unwrap()
            .get(member)
            .and_then(Option::as_ref)
            .and_then(|member| match &member.root {
                Some(Product::WorkspaceFs(node)) => Some(node.clone()),
                _ => None,
            })
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(crate::ContextualWorkspaceFsProductObservation {
            graph: self.clone(),
            node,
            member,
        })
    }
    /// Inspect the exact original entered Box reader resolver node.
    /// # Errors
    /// Refuses a different or not-yet-entered member.
    /// # Panics
    /// Panics if an internal ownership lock is poisoned.
    pub fn workspace_reader_resolver_product(
        &self,
        member: usize,
    ) -> Result<crate::ContextualWorkspaceReaderResolverProductObservation, ExtensionError> {
        let node = self
            .registration
            .graph
            .members
            .lock()
            .unwrap()
            .get(member)
            .and_then(Option::as_ref)
            .and_then(|member| match &member.root {
                Some(Product::WorkspaceReaderResolver(node)) => Some(node.clone()),
                _ => None,
            })
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(crate::ContextualWorkspaceReaderResolverProductObservation {
            graph: self.clone(),
            node,
            member,
        })
    }
    /// Inspect the exact original entered Box subprocess node.
    /// # Errors
    /// Refuses a different or not-yet-entered member.
    /// # Panics
    /// Panics if an internal ownership lock is poisoned.
    pub fn subprocess_product(
        &self,
        member: usize,
    ) -> Result<crate::ContextualSubprocessProductObservation, ExtensionError> {
        let node = self
            .registration
            .graph
            .members
            .lock()
            .unwrap()
            .get(member)
            .and_then(Option::as_ref)
            .and_then(|member| match &member.root {
                Some(Product::Subprocess(node)) => Some(node.clone()),
                _ => None,
            })
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(crate::ContextualSubprocessProductObservation {
            graph: self.clone(),
            node,
            member,
        })
    }
    /// Signal only this original generation, never a replacement in its slot.
    /// # Errors
    /// Refuses self-close or a replaced generation without changing either slot.
    /// # Panics
    /// Panics if an internal slot lock has been poisoned.
    pub fn request_close(&self) -> Result<(), ExtensionError> {
        let registration = &self.registration;
        registration.graph.check_self_close()?;
        if let Some(slot) = registration.graph.slot.upgrade() {
            let mut state = slot.state.lock().unwrap();
            if state
                .producer
                .as_ref()
                .is_none_or(|current| !Arc::ptr_eq(current, registration))
            {
                return Err(ExtensionError::ContextIdentityMismatch);
            }
            state.closed = true;
        }
        registration.close();
        registration.graph.signal();
        Ok(())
    }
    /// Wait for original initializer/host/constructor and registered member joins.
    /// This does not initiate close or certify disposal of held object references.
    /// # Errors
    /// Reports self-close, timeout, abnormal original tasks or incomplete cleanup.
    pub async fn registered_graph_join(
        &self,
        bound: Duration,
    ) -> Result<JoinedContextualGenerationGraph, ExtensionError> {
        self.registration.graph.check_self_close()?;
        tokio::time::timeout(bound, self.join())
            .await
            .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
    async fn join(&self) -> Result<JoinedContextualGenerationGraph, ExtensionError> {
        let registration = &self.registration;
        registration.source.closing.cancelled().await;
        // Early actual cleanup may release the still-running constructor. A final
        // pass after its original initializer joins observes any late mount record.
        let (initialized, early) = tokio::join!(
            registration.graph.initializer.join(),
            registration.graph.clean(),
        );
        initialized?;
        early?;
        registration.cleanup().await?;
        registration.graph.clean().await?;
        let members = registration.graph.members.lock().unwrap();
        let dispositions = members
            .iter()
            .map(|member| match member {
                None => Ok(ContextualGenerationMemberDisposition::NotEntered),
                Some(member) => match &member.joined {
                    Some(MemberJoin::Mounted(_))
                        if !member.failed && member.mount_identity.is_some() =>
                    {
                        Ok(ContextualGenerationMemberDisposition::MountedJoined)
                    }
                    Some(MemberJoin::Cleanup) if !member.failed => {
                        Ok(ContextualGenerationMemberDisposition::CleanupJoined)
                    }
                    _ => Err(ExtensionError::ContextCleanupIncomplete),
                },
            })
            .collect::<Result<_, _>>()?;
        let mount_joins = members
            .iter()
            .map(
                |member| match member.as_ref().and_then(|member| member.joined.as_ref()) {
                    Some(MemberJoin::Mounted(joined)) => Some(joined.clone()),
                    _ => None,
                },
            )
            .collect();
        Ok(JoinedContextualGenerationGraph {
            registration: registration.clone(),
            dispositions,
            mount_joins,
        })
    }
}
impl Graph {
    pub(super) fn new(slot: &Arc<Slot>, count: usize) -> Result<Self, ExtensionError> {
        let initializer = slot
            .state
            .lock()
            .unwrap()
            .initialization
            .clone()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(Self {
            initializer,
            slot: Arc::downgrade(slot),
            owner: slot.owner.clone(),
            members: Mutex::new((0..count).map(|_| None).collect()),
            cleanup_gate: AsyncMutex::new(()),
        })
    }
    fn check_self_close(&self) -> Result<(), ExtensionError> {
        if crate::contextual::INITIALIZING
            .try_with(|owner| owner == &self.owner)
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        for member in self.members.lock().unwrap().iter().flatten() {
            if let Some(mount) = &member.mount {
                mount.check_close_allowed()?;
            }
        }
        Ok(())
    }
    pub(super) fn entered(&self, index: usize, cleanup: Arc<RecordCleanup>) {
        let mut members = self.members.lock().unwrap();
        let member = members
            .get_mut(index)
            .expect("original frozen member ordinal");
        assert!(member.is_none(), "original member entered once");
        *member = Some(Member {
            cleanup,
            root: None,
            mount: None,
            mount_identity: None,
            failed: false,
            joined: None,
        });
    }
    pub(super) fn mounted(&self, index: usize, mount: RecordMount) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered member");
        member.mount_identity = Some(mount.identity());
        member.mount = Some(mount);
        member.joined = None;
    }
    pub(super) fn root(&self, index: usize, root: Arc<crate::contextual::product::Cell>) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered root member");
        assert!(member.root.is_none(), "original root cell once");
        member.root = Some(Product::Root(root));
        member.joined = None;
    }
    pub(super) fn tool(&self, index: usize, node: Arc<crate::contextual::product::tool::Node>) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered tool member");
        assert!(member.root.is_none());
        member.root = Some(Product::Tool(node));
        member.joined = None;
    }
    pub(super) fn workspace_fs(
        &self,
        index: usize,
        node: Arc<crate::contextual::product::workspace_fs::Node>,
    ) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered filesystem member");
        assert!(member.root.is_none());
        member.root = Some(Product::WorkspaceFs(node));
        member.joined = None;
    }
    pub(super) fn workspace_reader_resolver(
        &self,
        index: usize,
        node: Arc<crate::contextual::product::workspace_reader::Node>,
    ) {
        let mut members = self.members.lock().unwrap();
        let member = members[index]
            .as_mut()
            .expect("entered reader resolver member");
        assert!(member.root.is_none());
        member.root = Some(Product::WorkspaceReaderResolver(node));
        member.joined = None;
    }
    pub(super) fn subprocess(
        &self,
        index: usize,
        node: Arc<crate::contextual::product::subprocess::Node>,
    ) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered subprocess member");
        assert!(member.root.is_none());
        member.root = Some(Product::Subprocess(node));
        member.joined = None;
    }
    pub(super) fn provider(
        &self,
        index: usize,
        node: Arc<crate::contextual::product::provider::Node>,
    ) {
        let mut members = self.members.lock().unwrap();
        let member = members[index].as_mut().expect("entered provider member");
        assert!(member.root.is_none());
        member.root = Some(Product::Provider(node));
        member.joined = None;
    }
    pub(super) fn failed(&self, index: usize) {
        self.members.lock().unwrap()[index]
            .as_mut()
            .expect("entered member")
            .failed = true;
    }
    pub(super) fn signal(&self) {
        for member in self.members.lock().unwrap().iter().flatten() {
            member.cleanup.close();
            if let Some(root) = &member.root {
                root.close();
            }
            if let Some(mount) = &member.mount {
                mount.signal();
            }
        }
    }
    pub(super) async fn clean(&self) -> Result<(), ExtensionError> {
        let _gate = self.cleanup_gate.lock().await;
        let members: Vec<_> = self
            .members
            .lock()
            .unwrap()
            .iter()
            .enumerate()
            .filter_map(|(index, member)| {
                member.as_ref().map(|member| {
                    (
                        index,
                        member.cleanup.clone(),
                        member.mount.clone(),
                        member.joined.clone(),
                        member.root.clone(),
                    )
                })
            })
            .collect();
        self.signal();
        let results = join_all(members.into_iter().rev().map(
            |(index, cleanup, mount, joined, root)| async move {
                if let Some(joined) = joined {
                    if let Some(root) = &root {
                        root.join().await?;
                    }
                    return Ok((index, joined, root));
                }
                let closing = async {
                    match mount {
                        Some(mount) => mount.join_owned().await.map(MemberJoin::Mounted),
                        None => Ok(MemberJoin::Cleanup),
                    }
                };
                let root_join = async {
                    match &root {
                        Some(root) => root.join().await,
                        None => Ok(()),
                    }
                };
                let (mount, cleanup, root_result) =
                    tokio::join!(closing, cleanup.wait_drained(), root_join);
                cleanup?;
                root_result?;
                Ok((index, mount?, root))
            },
        ))
        .await;
        let mut members = self.members.lock().unwrap();
        let mut error = None;
        let mut retired = Vec::new();
        for result in results {
            match result {
                Ok((index, joined, root)) => {
                    let member = members[index].as_mut().expect("original member");
                    // A constructor may publish a mount during early cleanup.
                    let same_root = match (&root, &member.root) {
                        (None, None) => true,
                        (Some(left), Some(right)) => left.same(right),
                        _ => false,
                    };
                    if same_root
                        && (matches!(joined, MemberJoin::Mounted(_)) || member.mount.is_none())
                    {
                        member.joined = Some(joined);
                        if let Some(mount) = member.mount.take() {
                            retired.push(mount);
                        }
                    }
                    if member.failed {
                        error = Some(ExtensionError::ContextCleanupIncomplete);
                    }
                }
                Err(failure) => {
                    error = Some(failure);
                }
            }
        }
        drop(members);
        // Original tasks are joined. Release our extra mount handles outside
        // graph locks; this does not claim final disposal of the mounted object.
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(retired)))
        {
            crate::dispatch::discard_panic_payload(payload);
            for member in self.members.lock().unwrap().iter_mut().flatten() {
                member.failed = true;
            }
            return Err(ExtensionError::ContextCleanupIncomplete);
        }
        error.map_or(Ok(()), Err)
    }
}
impl std::fmt::Debug for JoinedContextualGenerationGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedContextualGenerationGraph")
            .field("generation", self.generation())
            .field("dispositions", &self.dispositions)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;

impl Drop for Graph {
    fn drop(&mut self) {
        if let Ok(members) = self.members.get_mut() {
            for root in members
                .iter()
                .flatten()
                .filter_map(|member| member.root.as_ref())
            {
                root.close();
            }
        }
    }
}
