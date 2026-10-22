//! Standard workspace tools as one Crabber extension, with explicit host policies.
#![doc = include_str!("../../../docs/standard-tools.md")]
use async_trait::async_trait;
use crabber::{
    ExtensionError, ToolDefinition,
    core::ToolInfo,
    extension::{Extension, Registrar},
};
use crabber_tools_core::{
    CONFIG_IDENTITY_VERSION, Capacity, Limits, TOOLS_IDENTITY_REVISION, WorkspaceRoot, config_hash,
    schema_hash,
};
use crabber_tools_search::SearchPolicy;
use crabber_tools_shell::ShellPolicy;
use crabber_tools_trackerwrite::TrackerPolicy;
use crabber_tools_urlfetch::UrlFetchPolicy;
use crabber_tools_userinteract::UserInteractPolicy;
use std::sync::Arc;
mod factory;
pub use factory::StandardToolsFactory;
mod ids;
pub mod prelude;
pub use ids::*;
/// A fully constructed tool and its deterministic metadata.
pub struct Definition {
    /// Canonical catalog id.
    pub id: ToolId,
    /// Model-facing information.
    pub info: ToolInfo,
    /// Versioned schema identity.
    pub schema_hash: String,
    /// Native executor.
    pub definition: Arc<ToolDefinition>,
}
/// Root-free, deterministic metadata in canonical id order.
pub fn metadata() -> Vec<(ToolId, ToolInfo, String)> {
    ToolId::ALL
        .into_iter()
        .map(|id| {
            let info = match id {
                ToolId::FileRead => crabber_tools_fileops::read::info(),
                ToolId::FileWrite => crabber_tools_fileops::write::info(),
                ToolId::FileEdit => crabber_tools_fileops::edit::info(),
                ToolId::FileList => crabber_tools_fileops::list::info(),
                ToolId::MultiEdit => crabber_tools_fileops::multi_edit::info(),
                ToolId::FileDelete => crabber_tools_fileops::file_delete::info(),
                ToolId::FileMove => crabber_tools_fileops::file_move::info(),
                ToolId::Search => crabber_tools_search::info(),
                ToolId::Shell => crabber_tools_shell::info(),
                ToolId::Glob => crabber_tools_glob::info(),
                ToolId::ApplyPatch => crabber_tools_applypatch::info(),
                ToolId::UrlFetch => crabber_tools_urlfetch::info(),
                ToolId::TrackerWrite => crabber_tools_trackerwrite::info(),
                ToolId::UserInteract => crabber_tools_userinteract::info(),
            };
            let hash = schema_hash(&info);
            (id, info, hash)
        })
        .collect()
}
/// Root-independent policies cloned into each admitted workspace.
#[derive(Clone)]
pub struct Policies {
    /// Register only this subset; cannot be empty.
    pub enabled: EnabledSet,
    /// Run-wide restriction: also denies other extensions and host tools. Usually false.
    pub restrict_to_enabled: bool,
    /// Host-owned shell policy.
    pub shell: ShellPolicy,
    /// Host-owned ripgrep policy.
    pub search: SearchPolicy,
    /// Explicit URL policy; required when UrlFetch is enabled.
    pub url_fetch: Option<UrlFetchPolicy>,
    /// Explicit bn backend policy; required when TrackerWrite is enabled.
    pub tracker: Option<TrackerPolicy>,
    /// Explicit interaction policy; required when UserInteract is enabled.
    pub user_interact: Option<UserInteractPolicy>,
    /// Shared finite bounds for tools other than user_interact.
    pub limits: Limits,
}
/// One extension for one root. Different roots on one agent collide on tool names.
pub struct StandardTools {
    definitions: Vec<Definition>,
    hash: String,
    restrict: bool,
}
impl StandardTools {
    /// Validate all policies and build a shared capacity plus process-global root lock.
    pub fn new(root: Arc<WorkspaceRoot>, options: Policies) -> Result<Self, ExtensionError> {
        options.validate()?;
        let capacity = Capacity::new(&options.limits)?;
        let files = Arc::new(crabber_tools_fileops::Options {
            root: root.clone(),
            limits: options.limits.clone(),
            capacity: capacity.clone(),
            pre_write: None,
        });
        let search = Arc::new(crabber_tools_search::Options {
            root: root.clone(),
            limits: options.limits.clone(),
            capacity: capacity.clone(),
            policy: options.search.clone(),
        });
        let shell = Arc::new(crabber_tools_shell::Options {
            root: root.clone(),
            limits: options.limits.clone(),
            capacity: capacity.clone(),
            policy: options.shell.clone(),
        });
        let mut definitions = vec![];
        for (id, info, hash) in metadata() {
            if options.enabled.contains(id) {
                let definition = match id {
                    ToolId::FileRead => crabber_tools_fileops::read::definition(files.clone()),
                    ToolId::FileWrite => crabber_tools_fileops::write::definition(files.clone()),
                    ToolId::FileEdit => crabber_tools_fileops::edit::definition(files.clone()),
                    ToolId::FileList => crabber_tools_fileops::list::definition(files.clone()),
                    ToolId::MultiEdit => {
                        crabber_tools_fileops::multi_edit::definition(files.clone())
                    }
                    ToolId::FileDelete => {
                        crabber_tools_fileops::file_delete::definition(files.clone())
                    }
                    ToolId::FileMove => crabber_tools_fileops::file_move::definition(files.clone()),
                    ToolId::Search => crabber_tools_search::definition(search.clone())?,
                    ToolId::Shell => crabber_tools_shell::definition(shell.clone())?,
                    ToolId::Glob => {
                        crabber_tools_glob::definition(Arc::new(crabber_tools_glob::Options {
                            root: root.clone(),
                            limits: options.limits.clone(),
                            capacity: capacity.clone(),
                        }))
                    }
                    ToolId::ApplyPatch => crabber_tools_applypatch::definition(Arc::new(
                        crabber_tools_applypatch::Options {
                            root: root.clone(),
                            limits: options.limits.clone(),
                            capacity: capacity.clone(),
                        },
                    )),
                    ToolId::UrlFetch => crabber_tools_urlfetch::definition(Arc::new(
                        crabber_tools_urlfetch::Options {
                            root: root.clone(),
                            limits: options.limits.clone(),
                            capacity: capacity.clone(),
                            policy: options.url_fetch.clone().ok_or_else(|| {
                                ExtensionError::Plan("url_fetch policy is required".into())
                            })?,
                        },
                    ))?,
                    ToolId::TrackerWrite => crabber_tools_trackerwrite::definition(Arc::new(
                        crabber_tools_trackerwrite::Options {
                            root: root.clone(),
                            limits: options.limits.clone(),
                            capacity: capacity.clone(),
                            policy: options.tracker.clone().ok_or_else(|| {
                                ExtensionError::Plan("tracker_write policy is required".into())
                            })?,
                        },
                    ))?,
                    ToolId::UserInteract => crabber_tools_userinteract::definition(Arc::new(
                        crabber_tools_userinteract::Options {
                            policy: options.user_interact.clone().ok_or_else(|| {
                                ExtensionError::Plan("user_interact policy is required".into())
                            })?,
                        },
                    ))?,
                };
                definitions.push(Definition {
                    id,
                    info,
                    schema_hash: hash,
                    definition,
                });
            }
        }
        if definitions.is_empty() {
            return Err(ExtensionError::Plan("enabled set is empty".into()));
        }
        let hash = config_hash(&(options.config_identity(), root.path()));
        Ok(Self {
            definitions,
            hash,
            restrict: options.restrict_to_enabled,
        })
    }
    /// Enabled tools in canonical registration order.
    pub fn definitions(&self) -> &[Definition] {
        &self.definitions
    }
}
#[async_trait]
impl Extension for StandardTools {
    fn id(&self) -> &str {
        "crabber-tools/standard"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
        for d in &self.definitions {
            r.tool(d.definition.clone());
        }
        if self.restrict {
            r.restrict_tools(self.definitions.iter().map(|d| d.info.name.clone()));
        }
        Ok(())
    }
}

impl Policies {
    // One owner for the root-free fingerprint contract. Both mounting paths use
    // identical enabled schemas and policy inputs; shared mounts additionally bind root.
    pub(crate) fn config_identity(&self) -> String {
        let identities: Vec<_> = metadata()
            .into_iter()
            .filter(|(id, _, _)| self.enabled.contains(*id))
            .map(|(id, _, hash)| (id.id(), hash))
            .collect();
        config_hash(&(
            CONFIG_IDENTITY_VERSION,
            TOOLS_IDENTITY_REVISION,
            identities,
            &self.shell,
            &self.search,
            &self.url_fetch,
            &self.tracker,
            &self.user_interact,
            &self.limits,
            self.restrict_to_enabled,
        ))
    }

    /// Validate all configured policies, including optional disabled-tool policies.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        self.shell.validate()?;
        self.search.validate()?;
        self.limits.validate()?;
        if let Some(policy) = &self.url_fetch {
            policy.validate()?;
        }
        if let Some(policy) = &self.tracker {
            policy.validate()?;
        }
        if let Some(policy) = &self.user_interact {
            policy.validate()?;
        }
        if self.enabled.contains(ToolId::UrlFetch) && self.url_fetch.is_none() {
            return Err(ExtensionError::Plan("url_fetch policy is required".into()));
        }
        if self.enabled.contains(ToolId::TrackerWrite) && self.tracker.is_none() {
            return Err(ExtensionError::Plan(
                "tracker_write policy is required".into(),
            ));
        }
        if self.enabled.contains(ToolId::UserInteract) && self.user_interact.is_none() {
            return Err(ExtensionError::Plan(
                "user_interact policy is required".into(),
            ));
        }
        if !ToolId::ALL.into_iter().any(|id| self.enabled.contains(id)) {
            return Err(ExtensionError::Plan("enabled set is empty".into()));
        }
        Ok(())
    }
}
