//! Fresh instruction files through the runtime-authorized workspace reader.
mod chain;
mod files;
/// Optional filesystem capabilities constructed by the trusted host.
pub mod host;
mod middleware;

use async_trait::async_trait;
use crabber::extension::{Extension, ExtensionError, MiddlewareDescriptor, Registrar};
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

pub const PROMPT_NAME: &str = "workspace/instructions";
pub const DEFAULT_ORDER: i32 = 100;
/// Declares where the host roots the reader it returns for an admitted workspace.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReaderRoot {
    Boundary,
    Workspace,
}
#[derive(Clone, Serialize)]
pub struct Limits {
    pub max_file_names: usize,
    pub max_chain_depth: usize,
    pub max_file_bytes: usize,
    pub max_section_bytes: usize,
    pub max_in_flight: usize,
    pub max_wait: Duration,
}
pub struct Options {
    pub order: i32,
    /// Ordered basenames; AGENTS.md is the reference default.
    pub file_names: Vec<String>,
    pub reader_root: ReaderRoot,
    /// Absolute canonical directory strings; empty in Workspace mode.
    pub boundaries: Vec<String>,
    pub limits: Limits,
}
pub struct WorkspaceInstructions {
    policy: Arc<Policy>,
    hash: String,
}
struct Policy {
    order: i32,
    names: Vec<String>,
    reader_root: ReaderRoot,
    boundaries: Vec<String>,
    limits: Limits,
    capacity: Arc<Semaphore>,
}
impl WorkspaceInstructions {
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let l = &options.limits;
        if l.max_file_names == 0
            || l.max_file_names > 16
            || l.max_chain_depth == 0
            || l.max_chain_depth > 64
            || l.max_file_bytes == 0
            || l.max_file_bytes > 16 * 1024
            || l.max_section_bytes < 256
            || l.max_section_bytes > 32 * 1024
            || l.max_in_flight == 0
            || l.max_in_flight > 256
            || l.max_wait.is_zero()
            || l.max_wait > Duration::from_secs(5)
        {
            return Err(crate::config_error("instructions-limits"));
        }
        let mut names = std::collections::HashSet::new();
        let mut boundaries = std::collections::HashSet::new();
        if options.file_names.is_empty()
            || options.file_names.len() > l.max_file_names
            || options
                .file_names
                .iter()
                .any(|n| !valid_file_name(n) || !names.insert(n))
            || options.boundaries.len() > 16
            || options
                .boundaries
                .iter()
                .any(|b| !chain::absolute(b) || !boundaries.insert(b))
            || match options.reader_root {
                ReaderRoot::Boundary => options.boundaries.is_empty(),
                ReaderRoot::Workspace => !options.boundaries.is_empty(),
            }
        {
            return Err(crate::config_error("instructions-policy"));
        }
        let hash = crate::config_hash(&(
            "workspace-instructions-v2",
            options.order,
            &options.file_names,
            options.reader_root,
            &options.boundaries,
            l,
        ));
        Ok(Self {
            hash,
            policy: Arc::new(Policy {
                order: options.order,
                names: options.file_names,
                reader_root: options.reader_root,
                boundaries: options.boundaries,
                capacity: Arc::new(Semaphore::new(l.max_in_flight)),
                limits: options.limits,
            }),
        })
    }
}
fn valid_file_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.len() <= 255
        && !name
            .chars()
            .any(|c| c.is_control() || "/\\:*?\"<>|".contains(c))
}
fn failure(code: &'static str) -> String {
    format!("workspace instructions failed: {code}")
}
#[async_trait]
impl Extension for WorkspaceInstructions {
    fn id(&self) -> &str {
        "crabber-extensions/workspace-instructions"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let descriptor = MiddlewareDescriptor::new(
            "crabber-extensions.workspace-instructions",
            "2",
            self.hash.clone(),
        )
        .map_err(|_| crate::config_error("instructions-identity"))?;
        registrar.system_prompt_middleware(
            PROMPT_NAME,
            self.policy.order,
            descriptor,
            Arc::new(middleware::Middleware(self.policy.clone())),
        );
        Ok(())
    }
}
