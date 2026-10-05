//! Plan-sealed workspace `AGENTS.md` system-prompt middleware.

use async_trait::async_trait;
use crabber_extension::{
    Extension, ExtensionError, MAX_PROMPT_CONTRIBUTION_BYTES, MiddlewareDescriptor,
    ModelAttemptContext, Registrar, SystemPromptMiddleware, WorkspaceReadErrorKind,
};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt::Write as _, sync::Arc};

pub const AGENTS_MD_EXTENSION_ID: &str = "crabber/middleware/agentsmd";
pub const AGENTS_MD_REGISTRATION_ID: &str = "workspace-agents-md";
pub const AGENTS_MD_KIND: &str = "agentsmd";
pub const AGENTS_MD_VERSION: &str = "1";
pub const AGENTS_MD_MAX_FILES: usize = 16;
pub const AGENTS_MD_MAX_FILE_BYTES: usize = 32 * 1024;
const MAX_PATH_BYTES: usize = 1024;

/// Validation failure for [`AgentsMdConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AgentsMdConfigError {
    #[error("AGENTS.md files count must be between 1 and 16")]
    InvalidFileCount,
    #[error("duplicate AGENTS.md path")]
    DuplicatePath,
    #[error("invalid AGENTS.md path")]
    InvalidPath,
}

/// Validated configuration for the first-party `AGENTS.md` recipe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentsMdConfig {
    files: Vec<String>,
    required: bool,
}

impl AgentsMdConfig {
    /// Creates a configuration without rewriting any path.
    ///
    /// # Errors
    /// Returns an error unless every path and the total file count satisfy the
    /// recipe's strict portable-relative-path grammar.
    pub fn new(files: Vec<String>, required: bool) -> Result<Self, AgentsMdConfigError> {
        if !(1..=AGENTS_MD_MAX_FILES).contains(&files.len()) {
            return Err(AgentsMdConfigError::InvalidFileCount);
        }
        let mut unique = HashSet::with_capacity(files.len());
        for path in &files {
            if !valid_path(path) {
                return Err(AgentsMdConfigError::InvalidPath);
            }
            if !unique.insert(path.as_str()) {
                return Err(AgentsMdConfigError::DuplicatePath);
            }
        }
        Ok(Self { files, required })
    }

    #[must_use]
    pub fn files(&self) -> &[String] {
        &self.files
    }

    #[must_use]
    pub fn required(&self) -> bool {
        self.required
    }
}

impl Default for AgentsMdConfig {
    fn default() -> Self {
        Self::new(vec!["AGENTS.md".into()], false).expect("default configuration is valid")
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentsMdConfigWire {
    files: Vec<String>,
    required: bool,
}

impl<'de> Deserialize<'de> for AgentsMdConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = AgentsMdConfigWire::deserialize(deserializer)?;
        Self::new(wire.files, wire.required).map_err(serde::de::Error::custom)
    }
}

fn valid_path(path: &str) -> bool {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains("//")
        || path.contains(['\\', ':', '\0'])
        || path.chars().any(char::is_control)
    {
        return false;
    }
    path.split('/')
        .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// First-party extension that installs exactly one typed prompt middleware.
#[derive(Clone)]
pub struct AgentsMdExtension {
    config: AgentsMdConfig,
    order: i32,
    descriptor: MiddlewareDescriptor,
    config_hash: String,
}

impl AgentsMdExtension {
    /// Revalidates the configuration and seals its canonical SHA-256 identity.
    ///
    /// # Errors
    /// Returns an error when the supplied configuration is invalid.
    ///
    /// # Panics
    /// Panics only if serializing the fixed configuration fields or constructing
    /// the compile-time-constant descriptor unexpectedly fails.
    pub fn new(config: AgentsMdConfig) -> Result<Self, AgentsMdConfigError> {
        let config = AgentsMdConfig::new(config.files, config.required)?;
        let canonical = serde_json::to_vec(&config).expect("validated configuration serializes");
        let config_hash = hex_sha256(&canonical);
        let descriptor =
            MiddlewareDescriptor::new(AGENTS_MD_KIND, AGENTS_MD_VERSION, config_hash.clone())
                .expect("first-party middleware descriptor is valid");
        Ok(Self {
            config,
            order: 0,
            descriptor,
            config_hash,
        })
    }

    #[must_use]
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }
}

impl Default for AgentsMdExtension {
    fn default() -> Self {
        Self::new(AgentsMdConfig::default()).expect("default extension configuration is valid")
    }
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

#[async_trait]
impl Extension for AgentsMdExtension {
    fn id(&self) -> &str {
        AGENTS_MD_EXTENSION_ID
    }

    fn version(&self) -> &str {
        AGENTS_MD_VERSION
    }

    fn config_hash(&self) -> String {
        self.config_hash.clone()
    }

    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.system_prompt_middleware(
            AGENTS_MD_REGISTRATION_ID,
            self.order,
            self.descriptor.clone(),
            Arc::new(AgentsMdMiddleware {
                config: self.config.clone(),
            }),
        );
        Ok(())
    }
}

struct AgentsMdMiddleware {
    config: AgentsMdConfig,
}

#[async_trait]
impl SystemPromptMiddleware for AgentsMdMiddleware {
    async fn contribute(&self, context: ModelAttemptContext) -> Result<Option<String>, String> {
        if context.cancellation().is_cancelled() {
            return Err(cancelled());
        }
        let resolver = context
            .workspace_reader_resolver()
            .ok_or_else(|| "AGENTS.md workspace reader unavailable".to_owned())?;
        let reader = resolver
            .resolve(context.workspace())
            .await
            .map_err(|_| "AGENTS.md workspace reader unavailable".to_owned())?;

        let mut frames = Vec::new();
        for path in self.config.files() {
            if context.cancellation().is_cancelled() {
                return Err(cancelled());
            }
            let bytes = match reader.read_limited(path, AGENTS_MD_MAX_FILE_BYTES).await {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == WorkspaceReadErrorKind::NotFound => {
                    if self.config.required() {
                        return Err("required AGENTS.md file is unavailable".into());
                    }
                    continue;
                }
                Err(_) => return Err("AGENTS.md workspace read failed".into()),
            };
            if bytes.len() > AGENTS_MD_MAX_FILE_BYTES {
                return Err("AGENTS.md workspace read failed".into());
            }
            let body = std::str::from_utf8(&bytes)
                .map_err(|_| "AGENTS.md workspace read failed".to_owned())?;
            frames.push(format!(
                "## Workspace instructions: {path}\n<!-- crabber:agentsmd bytes={} -->\n{body}\n## End workspace instructions: {path}\n",
                bytes.len()
            ));
        }
        if context.cancellation().is_cancelled() {
            return Err(cancelled());
        }
        if frames.is_empty() {
            return Ok(None);
        }
        let contribution = frames.join("\n");
        if contribution.len() > MAX_PROMPT_CONTRIBUTION_BYTES {
            return Err("AGENTS.md contribution exceeds byte limit".into());
        }
        Ok(Some(contribution))
    }
}

fn cancelled() -> String {
    "AGENTS.md contribution cancelled".into()
}
