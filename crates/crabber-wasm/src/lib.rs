//! Sandboxed Component Model extensions.
#![allow(clippy::missing_errors_doc)]
mod adapters;

use async_trait::async_trait;
use crabber_core::ToolInfo;
use crabber_extension::{
    Extension, ExtensionError, Registrar, ToolContext, ToolDefinition, ToolExecutor,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Notify};
use wasmtime::component::{Component, Instance, Linker, ResourceTable, Val, types::ComponentItem};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, UpdateDeadline};
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

pub type LogObserver = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_module_bytes: usize,
    pub max_memory_bytes: usize,
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    pub call_timeout: Duration,
    pub close_drain: Duration,
    pub max_state_entries: usize,
    pub max_state_value_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_module_bytes: 16 << 20,
            max_memory_bytes: 64 << 20,
            max_input_bytes: 256 << 10,
            max_output_bytes: 256 << 10,
            call_timeout: Duration::from_secs(2),
            close_drain: Duration::from_secs(2),
            max_state_entries: 64,
            max_state_value_bytes: 16 << 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum InstanceMode {
    #[default]
    PerCall,
    Persistent,
}

#[derive(Debug, Clone)]
pub struct ModuleConfig {
    pub name: String,
    pub path: PathBuf,
    pub allowed_root: PathBuf,
    pub expected_sha256: [u8; 32],
    pub config_json: String,
    pub limits: Limits,
    pub instance_mode: InstanceMode,
}

#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("invalid module config: {0}")]
    Config(String),
    #[error("module path rejected: {0}")]
    Path(String),
    #[error("module SHA-256 mismatch")]
    Hash,
    #[error("module or payload exceeds configured size")]
    Size,
    #[error("component contract violation: {0}")]
    Contract(String),
    #[error("guest trapped: {0}")]
    Trap(String),
    #[error("guest call timed out")]
    Timeout,
    #[error("loader closed")]
    Closed,
    #[error("invalid guest payload: {0}")]
    Payload(String),
    #[error("wasmtime engine error: {0}")]
    Engine(String),
}

pub struct Loader {
    engine: Engine,
    closed: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    drained: Arc<Notify>,
    close_drain_ms: AtomicU64,
    log_observer: LogObserver,
}

pub struct LoadedModule {
    pub component: Component,
    pub engine: Engine,
    pub config: ModuleConfig,
    pub roles: Vec<&'static str>,
    admission: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    drained: Arc<Notify>,
    serial: Mutex<()>,
    log_observer: LogObserver,
}

struct ActiveCall {
    active: Arc<AtomicUsize>,
    drained: Arc<Notify>,
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        if self.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
    }
}

pub struct WasmExtension {
    config: ModuleConfig,
}

impl WasmExtension {
    #[must_use]
    pub fn new(config: ModuleConfig) -> Self {
        Self { config }
    }
}

struct WasmTool {
    _loader: Arc<Loader>,
    module: Arc<LoadedModule>,
    name: String,
}

#[async_trait]
impl ToolExecutor for WasmTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        self.invoke(arguments, None).await
    }

    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        self.invoke(arguments, Some(context)).await
    }
}

impl WasmTool {
    async fn invoke(
        &self,
        arguments: Value,
        context: Option<ToolContext>,
    ) -> Result<Value, ExtensionError> {
        let turn = turn_metadata(context.as_ref());
        let args = [
            Val::String(self.name.clone()),
            Val::String(
                context
                    .as_ref()
                    .map_or_else(String::new, |context| context.call_id.to_string()),
            ),
            Val::String(arguments.to_string()),
            turn,
        ];
        let result = self
            .module
            .call("tool-api", "execute", &args)
            .await
            .map_err(|e| ExtensionError::Tool(e.to_string()))?;
        let Val::Result(Ok(Some(value))) = result else {
            return Err(ExtensionError::Tool("guest execute rejected call".into()));
        };
        let Val::String(json) = *value else {
            return Err(ExtensionError::Tool("guest returned non-JSON".into()));
        };
        serde_json::from_str(&json)
            .map_err(|e| ExtensionError::Tool(format!("invalid guest JSON: {e}")))
    }
}

fn turn_metadata(context: Option<&ToolContext>) -> Val {
    let string = |name: &str, value: String| (name.into(), Val::String(value));
    Val::Record(vec![
        string(
            "run-id",
            context.map_or_else(String::new, |v| v.run_id.to_string()),
        ),
        string(
            "session-id",
            context.map_or_else(String::new, |v| v.session_id.to_string()),
        ),
        string("epoch-id", String::new()),
        ("turn-index".into(), Val::U32(0)),
        string("agent-name", String::new()),
        string("agent-mode", String::new()),
        string("provider-id", String::new()),
        string("model-id", String::new()),
        ("tool-names".into(), Val::List(vec![])),
        ("message-count".into(), Val::U32(0)),
        (
            "role-counts".into(),
            Val::Record(
                ["system", "user", "assistant", "tool"]
                    .into_iter()
                    .map(|key| (key.into(), Val::U32(0)))
                    .collect(),
            ),
        ),
        ("has-system-prompt".into(), Val::Bool(false)),
        string("workspace-id", String::new()),
    ])
}

fn turn_metadata_from_projection(value: &Value) -> Val {
    let mut metadata = turn_metadata(None);
    if let Val::Record(fields) = &mut metadata {
        for (name, key) in [
            ("run-id", "run_id"),
            ("session-id", "session_id"),
            ("provider-id", "provider_id"),
            ("model-id", "model_id"),
        ] {
            if let Some((_, field)) = fields.iter_mut().find(|(field, _)| field == name) {
                *field = Val::String(value[key].as_str().unwrap_or_default().to_owned());
            }
        }
        for (name, key) in [
            ("turn-index", "turn_index"),
            ("message-count", "message_count"),
        ] {
            if let Some((_, field)) = fields.iter_mut().find(|(field, _)| field == name) {
                *field = Val::U32(
                    value[key]
                        .as_u64()
                        .unwrap_or_default()
                        .try_into()
                        .unwrap_or(u32::MAX),
                );
            }
        }
        if let Some((_, field)) = fields
            .iter_mut()
            .find(|(field, _)| field == "has-system-prompt")
        {
            *field = Val::Bool(value["has_system_prompt"].as_bool().unwrap_or(false));
        }
    }
    metadata
}

#[async_trait]
impl Extension for WasmExtension {
    fn id(&self) -> &str {
        &self.config.name
    }
    fn version(&self) -> &'static str {
        "0.1.0"
    }
    fn config_hash(&self) -> String {
        format!("{:x?}", self.config.expected_sha256)
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let loader = Arc::new(Loader::new().map_err(|e| ExtensionError::Plan(e.to_string()))?);
        let module = loader
            .load(self.config.clone())
            .await
            .map_err(|e| ExtensionError::Plan(e.to_string()))?;
        if module.roles.contains(&"tool") {
            let tools = module
                .call("tool-api", "tools", &[])
                .await
                .map_err(|e| ExtensionError::Plan(e.to_string()))?;
            let Val::List(tools) = tools else {
                return Err(ExtensionError::Plan("tool list is not a list".into()));
            };
            for tool in tools {
                let Val::Record(fields) = tool else {
                    return Err(ExtensionError::Plan("invalid tool metadata".into()));
                };
                let string = |key: &str| {
                    fields.iter().find_map(|(name, value)| {
                        if name == key {
                            if let Val::String(value) = value {
                                Some(value.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                };
                let name = string("name")
                    .ok_or_else(|| ExtensionError::Plan("tool without name".into()))?;
                let parameters =
                    serde_json::from_str(&string("parameters-json-schema").unwrap_or_default())
                        .map_err(|e| ExtensionError::Plan(format!("invalid tool schema: {e}")))?;
                let required_permissions = fields
                    .iter()
                    .find_map(|(key, value)| {
                        if key == "required-permissions" {
                            if let Val::List(values) = value {
                                Some(
                                    values
                                        .iter()
                                        .filter_map(|v| {
                                            if let Val::String(s) = v {
                                                Some(s.clone())
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                )
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();
                let retry_safe = fields
                    .iter()
                    .any(|(key, value)| key == "retry-safe" && matches!(value, Val::Bool(true)));
                registrar.tool(Arc::new(ToolDefinition {
                    info: ToolInfo {
                        name: name.clone(),
                        description: string("description").unwrap_or_default(),
                        parameters,
                        retry_safe,
                        required_permissions,
                    },
                    executor: Arc::new(WasmTool {
                        _loader: Arc::clone(&loader),
                        module: Arc::clone(&module),
                        name,
                    }),
                }));
            }
        }
        adapters::mount(&module, &loader, registrar).await?;
        Ok(())
    }
}

struct HostState {
    wasi: WasiCtx,
    table: ResourceTable,
    limits: StoreLimits,
    state: BTreeMap<String, String>,
    max_state_entries: usize,
    max_state_value_bytes: usize,
    stdout: MemoryOutputPipe,
    stderr: MemoryOutputPipe,
    log_observer: LogObserver,
    extension_id: String,
    max_log_bytes: usize,
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl Loader {
    /// Creates a Component Model engine with asynchronous calls and epoch interruption.
    pub fn new() -> Result<Self, WasmError> {
        let mut config = Config::new();
        config.wasm_component_model(true).epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|error| WasmError::Engine(error.to_string()))?;
        let closed = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let drained = Arc::new(Notify::new());
        let ticker_engine = engine.clone();
        let ticker_closed = Arc::clone(&closed);
        let ticker_active = Arc::clone(&active);
        std::thread::spawn(move || {
            while !ticker_closed.load(Ordering::Acquire)
                || ticker_active.load(Ordering::Acquire) > 0
            {
                std::thread::sleep(Duration::from_millis(10));
                ticker_engine.increment_epoch();
            }
        });
        let log_observer: LogObserver = Arc::new(|extension, level, message| match level {
            "debug" => tracing::debug!(extension, "{message}"),
            "warn" => tracing::warn!(extension, "{message}"),
            "error" => tracing::error!(extension, "{message}"),
            _ => tracing::info!(extension, "{message}"),
        });
        Ok(Self {
            engine,
            closed,
            active,
            drained,
            close_drain_ms: AtomicU64::new(2_000),
            log_observer,
        })
    }

    #[must_use]
    pub fn with_log_observer(mut self, observer: LogObserver) -> Self {
        self.log_observer = observer;
        self
    }

    pub async fn load(&self, config: ModuleConfig) -> Result<Arc<LoadedModule>, WasmError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WasmError::Closed);
        }
        if config.name.is_empty()
            || config.limits.max_module_bytes == 0
            || config.limits.max_memory_bytes == 0
            || config.limits.max_input_bytes == 0
            || config.limits.max_output_bytes == 0
        {
            return Err(WasmError::Config("name and limits must be nonzero".into()));
        }
        if matches!(config.instance_mode, InstanceMode::Persistent) {
            return Err(WasmError::Config(
                "persistent instances are not supported".into(),
            ));
        }
        self.close_drain_ms.store(
            u64::try_from(config.limits.close_drain.as_millis()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
        let root = config
            .allowed_root
            .canonicalize()
            .map_err(|e| WasmError::Path(e.to_string()))?;
        let path = config
            .path
            .canonicalize()
            .map_err(|e| WasmError::Path(e.to_string()))?;
        if !path.starts_with(&root) || !path.is_file() {
            return Err(WasmError::Path(path.display().to_string()));
        }
        let size = std::fs::metadata(&path)
            .map_err(|e| WasmError::Path(e.to_string()))?
            .len();
        if size > config.limits.max_module_bytes as u64 {
            return Err(WasmError::Size);
        }
        let bytes = std::fs::read(&path).map_err(|e| WasmError::Path(e.to_string()))?;
        if bytes.len() > config.limits.max_module_bytes {
            return Err(WasmError::Size);
        }
        let actual = Sha256::digest(&bytes);
        // Digest comparison remains constant time over all bytes.
        if actual
            .iter()
            .zip(config.expected_sha256)
            .fold(0_u8, |diff, (a, b)| diff | (a ^ b))
            != 0
        {
            return Err(WasmError::Hash);
        }
        let component =
            Component::new(&self.engine, &bytes).map_err(|e| WasmError::Contract(e.to_string()))?;
        validate_imports(&self.engine, &component)?;
        let roles = detect_roles(&self.engine, &component)?;
        let module = Arc::new(LoadedModule {
            component,
            engine: self.engine.clone(),
            config,
            roles,
            admission: Arc::clone(&self.closed),
            active: Arc::clone(&self.active),
            drained: Arc::clone(&self.drained),
            serial: Mutex::new(()),
            log_observer: Arc::clone(&self.log_observer),
        });
        module.validate().await?;
        Ok(module)
    }

    fn stop_admission(&self) {
        self.closed.store(true, Ordering::Release);
        self.engine.increment_epoch();
    }

    pub async fn close(&self) {
        self.stop_admission();
        let drain = Duration::from_millis(self.close_drain_ms.load(Ordering::Acquire));
        let _ = tokio::time::timeout(drain, async {
            loop {
                let notified = self.drained.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.active.load(Ordering::Acquire) == 0 {
                    break;
                }
                notified.await;
            }
        })
        .await;
    }
}

impl Drop for Loader {
    fn drop(&mut self) {
        self.stop_admission();
    }
}

impl LoadedModule {
    async fn validate(&self) -> Result<(), WasmError> {
        let mut store = self.store();
        let instance = self.instance(&mut store).await?;
        let manifest = self
            .call_in_instance(&mut store, &instance, "manifest-api", "describe", &[])
            .await?;
        let Val::Record(fields) = manifest else {
            return Err(WasmError::Contract("invalid manifest".into()));
        };
        let field = |name: &str| {
            fields
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
        };
        let (Some(Val::String(id)), Some(Val::String(version)), Some(Val::List(roles))) =
            (field("id"), field("version"), field("roles"))
        else {
            return Err(WasmError::Contract("invalid manifest fields".into()));
        };
        if id.is_empty() || version.is_empty() || id != &self.config.name {
            return Err(WasmError::Contract("empty id or version".into()));
        }
        let mut declared = roles
            .iter()
            .map(|role| match role {
                Val::Enum(role) => Ok(role.as_str()),
                _ => Err(WasmError::Contract("invalid role".into())),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut detected = self.roles.clone();
        declared.sort_unstable();
        detected.sort_unstable();
        if declared != detected {
            return Err(WasmError::Contract("manifest role mismatch".into()));
        }
        let configured = self
            .call_in_instance(
                &mut store,
                &instance,
                "manifest-api",
                "configure",
                &[Val::String(self.config.config_json.clone())],
            )
            .await?;
        if !matches!(configured, Val::Result(Ok(None))) {
            return Err(WasmError::Contract("configuration rejected".into()));
        }
        Ok(())
    }

    fn linker(&self) -> Result<Linker<HostState>, WasmError> {
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|e| WasmError::Engine(e.to_string()))?;
        linker
            .instance("crabber:host/log@0.1.0")
            .map_err(|e| WasmError::Engine(e.to_string()))?
            .func_new_async("log", |store, _ty, args, _result| {
                Box::new(async move {
                    let (Some(Val::Enum(level)), Some(Val::String(message))) =
                        (args.first(), args.get(1))
                    else {
                        return Err(wasmtime::Error::msg("invalid log input"));
                    };
                    if message.len() > store.data().max_log_bytes {
                        return Err(wasmtime::Error::msg("log message exceeds limit"));
                    }
                    (store.data().log_observer)(&store.data().extension_id, level, message);
                    Ok(())
                })
            })
            .map_err(|e| WasmError::Engine(e.to_string()))?;
        {
            let mut state = linker
                .instance("crabber:host/state@0.1.0")
                .map_err(|e| WasmError::Engine(e.to_string()))?;
            state
                .func_new_async("get", |store, _ty, args, result| {
                    Box::new(async move {
                        let Some(Val::String(key)) = args.first() else {
                            return Err(wasmtime::Error::msg("invalid state key"));
                        };
                        result[0] = Val::Option(
                            store
                                .data()
                                .state
                                .get(key)
                                .cloned()
                                .map(|value| Box::new(Val::String(value))),
                        );
                        Ok(())
                    })
                })
                .map_err(|e| WasmError::Engine(e.to_string()))?;
            state
                .func_new_async("set", |mut store, _ty, args, result| {
                    Box::new(async move {
                        let (Some(Val::String(key)), Some(Val::String(value))) =
                            (args.first(), args.get(1))
                        else {
                            return Err(wasmtime::Error::msg("invalid state input"));
                        };
                        let state = store.data_mut();
                        let error = if key.len() > state.max_state_value_bytes
                            || value.len() > state.max_state_value_bytes
                            || (!state.state.contains_key(key)
                                && state.state.len() >= state.max_state_entries)
                        {
                            Some("state limit exceeded".to_owned())
                        } else {
                            state.state.insert(key.clone(), value.clone());
                            None
                        };
                        result[0] = Val::Result(match error {
                            Some(error) => Err(Some(Box::new(Val::String(error)))),
                            None => Ok(None),
                        });
                        Ok(())
                    })
                })
                .map_err(|e| WasmError::Engine(e.to_string()))?;
            state
                .func_new_async("delete", |mut store, _ty, args, _result| {
                    Box::new(async move {
                        let Some(Val::String(key)) = args.first() else {
                            return Err(wasmtime::Error::msg("invalid state key"));
                        };
                        store.data_mut().state.remove(key);
                        Ok(())
                    })
                })
                .map_err(|e| WasmError::Engine(e.to_string()))?;
        }
        Ok(linker)
    }

    fn store(&self) -> Store<HostState> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.config.limits.max_memory_bytes)
            .build();
        let stdout = MemoryOutputPipe::new(self.config.limits.max_output_bytes);
        let stderr = MemoryOutputPipe::new(self.config.limits.max_output_bytes);
        let mut wasi = WasiCtxBuilder::new();
        wasi.stdout(stdout.clone()).stderr(stderr.clone());
        let mut store = Store::new(
            &self.engine,
            HostState {
                wasi: wasi.build(),
                table: ResourceTable::new(),
                limits,
                state: BTreeMap::new(),
                max_state_entries: self.config.limits.max_state_entries,
                max_state_value_bytes: self.config.limits.max_state_value_bytes,
                stdout,
                stderr,
                log_observer: Arc::clone(&self.log_observer),
                extension_id: self.config.name.clone(),
                max_log_bytes: self.config.limits.max_output_bytes,
            },
        );
        store.limiter(|state| &mut state.limits);
        let admitted = Arc::clone(&self.admission);
        let started = Instant::now();
        let timeout = self.config.limits.call_timeout;
        store.epoch_deadline_callback(move |_| {
            if admitted.load(Ordering::Acquire) {
                return Err(wasmtime::Error::msg("loader closed"));
            }
            if started.elapsed() >= timeout {
                return Err(wasmtime::Error::msg("epoch deadline exceeded"));
            }
            Ok(UpdateDeadline::Yield(1))
        });
        store.set_epoch_deadline(1);
        store
    }

    async fn instance(&self, store: &mut Store<HostState>) -> Result<Instance, WasmError> {
        self.linker()?
            .instantiate_async(store, &self.component)
            .await
            .map_err(classify_trap)
    }

    pub async fn call(
        &self,
        interface: &str,
        function: &str,
        args: &[Val],
    ) -> Result<Val, WasmError> {
        if self.admission.load(Ordering::Acquire) {
            return Err(WasmError::Closed);
        }
        let _guard = self.serial.lock().await;
        self.active.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveCall {
            active: Arc::clone(&self.active),
            drained: Arc::clone(&self.drained),
        };
        if self.admission.load(Ordering::Acquire) {
            return Err(WasmError::Closed);
        }
        if args
            .iter()
            .any(|arg| val_bytes(arg) > self.config.limits.max_input_bytes)
        {
            return Err(WasmError::Size);
        }
        let sink = crabber_extension::current_state_sink();
        let prior = if let Some(sink) = &sink {
            sink.snapshot(&self.config.name)
                .await
                .map_err(WasmError::Engine)?
        } else {
            BTreeMap::new()
        };
        let mut store = self.store();
        store.data_mut().state = prior.clone();
        let instance = self.instance(&mut store).await?;
        let configured = self
            .call_in_instance(
                &mut store,
                &instance,
                "manifest-api",
                "configure",
                &[Val::String(self.config.config_json.clone())],
            )
            .await?;
        if !matches!(configured, Val::Result(Ok(None))) {
            return Err(WasmError::Contract("configuration rejected".into()));
        }
        let value = self
            .call_in_instance(&mut store, &instance, interface, function, args)
            .await;
        self.flush_logs(&store);
        let value = value?;
        if val_bytes(&value) > self.config.limits.max_output_bytes {
            return Err(WasmError::Size);
        }
        if let Some(sink) = sink
            && !matches!(&value, Val::Result(Err(_)))
            && !matches!(&value, Val::Variant(name, _) if name == "error")
        {
            let state = &store.data().state;
            let changes = prior
                .iter()
                .filter(|(key, _)| !state.contains_key(*key))
                .map(|(key, _)| (key.clone(), None))
                .chain(
                    state
                        .iter()
                        .filter(|(key, value)| prior.get(*key) != Some(*value))
                        .map(|(key, value)| (key.clone(), Some(value.clone()))),
                )
                .collect::<Vec<_>>();
            if !changes.is_empty() {
                sink.apply(&self.config.name, changes)
                    .await
                    .map_err(WasmError::Engine)?;
            }
        }
        Ok(value)
    }

    fn flush_logs(&self, store: &Store<HostState>) {
        for (level, pipe) in [
            ("info", &store.data().stdout),
            ("warn", &store.data().stderr),
        ] {
            let contents = pipe.contents();
            if !contents.is_empty() {
                let message = String::from_utf8_lossy(&contents);
                (self.log_observer)(&self.config.name, level, &message);
            }
        }
    }

    async fn call_in_instance(
        &self,
        store: &mut Store<HostState>,
        instance: &Instance,
        interface: &str,
        function: &str,
        args: &[Val],
    ) -> Result<Val, WasmError> {
        let name = format!("crabber:extensions/{interface}@0.1.0");
        let iface = instance
            .get_export_index(&mut *store, None, &name)
            .ok_or_else(|| WasmError::Contract(format!("missing {name}")))?;
        let func = instance
            .get_export_index(&mut *store, Some(&iface), function)
            .ok_or_else(|| WasmError::Contract(format!("missing {function}")))?;
        let func = instance
            .get_func(&mut *store, func)
            .ok_or_else(|| WasmError::Contract(format!("missing {function}")))?;
        let mut result = [Val::Bool(false)];
        func.call_async(store, args, &mut result)
            .await
            .map_err(classify_trap)?;
        Ok(result.into_iter().next().unwrap())
    }
}

fn val_bytes(value: &Val) -> usize {
    match value {
        Val::String(s) | Val::Enum(s) => s.len(),
        Val::List(values) | Val::Tuple(values) => values.iter().map(val_bytes).sum(),
        Val::Record(fields) => fields.iter().map(|(_, v)| val_bytes(v)).sum(),
        Val::Option(Some(v))
        | Val::Variant(_, Some(v))
        | Val::Result(Ok(Some(v)) | Err(Some(v))) => val_bytes(v),
        _ => 0,
    }
}

#[allow(clippy::needless_pass_by_value)]
fn classify_trap(error: wasmtime::Error) -> WasmError {
    let message = format!("{error:#}");
    if message.contains("loader closed") {
        WasmError::Closed
    } else if message.contains("interrupt") || message.contains("epoch deadline") {
        WasmError::Timeout
    } else {
        WasmError::Trap(message)
    }
}

fn validate_imports(engine: &Engine, component: &Component) -> Result<(), WasmError> {
    for (name, import) in component.component_type().imports(engine) {
        let has_functions = match &import.ty {
            ComponentItem::ComponentInstance(instance) => instance
                .exports(engine)
                .any(|(_, item)| matches!(item.ty, ComponentItem::ComponentFunc(_))),
            ComponentItem::Type(_) => false,
            _ => true,
        };
        let approved_function_interface = name.starts_with("crabber:host/log@")
            || name.starts_with("crabber:host/state@")
            || name.starts_with("wasi:io/")
            || name.starts_with("wasi:clocks/")
            || name.starts_with("wasi:random/")
            || name.starts_with("wasi:cli/");
        let approved_type_interface = name.starts_with("crabber:extensions/") && !has_functions;
        if !approved_function_interface && !approved_type_interface {
            return Err(WasmError::Contract(format!("forbidden import {name}")));
        }
    }
    Ok(())
}

const ROLES: &[(&str, &str)] = &[
    ("tool-api", "tool"),
    ("permissions-policy-api", "permissions-policy"),
    ("context-source-api", "context-source"),
    ("prompt-section-api", "prompt-section"),
    ("event-sink-api", "event-sink"),
    ("hook-api", "hook"),
    ("tool-middleware-api", "tool-middleware"),
    ("model-controls-api", "model-controls"),
];

fn detect_roles(engine: &Engine, component: &Component) -> Result<Vec<&'static str>, WasmError> {
    let exports = component
        .component_type()
        .exports(engine)
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<_>>();
    if !exports
        .iter()
        .any(|name| name.starts_with("crabber:extensions/manifest-api@"))
    {
        return Err(WasmError::Contract("missing manifest-api export".into()));
    }
    Ok(ROLES
        .iter()
        .filter(|(api, _)| {
            exports
                .iter()
                .any(|name| name.starts_with(&format!("crabber:extensions/{api}@")))
        })
        .map(|(_, role)| *role)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_extension::StateSink;

    fn fixture(name: &str) -> ModuleConfig {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/wasm/target/wasm32-wasip2/release");
        let path = root.join(format!("{}.wasm", name.replace('-', "_")));
        let bytes = std::fs::read(&path).expect("run cargo xtask build-fixtures first");
        ModuleConfig {
            name: name.into(),
            path,
            allowed_root: root,
            expected_sha256: Sha256::digest(bytes).into(),
            config_json: "{}".into(),
            limits: Limits::default(),
            instance_mode: InstanceMode::PerCall,
        }
    }

    #[tokio::test]
    async fn loads_and_calls_echo() {
        let loader = Loader::new().unwrap();
        let module = loader.load(fixture("echo-tool")).await.unwrap();
        assert_eq!(module.roles, ["tool"]);
        let tools = module.call("tool-api", "tools", &[]).await.unwrap();
        assert!(matches!(tools, Val::List(items) if items.len() == 1));
    }

    #[tokio::test]
    async fn guest_log_and_stdout_reach_observer() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let log_callback: LogObserver = {
            let observed = Arc::clone(&observed);
            Arc::new(move |_extension, level, message| {
                observed
                    .lock()
                    .unwrap()
                    .push((level.to_owned(), message.to_owned()));
            })
        };
        let loader = Loader::new().unwrap().with_log_observer(log_callback);
        let module = loader.load(fixture("echo-tool")).await.unwrap();
        module
            .call(
                "tool-api",
                "execute",
                &[
                    Val::String("echo".into()),
                    Val::String(String::new()),
                    Val::String("{}".into()),
                    turn_metadata(None),
                ],
            )
            .await
            .unwrap();
        let observed = observed.lock().unwrap();
        assert!(
            observed
                .iter()
                .any(|(_, message)| message.contains("echo-called"))
        );
        assert!(
            observed
                .iter()
                .any(|(_, message)| message.contains("echo-stdout"))
        );
    }

    #[tokio::test]
    async fn notify_failures_reach_observer() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let callback: LogObserver = {
            let observed = Arc::clone(&observed);
            Arc::new(move |_, level, message| {
                observed
                    .lock()
                    .unwrap()
                    .push((level.to_owned(), message.to_owned()));
            })
        };
        let loader = Loader::new().unwrap().with_log_observer(callback);
        let module = loader.load(fixture("all-in-one")).await.unwrap();
        let event = Val::Record(vec![
            ("kind".into(), Val::String("fail".into())),
            ("session-id".into(), Val::String(String::new())),
            ("run-id".into(), Val::String(String::new())),
            ("turn-id".into(), Val::String(String::new())),
            ("message-id".into(), Val::String(String::new())),
            ("tool-call-id".into(), Val::String(String::new())),
            ("epoch-id".into(), Val::String(String::new())),
            ("timestamp-unix-millis".into(), Val::S64(0)),
            ("payload-summary".into(), Val::String(String::new())),
        ]);
        let event_result = module.call("event-sink-api", "emit", &[event]).await;
        assert!(adapters::report_notify(&module, "event-sink", event_result).is_err());
        let hook_result = module
            .call(
                "hook-api",
                "before-turn",
                &[turn_metadata_from_projection(
                    &serde_json::json!({"run_id":"fail"}),
                )],
            )
            .await;
        assert!(adapters::report_notify(&module, "before-turn", hook_result).is_err());
        let observed = observed.lock().unwrap();
        assert_eq!(observed.len(), 2);
        assert!(observed.iter().all(|(level, _)| level == "error"));
        assert!(observed[0].1.contains("event failure"));
        assert!(observed[1].1.contains("hook failure"));
    }

    #[tokio::test]
    async fn rejects_wrong_hash_and_path() {
        let loader = Loader::new().unwrap();
        let mut config = fixture("echo-tool");
        config.expected_sha256 = [0; 32];
        assert!(matches!(loader.load(config).await, Err(WasmError::Hash)));
        let mut config = fixture("echo-tool");
        config.allowed_root = PathBuf::from("/tmp");
        assert!(matches!(loader.load(config).await, Err(WasmError::Path(_))));
        let mut config = fixture("echo-tool");
        config.limits.max_module_bytes = 1;
        assert!(matches!(loader.load(config).await, Err(WasmError::Size)));
    }

    #[tokio::test]
    async fn rejects_invalid_contracts_and_filesystem_import() {
        let loader = Loader::new().unwrap();
        for name in ["missing-manifest", "role-mismatch", "filesystem-import"] {
            let result = loader.load(fixture(name)).await;
            assert!(
                matches!(result, Err(WasmError::Contract(_))),
                "{name} loaded unexpectedly"
            );
        }
    }

    #[tokio::test]
    async fn mounts_echo_through_native_registry() {
        use crabber_extension::{Registry, Scope};
        let registry = Registry::new();
        let _mount = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("echo-tool"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&crabber_core::SessionId::new());
        assert_eq!(plan.tools.len(), 1);
        assert_eq!(
            plan.tools[0]
                .executor
                .execute(serde_json::json!({"hello":"wasm"}))
                .await
                .unwrap(),
            serde_json::json!({"hello":"wasm"})
        );
    }

    #[tokio::test]
    async fn slow_guest_times_out() {
        let loader = Loader::new().unwrap();
        let mut config = fixture("slow-tool");
        config.limits.call_timeout = Duration::from_millis(100);
        let module = loader.load(config).await.unwrap();
        let started = std::time::Instant::now();
        let result = module
            .call(
                "tool-api",
                "execute",
                &[
                    Val::String("slow-tool".into()),
                    Val::String(String::new()),
                    Val::String("{}".into()),
                    turn_metadata(None),
                ],
            )
            .await;
        assert!(matches!(result, Err(WasmError::Timeout)), "{result:?}");
        assert!(started.elapsed() <= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn close_interrupts_and_drains_active_guest() {
        let loader = Loader::new().unwrap();
        let mut config = fixture("slow-tool");
        config.limits.call_timeout = Duration::from_secs(5);
        config.limits.close_drain = Duration::from_millis(300);
        let module = loader.load(config).await.unwrap();
        let call = tokio::spawn(async move {
            module
                .call(
                    "tool-api",
                    "execute",
                    &[
                        Val::String("slow-tool".into()),
                        Val::String(String::new()),
                        Val::String("{}".into()),
                        turn_metadata(None),
                    ],
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let started = Instant::now();
        loader.close().await;
        assert!(started.elapsed() < Duration::from_millis(300));
        assert!(matches!(call.await.unwrap(), Err(WasmError::Closed)));
    }

    #[tokio::test]
    async fn hungry_guest_is_bounded() {
        let loader = Loader::new().unwrap();
        let mut config = fixture("hungry-tool");
        config.limits.max_memory_bytes = 16 << 20;
        let module = loader.load(config).await.unwrap();
        let result = module
            .call(
                "tool-api",
                "execute",
                &[
                    Val::String("hungry-tool".into()),
                    Val::String(String::new()),
                    Val::String("{}".into()),
                    turn_metadata(None),
                ],
            )
            .await;
        assert!(
            matches!(result, Err(WasmError::Trap(_) | WasmError::Size)),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn guest_exit_is_a_trap() {
        let loader = Loader::new().unwrap();
        let module = loader.load(fixture("exit-tool")).await.unwrap();
        let result = module
            .call(
                "tool-api",
                "execute",
                &[
                    Val::String("echo".into()),
                    Val::String(String::new()),
                    Val::String("{}".into()),
                    turn_metadata(None),
                ],
            )
            .await;
        assert!(matches!(result, Err(WasmError::Trap(_))), "{result:?}");
    }

    #[tokio::test]
    async fn native_adapters_dispatch_guest_roles() {
        use crabber_extension::{ContextAssemble, Registry, Scope, ToolResultTransform};
        let registry = Registry::new();
        let _deny = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("deny-policy"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let _banner = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("banner-context"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let _redact = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("redact-middleware"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&crabber_core::SessionId::new());
        assert_eq!(
            plan.guards[0].check("dangerous", &Value::Null),
            crabber_extension::GuardDecision::Deny
        );
        assert_eq!(
            plan.guards[0].check("ask_me", &Value::Null),
            crabber_extension::GuardDecision::Ask
        );
        assert_eq!(
            plan.guards[0].check("safe", &Value::Null),
            crabber_extension::GuardDecision::Allow
        );
        let context = plan
            .dispatcher
            .transform::<ContextAssemble>(serde_json::json!({"system_prelude":[],"user_suffix":[]}))
            .await
            .unwrap();
        assert!(
            context["system_prelude"][0]
                .as_str()
                .unwrap()
                .contains("banner")
        );
        let result = plan
            .dispatcher
            .transform::<ToolResultTransform>(
                serde_json::json!({"result":{"secret":"secret"},"is_error":false}),
            )
            .await
            .unwrap();
        assert!(result.to_string().contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn every_fixture_passes_import_allowlist() {
        let loader = Loader::new().unwrap();
        for name in [
            "echo-tool",
            "deny-policy",
            "banner-context",
            "redact-middleware",
            "counter-sink",
            "all-in-one",
            "tool-and-sink",
            "slow-tool",
            "hungry-tool",
        ] {
            let module = loader
                .load(fixture(name))
                .await
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            if name == "all-in-one" {
                assert_eq!(module.roles.len(), 8);
            }
            if name == "tool-and-sink" {
                assert_eq!(module.roles.len(), 2);
            }
        }
    }

    struct TestSink {
        session: crabber_core::SessionId,
        state: Mutex<BTreeMap<String, String>>,
    }

    #[async_trait]
    impl StateSink for TestSink {
        fn session_id(&self) -> &crabber_core::SessionId {
            &self.session
        }
        async fn snapshot(&self, _extension_id: &str) -> Result<BTreeMap<String, String>, String> {
            Ok(self.state.lock().await.clone())
        }
        async fn apply(
            &self,
            _extension_id: &str,
            changes: Vec<(String, Option<String>)>,
        ) -> Result<(), String> {
            let mut state = self.state.lock().await;
            for (key, value) in changes {
                if let Some(value) = value {
                    state.insert(key, value);
                } else {
                    state.remove(&key);
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn concurrent_sessions_serialize_without_state_leak() {
        let loader = Loader::new().unwrap();
        let module = loader.load(fixture("counter-sink")).await.unwrap();
        let event = Val::Record(vec![
            ("kind".into(), Val::String("test".into())),
            ("session-id".into(), Val::String(String::new())),
            ("run-id".into(), Val::String(String::new())),
            ("turn-id".into(), Val::String(String::new())),
            ("message-id".into(), Val::String(String::new())),
            ("tool-call-id".into(), Val::String(String::new())),
            ("epoch-id".into(), Val::String(String::new())),
            ("timestamp-unix-millis".into(), Val::S64(0)),
            ("payload-summary".into(), Val::String(String::new())),
        ]);
        let sinks = (0..2)
            .map(|_| {
                Arc::new(TestSink {
                    session: crabber_core::SessionId::new(),
                    state: Mutex::new(BTreeMap::new()),
                })
            })
            .collect::<Vec<_>>();
        let mut handles = Vec::new();
        for sink in &sinks {
            let module = Arc::clone(&module);
            let sink: Arc<dyn StateSink> = Arc::clone(sink) as Arc<dyn StateSink>;
            let event = event.clone();
            handles.push(tokio::spawn(crabber_extension::with_state_sink(
                sink,
                async move {
                    for _ in 0..100 {
                        let value = module
                            .call("event-sink-api", "emit", std::slice::from_ref(&event))
                            .await
                            .unwrap();
                        assert!(matches!(value, Val::Result(Ok(None))));
                    }
                },
            )));
        }
        for handle in handles {
            handle.await.unwrap();
        }
        for sink in sinks {
            assert_eq!(
                sink.state.lock().await.get("count").map(String::as_str),
                Some("100")
            );
        }
    }
}
