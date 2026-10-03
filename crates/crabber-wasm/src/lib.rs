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
use tokio_util::sync::CancellationToken;
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
        self.active.fetch_add(1, Ordering::AcqRel);
        let _active = ActiveCall {
            active: Arc::clone(&self.active),
            drained: Arc::clone(&self.drained),
        };
        if self.closed.load(Ordering::Acquire) {
            return Err(WasmError::Closed);
        }
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
        let mut store = self.store(&CancellationToken::new());
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

    fn store(&self, cancellation: &CancellationToken) -> Store<HostState> {
        let limits = guest_store_limits(&self.config.limits);
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
        let cancellation = cancellation.clone();
        store.epoch_deadline_callback(move |_| {
            if cancellation.is_cancelled() {
                return Err(wasmtime::Error::msg("guest call cancelled"));
            }
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
        self.call_cancellable(interface, function, args, &CancellationToken::new())
            .await
    }

    /// Calls an ordinary guest with token-driven epoch interruption.
    pub async fn call_cancellable(
        &self,
        interface: &str,
        function: &str,
        args: &[Val],
        cancellation: &CancellationToken,
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
        let mut store = self.store(cancellation);
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

fn guest_store_limits(limits: &Limits) -> StoreLimits {
    StoreLimitsBuilder::new()
        .memory_size(limits.max_memory_bytes)
        .memories(1)
        .tables(4)
        .table_elements(limits.max_memory_bytes / 64)
        .instances(64)
        .build()
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

    /// WASM guests do not receive workspace context: the guest-visible record
    /// is pinned field for field, and `workspace-id` stays the empty string
    /// whatever the native `ToolContext` or `ContextAssemble` payload carries.
    /// The expected strings follow wasmtime's `Val` `Debug` format (wasmtime is
    /// pinned at `=49.0.1`); regenerate them, keeping every value, on a bump.
    #[test]
    fn guest_turn_metadata_record_is_pinned() {
        fn record(run: &str, session: &str, provider: &str, turn: u32, system: bool) -> String {
            format!(
                "Record([(\"run-id\", String({run:?})), (\"session-id\", String({session:?})), \
                 (\"epoch-id\", String(\"\")), (\"turn-index\", U32({turn})), \
                 (\"agent-name\", String(\"\")), (\"agent-mode\", String(\"\")), \
                 (\"provider-id\", String({provider:?})), (\"model-id\", String({provider:?})), \
                 (\"tool-names\", List([])), (\"message-count\", U32({turn})), \
                 (\"role-counts\", Record([(\"system\", U32(0)), (\"user\", U32(0)), \
                 (\"assistant\", U32(0)), (\"tool\", U32(0))])), \
                 (\"has-system-prompt\", Bool({system})), (\"workspace-id\", String(\"\"))])"
            )
        }
        assert_eq!(
            format!("{:?}", turn_metadata(None)),
            record("", "", "", 0, false)
        );
        let context = ToolContext::new(
            crabber_core::SessionId::new(),
            crabber_core::RunId::new(),
            crabber_core::ToolCallId::new(),
            tokio_util::sync::CancellationToken::new(),
            crabber_extension::HostServices::default(),
            crabber_extension::WorkspaceContext::from_persisted("native-ws", "/native/root"),
            Arc::new(|_| {}),
            None,
        );
        assert_eq!(
            format!("{:?}", turn_metadata(Some(&context))),
            record(
                &context.run_id.to_string(),
                &context.session_id.to_string(),
                "",
                0,
                false
            )
        );
        let projection = serde_json::json!({
            "run_id": "run", "session_id": "session", "provider_id": "p", "model_id": "p",
            "turn_index": 3, "message_count": 3, "has_system_prompt": true,
            "workspace_id": "native-ws", "workspace_directory": "/native/root",
        });
        assert_eq!(
            format!("{:?}", turn_metadata_from_projection(&projection)),
            record("run", "session", "p", 3, true)
        );
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

    async fn spinning_module() -> (Arc<Loader>, Arc<LoadedModule>, Arc<tokio::sync::Semaphore>) {
        let ready = Arc::new(tokio::sync::Semaphore::new(0));
        let observed = ready.clone();
        let loader = Arc::new(Loader::new().unwrap().with_log_observer(Arc::new(
            move |_, _, message| {
                if message == "spin-ready" {
                    observed.add_permits(1);
                }
            },
        )));
        let mut config = fixture("spinning-middleware");
        config.limits.call_timeout = Duration::from_secs(10);
        let module = loader.load(config).await.unwrap();
        (loader, module, ready)
    }

    fn spinning_args() -> Vec<Val> {
        vec![
            Val::String("echo-tool".into()),
            Val::String(String::new()),
            Val::String("{}".into()),
            Val::String("SECRET-GUEST-PAYLOAD".into()),
            Val::Bool(false),
            turn_metadata(None),
        ]
    }

    #[tokio::test]
    async fn token_interrupts_active_guest_and_releases_serial_lock() {
        use crabber_runtime::INTERRUPT_SETTLEMENT_BOUND;
        let (loader, module, ready) = spinning_module().await;
        let token = CancellationToken::new();
        let call = tokio::spawn({
            let module = module.clone();
            let token = token.clone();
            async move {
                module
                    .call_cancellable(
                        "tool-middleware-api",
                        "after-tool-call",
                        &spinning_args(),
                        &token,
                    )
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), ready.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let started = Instant::now();
        token.cancel();
        let result = tokio::time::timeout(INTERRUPT_SETTLEMENT_BOUND, call)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, Err(WasmError::Trap(ref text)) if text.contains("guest call cancelled")),
            "{result:?}"
        );
        assert!(started.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
        assert_eq!(module.active.load(Ordering::Acquire), 0);
        tokio::time::timeout(
            INTERRUPT_SETTLEMENT_BOUND,
            module.call("manifest-api", "describe", &[]),
        )
        .await
        .unwrap()
        .unwrap();
        loader.close().await;
    }

    struct InterruptMiddleware {
        loader: Arc<Loader>,
        module: Arc<LoadedModule>,
        protected: bool,
    }
    #[async_trait]
    impl Extension for InterruptMiddleware {
        fn id(&self) -> &'static str {
            "interrupt-middleware"
        }
        fn version(&self) -> &'static str {
            "0.1.0"
        }
        fn config_hash(&self) -> String {
            "test".into()
        }
        async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
            adapters::mount(&self.module, &self.loader, registrar).await?;
            if self.protected {
                registrar.on_result_transform(
                    -1,
                    "accept",
                    Arc::new(|_, value| {
                        Box::pin(async move { Ok(crabber_extension::TransformOutput::new(value)) })
                    }),
                );
                registrar.on_final_redaction(
                    -100,
                    "protect",
                    Arc::new(|context, _| {
                        Box::pin(async move {
                            assert!(context.cancellation().is_cancelled());
                            Ok(crabber_extension::TransformOutput::new(serde_json::json!(
                                "protected"
                            )))
                        })
                    }),
                );
            }
            Ok(())
        }
    }

    async fn assert_wasm_interrupted_settlement(
        store: &crabber_session::MemoryStore,
        session_id: &crabber_core::SessionId,
        run_id: &crabber_core::RunId,
        call_id: &crabber_core::ToolCallId,
        expected: &str,
    ) {
        use crabber_core::{ContentBlock, ToolCallStatus, ToolResultStatus};
        use crabber_session::{SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store};
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: session_id.clone(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 1000,
                    text_bytes: 1 << 20,
                    encoded_bytes: 1 << 22,
                },
                continuation: None,
            })
            .await
            .unwrap()
        else {
            panic!("snapshot page");
        };
        let record = page
            .tool_calls
            .iter()
            .find(|record| &record.id == call_id)
            .unwrap();
        assert_eq!(record.status, ToolCallStatus::Interrupted);
        let result = record.result.as_ref().unwrap();
        assert_eq!(result.status, ToolResultStatus::Interrupted);
        assert_eq!(
            result.content,
            vec![ContentBlock::Text {
                text: expected.into()
            }]
        );
        assert_eq!(
            store.list_unfinished_tool_calls(run_id).await.unwrap(),
            [] as [crabber_core::ToolCallRecord; 0]
        );
        for message in &page.messages {
            for part in &message.parts {
                if let ContentBlock::ToolResult {
                    content, is_error, ..
                } = &part.content
                {
                    assert!(*is_error);
                    assert_eq!(*content, result.content);
                }
            }
        }
        let events = store.list_events(session_id, None, 1000).await.unwrap();
        let settled = events
            .iter()
            .find(|event| event.kind == crabber_core::EventKind::ToolCallSettled)
            .unwrap();
        assert_eq!(settled.payload["content"][0]["text"], expected);
        assert_eq!(settled.payload["is_error"], true);
    }

    #[tokio::test]
    async fn orchestrator_interrupts_active_wasm_with_fixed_or_final_redacted_settlement() {
        use crabber_core::{RunStatus, ToolCallId};
        use crabber_extension::{Registry, Scope};
        use crabber_providers::{FakeProvider, Selection, StreamDelta};
        use crabber_runtime::{
            INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT, Orchestrator, Request,
        };
        use crabber_session::MemoryStore;
        for protected in [false, true] {
            let (loader, module, ready) = spinning_module().await;
            let registry = Arc::new(Registry::new());
            registry
                .mount(
                    Arc::new(WasmExtension::new(fixture("echo-tool"))),
                    Scope::Global,
                )
                .await
                .unwrap();
            registry
                .mount(
                    Arc::new(InterruptMiddleware {
                        loader: loader.clone(),
                        module: module.clone(),
                        protected,
                    }),
                    Scope::Global,
                )
                .await
                .unwrap();
            let call_id = ToolCallId::new();
            let fake = FakeProvider::scripted(vec![vec![
                StreamDelta::ToolCallStart {
                    call_id: call_id.clone(),
                    name: "echo".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: call_id.clone(),
                    text: r#"{"text":"SECRET-ORIGINAL"}"#.into(),
                },
                StreamDelta::ToolCallDone {
                    call_id: call_id.clone(),
                },
                StreamDelta::Completed,
            ]]);
            let store = Arc::new(MemoryStore::new());
            let runtime = Orchestrator::builder()
                .store(store.clone())
                .resolver(Arc::new(fake))
                .plan_provider(registry.clone())
                .build()
                .unwrap();
            let handle = runtime
                .start(Request {
                    session_id: None,
                    workspace_id: "test".into(),
                    directory: ".".into(),
                    title: "test".into(),
                    text: "test".into(),
                    selection: Selection {
                        provider_id: "fake".into(),
                        model_id: "scripted".into(),
                    },
                    system_prompt: None,
                })
                .await
                .unwrap();
            let session_id = handle.session_id().clone();
            let run_id = handle.run_id().clone();
            tokio::time::timeout(Duration::from_secs(10), ready.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            let started = Instant::now();
            handle.interrupt();
            assert_eq!(
                tokio::time::timeout(INTERRUPT_SETTLEMENT_BOUND, handle.done())
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                RunStatus::Interrupted
            );
            assert!(started.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
            let expected = if protected {
                r#""protected""#
            } else {
                INTERRUPTED_RESULT_TEXT
            };
            assert_wasm_interrupted_settlement(&store, &session_id, &run_id, &call_id, expected)
                .await;
            assert_eq!(module.active.load(Ordering::Acquire), 0);
            assert!(module.serial.try_lock().is_ok());
            registry.close_all().await.unwrap();
            loader.close().await;
        }
    }

    #[tokio::test]
    async fn dropped_driver_releases_call_waiting_on_serial_mutex() {
        use crabber_extension::{ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput};
        let (loader, module, _) = spinning_module().await;
        let registry = crabber_extension::Registry::new();
        registry
            .mount(
                Arc::new(InterruptMiddleware {
                    loader: loader.clone(),
                    module: module.clone(),
                    protected: false,
                }),
                crabber_extension::Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&crabber_core::SessionId::new());
        let guard = module.serial.lock().await;
        let token = CancellationToken::new();
        let context = middleware_context(
            ToolInput::Normalized(serde_json::json!({})),
            ToolOutcomeClass::Succeeded,
        )
        .with_cancellation(token.clone());
        let mut invocation = Box::pin(
            plan.dispatcher
                .transform_tool_result(context, TransformOutput::new(serde_json::json!("secret"))),
        );
        std::future::poll_fn(|cx| {
            assert!(invocation.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        token.cancel();
        assert_eq!(
            invocation.await,
            ToolResultOutcome::Interrupted { redacted: None }
        );
        drop(guard);
        assert!(module.serial.try_lock().is_ok());
        assert_eq!(module.active.load(Ordering::Acquire), 0);
        drop(plan);
        registry.close_all().await.unwrap();
        loader.close().await;
    }

    struct BlockedHostMiddleware {
        module: Arc<LoadedModule>,
        entered: Arc<tokio::sync::Semaphore>,
        dropped: Arc<tokio::sync::Semaphore>,
    }

    struct HostImportGuard(Arc<tokio::sync::Semaphore>);
    impl Drop for HostImportGuard {
        fn drop(&mut self) {
            self.0.add_permits(1);
        }
    }

    #[async_trait]
    impl Extension for BlockedHostMiddleware {
        fn id(&self) -> &'static str {
            "blocked-host"
        }
        fn version(&self) -> &'static str {
            "0.1.0"
        }
        fn config_hash(&self) -> String {
            "test".into()
        }
        async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
            let module = self.module.clone();
            let entered = self.entered.clone();
            let dropped = self.dropped.clone();
            registrar.on_result_transform(
                0,
                "blocked-host",
                Arc::new(move |context, _| {
                    let module = module.clone();
                    let entered = entered.clone();
                    let dropped = dropped.clone();
                    Box::pin(async move {
                        let _serial = module.serial.lock().await;
                        let mut store = module.store(context.cancellation());
                        let mut linker = module.linker().unwrap();
                        linker
                            .allow_shadowing(true)
                            .instance("crabber:host/log@0.1.0")
                            .unwrap()
                            .func_new_async("log", move |_, _, _, _| {
                                let entered = entered.clone();
                                let dropped = dropped.clone();
                                Box::new(async move {
                                    let _guard = HostImportGuard(dropped);
                                    entered.add_permits(1);
                                    std::future::pending::<()>().await;
                                    Ok(())
                                })
                            })
                            .unwrap();
                        let instance = linker
                            .instantiate_async(&mut store, &module.component)
                            .await
                            .unwrap();
                        let value = module
                            .call_in_instance(
                                &mut store,
                                &instance,
                                "tool-middleware-api",
                                "after-tool-call",
                                &spinning_args(),
                            )
                            .await
                            .map_err(|error| ExtensionError::Tool(error.to_string()))?;
                        panic!("blocked host import returned: {value:?}");
                    })
                }),
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn dropped_driver_releases_guest_blocked_in_host_import() {
        use crabber_extension::{ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput};
        use crabber_runtime::INTERRUPT_SETTLEMENT_BOUND;
        let (loader, module, _) = spinning_module().await;
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let dropped = Arc::new(tokio::sync::Semaphore::new(0));
        let registry = crabber_extension::Registry::new();
        registry
            .mount(
                Arc::new(BlockedHostMiddleware {
                    module: module.clone(),
                    entered: entered.clone(),
                    dropped: dropped.clone(),
                }),
                crabber_extension::Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&crabber_core::SessionId::new());
        let token = CancellationToken::new();
        let context = middleware_context(
            ToolInput::Normalized(serde_json::json!({})),
            ToolOutcomeClass::Succeeded,
        )
        .with_cancellation(token.clone());
        let invocation = tokio::spawn(async move {
            let plan = plan;
            plan.dispatcher
                .transform_tool_result(context, TransformOutput::new(serde_json::json!("secret")))
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        token.cancel();
        assert_eq!(
            tokio::time::timeout(INTERRUPT_SETTLEMENT_BOUND, invocation)
                .await
                .unwrap()
                .unwrap(),
            ToolResultOutcome::Interrupted { redacted: None }
        );
        assert_eq!(dropped.available_permits(), 1);
        assert!(module.serial.try_lock().is_ok());
        registry.close_all().await.unwrap();
        loader.close().await;
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
    async fn close_interrupts_manifest_validation() {
        let loader = Arc::new(Loader::new().unwrap());
        let mut config = fixture("slow-tool");
        config.config_json = "hang-validation".into();
        config.limits.call_timeout = Duration::from_secs(5);
        config.limits.close_drain = Duration::from_millis(300);
        let loading = tokio::spawn({
            let loader = Arc::clone(&loader);
            async move { loader.load(config).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let started = Instant::now();
        loader.close().await;
        assert!(started.elapsed() < Duration::from_millis(300));
        assert!(matches!(loading.await.unwrap(), Err(WasmError::Closed)));
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

    #[test]
    fn multiple_memories_and_tables_cannot_bypass_store_budget() {
        let engine = Engine::default();
        let fixture_root =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/wasm-negative");
        for fixture_name in ["multi-memory.wat", "many-tables.wat"] {
            let wasm = wat::parse_file(fixture_root.join(fixture_name)).unwrap();
            let module = wasmtime::Module::new(&engine, wasm).unwrap();
            let limits = Limits {
                max_memory_bytes: 64 << 10,
                ..Limits::default()
            };
            let mut store = Store::new(&engine, guest_store_limits(&limits));
            store.limiter(|limits| limits);
            assert!(
                wasmtime::Instance::new(&mut store, &module, &[]).is_err(),
                "{fixture_name} exceeded budget"
            );
            let empty =
                wasmtime::Module::new(&engine, wat::parse_str("(module)").unwrap()).unwrap();
            wasmtime::Instance::new(&mut store, &empty, &[]).unwrap();
        }
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
        use crabber_extension::{ContextAssemble, Registry, Scope};
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
            .transform_tool_result(
                crabber_extension::ToolResultContext::new(
                    "secret-tool".into(),
                    true,
                    crabber_extension::ToolInput::Normalized(
                        serde_json::json!({"secret": "secret"}),
                    ),
                    crabber_core::ToolCallId::new(),
                    crabber_core::SessionId::new(),
                    crabber_core::RunId::new(),
                    crabber_extension::ToolOutcomeClass::Succeeded,
                ),
                crabber_extension::TransformOutput::new(serde_json::json!({"secret":"secret"})),
            )
            .await;
        let crabber_extension::ToolResultOutcome::Completed { result, .. } = result else {
            panic!("WASM redactor failed");
        };
        assert!(result.to_string().contains("[REDACTED]"));
    }

    // D9 retires the old characterize_* expectations of empty arguments and
    // result-only guest replies; these tests exercise the full envelope instead.
    async fn echo_middleware_plan() -> (
        crabber_extension::Registry,
        crabber_extension::MountHandle,
        crabber_extension::RunPlan,
    ) {
        use crabber_extension::{Registry, Scope};
        let registry = Registry::new();
        let mount = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("echo-middleware"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&crabber_core::SessionId::new());
        (registry, mount, plan)
    }

    fn middleware_context(
        input: crabber_extension::ToolInput,
        class: crabber_extension::ToolOutcomeClass,
    ) -> crabber_extension::ToolResultContext {
        crabber_extension::ToolResultContext::new(
            "secret-tool".into(),
            class != crabber_extension::ToolOutcomeClass::UnknownTool,
            input,
            crabber_core::ToolCallId::from("call-rtc"),
            crabber_core::SessionId::from("session-rtc"),
            crabber_core::RunId::from("run-rtc"),
            class,
        )
    }

    #[tokio::test]
    async fn wasm_after_tool_guest_receives_exact_context_and_arguments() {
        use crabber_extension::{
            InputUnavailable, ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput,
            result_envelope,
        };
        let (_registry, _mount, plan) = echo_middleware_plan().await;
        for (class, input) in [
            (
                ToolOutcomeClass::Succeeded,
                ToolInput::Normalized(serde_json::json!({"secret": [1, null]})),
            ),
            (
                ToolOutcomeClass::ExecutionFailed,
                ToolInput::Normalized(Value::Null),
            ),
            (
                ToolOutcomeClass::PermissionDenied,
                ToolInput::Normalized(serde_json::json!([1])),
            ),
            (
                ToolOutcomeClass::UnknownTool,
                ToolInput::Raw(serde_json::json!("provider text")),
            ),
            (
                ToolOutcomeClass::PrepareFailed,
                ToolInput::Unavailable {
                    reason: InputUnavailable::PrepareFailed,
                },
            ),
            (
                ToolOutcomeClass::UnknownTool,
                ToolInput::Unavailable {
                    reason: InputUnavailable::Unresolved,
                },
            ),
        ] {
            let context = middleware_context(input, class);
            let expected = result_envelope(&context, serde_json::json!({"a": 1}));
            let outcome = plan
                .dispatcher
                .transform_tool_result(context, TransformOutput::new(expected["result"].clone()))
                .await;
            let ToolResultOutcome::Completed { result, is_error } = outcome else {
                panic!("{outcome:?}");
            };
            assert_eq!(result["tool_name"], "secret-tool");
            assert_eq!(result["tool_call_id"], "call-rtc");
            assert_eq!(result["executed_input"], expected["context"]["input"]);
            assert_eq!(
                serde_json::from_str::<Value>(result["output_json"].as_str().unwrap()).unwrap(),
                expected
            );
            assert_eq!(result["is_error"], class.is_error());
            assert_eq!(is_error, class.is_error());
            assert_eq!(
                result["turn"],
                serde_json::json!({
                    "session_id":"", "run_id":"", "epoch_id":"", "turn_index":0,
                    "agent_name":"", "agent_mode":"", "provider_id":"", "model_id":"",
                    "tool_names":[], "message_count":0,
                    "role_counts":{"system":0,"user":0,"assistant":0,"tool":0},
                    "has_system_prompt":false, "workspace_id":""
                })
            );
        }
    }

    #[tokio::test]
    async fn wasm_after_tool_unchanged_and_mark_error_preserve_escalation() {
        use crabber_extension::{ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput};
        let (_registry, _mount, plan) = echo_middleware_plan().await;
        for class in [
            ToolOutcomeClass::Succeeded,
            ToolOutcomeClass::ExecutionFailed,
        ] {
            for marked in [false, true] {
                let result = serde_json::json!("__unchanged__");
                let outcome = plan
                    .dispatcher
                    .transform_tool_result(
                        middleware_context(ToolInput::Normalized(Value::Null), class),
                        TransformOutput {
                            result: result.clone(),
                            mark_error: marked,
                        },
                    )
                    .await;
                assert_eq!(
                    outcome,
                    ToolResultOutcome::Completed {
                        result,
                        is_error: class.is_error() || marked
                    }
                );
            }
        }
        let outcome = plan
            .dispatcher
            .transform_tool_result(
                middleware_context(
                    ToolInput::Normalized(Value::Null),
                    ToolOutcomeClass::Succeeded,
                ),
                TransformOutput::new(serde_json::json!("__mark_error__")),
            )
            .await;
        assert!(matches!(
            outcome,
            ToolResultOutcome::Completed { is_error: true, .. }
        ));
    }

    #[tokio::test]
    async fn wasm_after_tool_invalid_replies_are_sanitized_d2_failures() {
        use crabber_extension::{ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput};
        let (_registry, _mount, plan) = echo_middleware_plan().await;
        let mut markers = vec![
            "__error__".to_owned(),
            "__malformed__".into(),
            "__non_envelope__".into(),
            "__missing_mark_error__".into(),
            "__extra_key__".into(),
            "__bad_mark_error__".into(),
        ];
        for field in [
            "tool_name",
            "resolved",
            "input",
            "call_id",
            "session_id",
            "run_id",
            "class",
            "is_error",
            "phase",
        ] {
            markers.push(format!("__tamper__{field}"));
        }
        for marker in markers {
            let outcome = plan
                .dispatcher
                .transform_tool_result(
                    middleware_context(
                        ToolInput::Normalized(Value::Null),
                        ToolOutcomeClass::Succeeded,
                    ),
                    TransformOutput::new(serde_json::json!(marker)),
                )
                .await;
            assert_eq!(
                outcome,
                ToolResultOutcome::Failed {
                    handler: "wasm-after-tool:echo-middleware".into()
                },
                "{marker}"
            );
        }
    }

    #[tokio::test]
    async fn wasm_after_tool_deep_input_decode_boundary_is_contained() {
        use crabber_extension::{ToolInput, ToolOutcomeClass, ToolResultOutcome, TransformOutput};
        let (_registry, _mount, plan) = echo_middleware_plan().await;
        for (depth, raw) in [(124, true), (126, false)] {
            let deep: Value =
                serde_json::from_str(&("[".repeat(depth) + &"]".repeat(depth))).unwrap();
            let arguments = if raw {
                serde_json::json!({"$crabber_unknown_tool": {"raw": deep}})
            } else {
                deep.clone()
            };
            let record = crabber_core::ToolCallRecord {
                id: crabber_core::ToolCallId::from("call-rtc"),
                run_id: crabber_core::RunId::from("run-rtc"),
                name: "secret-tool".into(),
                arguments,
                status: crabber_core::ToolCallStatus::Running,
                retry_safe: false,
                result: None,
            };
            assert_eq!(
                serde_json::from_str::<crabber_core::ToolCallRecord>(
                    &serde_json::to_string(&record).unwrap()
                )
                .unwrap(),
                record
            );
            let input = if raw {
                ToolInput::Raw(deep)
            } else {
                ToolInput::Normalized(deep)
            };
            let context = middleware_context(
                input,
                if raw {
                    ToolOutcomeClass::UnknownTool
                } else {
                    ToolOutcomeClass::Succeeded
                },
            );
            let envelope =
                crabber_extension::result_envelope(&context, serde_json::json!("SECRET-ORIGINAL"));
            assert_eq!(
                serde_json::from_str::<Value>(&envelope.to_string()).is_ok(),
                raw
            );
            assert!(
                serde_json::from_str::<Value>(&envelope["context"]["input"].to_string()).is_ok()
            );
            let outcome = plan
                .dispatcher
                .transform_tool_result(context, TransformOutput::new(envelope["result"].clone()))
                .await;
            if raw {
                let ToolResultOutcome::Completed { result, is_error } = outcome else {
                    panic!("depth-124 raw input should roundtrip: {outcome:?}");
                };
                assert!(is_error);
                assert_eq!(result["executed_input"], envelope["context"]["input"]);
                let mut settled = record;
                settled.status = crabber_core::ToolCallStatus::Failed;
                settled.result = Some(crabber_core::ToolResult {
                    status: crabber_core::ToolResultStatus::Failed,
                    content: vec![crabber_core::ContentBlock::Text {
                        text: serde_json::to_string(&result).unwrap(),
                    }],
                });
                assert_eq!(
                    serde_json::from_str::<crabber_core::ToolCallRecord>(
                        &serde_json::to_string(&settled).unwrap()
                    )
                    .unwrap(),
                    settled
                );
            } else {
                assert_eq!(
                    outcome,
                    ToolResultOutcome::Failed {
                        handler: "wasm-after-tool:echo-middleware".into()
                    }
                );
            }
        }
    }

    #[tokio::test]
    async fn policy_guard_does_not_commit_state_after_fence_loss() {
        use crabber_extension::{Registry, Scope};
        use std::sync::atomic::AtomicUsize;
        struct LostSink {
            session: crabber_core::SessionId,
            writes: AtomicUsize,
        }
        #[async_trait]
        impl StateSink for LostSink {
            fn session_id(&self) -> &crabber_core::SessionId {
                &self.session
            }
            async fn snapshot(&self, _: &str) -> Result<BTreeMap<String, String>, String> {
                Ok(BTreeMap::new())
            }
            async fn apply(&self, _: &str, _: Vec<(String, Option<String>)>) -> Result<(), String> {
                self.writes.fetch_add(1, Ordering::SeqCst);
                Err("lease lost".into())
            }
        }
        let registry = Registry::new();
        let _mount = registry
            .mount(
                Arc::new(WasmExtension::new(fixture("all-in-one"))),
                Scope::Global,
            )
            .await
            .unwrap();
        let session = crabber_core::SessionId::new();
        let plan = registry.acquire(&session);
        let sink = Arc::new(LostSink {
            session,
            writes: AtomicUsize::new(0),
        });
        let decision = crabber_extension::with_state_sink(sink.clone(), async {
            plan.guards[0].check("stateful", &Value::Null)
        })
        .await;
        assert_eq!(decision, crabber_extension::GuardDecision::Deny);
        assert_eq!(sink.writes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn every_fixture_passes_import_allowlist() {
        let loader = Loader::new().unwrap();
        for name in [
            "echo-tool",
            "deny-policy",
            "banner-context",
            "redact-middleware",
            "echo-middleware",
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
