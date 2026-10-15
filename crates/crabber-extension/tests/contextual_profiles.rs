mod contextual_support;
use async_trait::async_trait;
use contextual_support::{Factory, Observed, context};
use crabber_core::{SessionContext, SessionId};
use crabber_extension::{
    ComponentIdentity, ContextualPlanProvider, Extension, ExtensionError, ExtensionFactory,
    FactoryContext, FactoryScope, Registrar, Registry, RunPlan, RunPlanProvider, Scope,
    StaticPlanProvider,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Shared {
    configuration: AtomicUsize,
    reads: AtomicUsize,
    installed: AtomicUsize,
}
#[async_trait]
impl Extension for Shared {
    fn id(&self) -> &'static str {
        "shared-probe"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.configuration.load(Ordering::SeqCst).to_string()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.installed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn inspection_uses_frozen_declarations_without_creating_owners_even_after_close() {
    let shared = Arc::new(Shared {
        configuration: AtomicUsize::new(1),
        reads: AtomicUsize::new(0),
        installed: AtomicUsize::new(0),
    });
    let observed = Arc::new(Observed::default());
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        vec![(shared.clone(), Scope::Global)],
        vec![Arc::new(Factory {
            name: "counter",
            scope: FactoryScope::Session,
            observed: observed.clone(),
        })],
        Duration::from_secs(1),
    )
    .unwrap();
    let target = context("target", "left");
    let original = provider.registration_fingerprint(&target).unwrap();
    shared.configuration.store(2, Ordering::SeqCst);
    assert_eq!(
        provider.registration_fingerprint(&target).unwrap(),
        original
    );
    assert_eq!(shared.reads.load(Ordering::SeqCst), 1);
    assert_eq!(shared.installed.load(Ordering::SeqCst), 0);
    assert_eq!(observed.created.load(Ordering::SeqCst), 0);
    let unrelated = context("other", "right");
    drop(provider.acquire_plan_in(&unrelated).await.unwrap());
    assert_eq!(shared.installed.load(Ordering::SeqCst), 1);
    assert_eq!(observed.created.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.registration_fingerprint(&target).unwrap(),
        original
    );
    provider.close_all(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        provider.registration_fingerprint(&target).unwrap(),
        original
    );
    assert_eq!(shared.reads.load(Ordering::SeqCst), 1);
    assert!(provider.acquire_plan_in(&target).await.is_err());
}
struct Declaration {
    version: &'static str,
    config: &'static str,
    scope: FactoryScope,
}
#[async_trait]
impl ExtensionFactory for Declaration {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "declared".into(),
            version: self.version.into(),
        }
    }
    fn config_hash(&self) -> String {
        self.config.into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    async fn create(&self, _: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        panic!("declaration inspection must never instantiate")
    }
}
#[test]
fn inspectable_versions_configuration_and_scope_change_the_frozen_profile() {
    let fingerprint = |version, config, scope| {
        ContextualPlanProvider::new(
            Registry::new(),
            vec![],
            vec![Arc::new(Declaration {
                version,
                config,
                scope,
            })],
            Duration::from_secs(1),
        )
        .unwrap()
        .registration_fingerprint(&context("target", "left"))
        .unwrap()
    };
    let original = fingerprint("1", "{\"a\":1,\"b\":2}", FactoryScope::Session);
    assert_eq!(
        original,
        fingerprint("1", "{\"b\":2,\"a\":1}", FactoryScope::Session)
    );
    assert_ne!(
        original,
        fingerprint("2", "{\"a\":1,\"b\":2}", FactoryScope::Session)
    );
    assert_ne!(
        original,
        fingerprint("1", "{\"a\":2,\"b\":2}", FactoryScope::Session)
    );
    assert_ne!(
        original,
        fingerprint("1", "{\"a\":1,\"b\":2}", FactoryScope::Workspace)
    );
}
struct Mounted(&'static str);
#[async_trait]
impl Extension for Mounted {
    fn id(&self) -> &'static str {
        self.0
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "mounted-v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
#[tokio::test]
async fn preexisting_scoped_registrations_ignore_unrelated_order_and_hold_no_lease() {
    let mut fingerprints = vec![];
    for unrelated_first in [false, true] {
        let registry = Registry::new();
        let entries = if unrelated_first {
            ["unrelated", "target"]
        } else {
            ["target", "unrelated"]
        };
        let mut handles = vec![];
        for name in entries {
            handles.push(
                registry
                    .mount(Arc::new(Mounted(name)), Scope::Session(name.into()))
                    .await
                    .unwrap(),
            );
        }
        let provider =
            ContextualPlanProvider::new(registry.clone(), vec![], vec![], Duration::from_secs(1))
                .unwrap();
        let original = provider
            .registration_fingerprint(&context("target", "left"))
            .unwrap();
        for handle in handles {
            handle.close().await.unwrap();
        }
        assert_eq!(
            provider
                .registration_fingerprint(&context("target", "left"))
                .unwrap(),
            original
        );
        fingerprints.push(original);
        provider.close_all(Duration::from_secs(1)).await.unwrap();
    }
    assert_eq!(fingerprints[0], fingerprints[1]);
}
struct Uninspectable;
#[async_trait]
impl RunPlanProvider for Uninspectable {
    async fn acquire_plan(&self, _: &SessionId) -> Result<RunPlan, ExtensionError> {
        panic!("inspection cannot acquire a fallback plan")
    }
}
#[test]
fn custom_providers_fail_explicitly_and_static_profiles_track_prompt_data() {
    assert_eq!(
        Uninspectable
            .registration_fingerprint(&context("target", "left"))
            .unwrap_err(),
        ExtensionError::RegistrationDescriptorUnavailable
    );
    let fingerprint = |text: &str| {
        StaticPlanProvider::new(
            vec![],
            vec![Arc::new(crabber_extension::PromptSection {
                name: "prompt".into(),
                order: 0,
                text: text.into(),
            })],
        )
        .registration_fingerprint(&SessionContext::new(
            "target".into(),
            "left",
            "/admitted/left",
        ))
        .unwrap()
    };
    assert_eq!(fingerprint("first"), fingerprint("first"));
    assert_ne!(fingerprint("first"), fingerprint("changed"));
}

struct Never;
#[async_trait]
impl crabber_extension::JsonToolExecutor for Never {
    async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
        panic!("profile inspection cannot execute a tool");
    }
}
fn tool(name: &str, parameters: serde_json::Value) -> Arc<crabber_extension::ToolDefinition> {
    Arc::new(crabber_extension::ToolDefinition {
        info: crabber_core::ToolInfo {
            name: name.into(),
            description: "declared".into(),
            parameters,
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(Never),
    })
}
#[test]
fn static_profiles_bind_visible_tool_schema_and_declaration_order() {
    let context = context("target", "left");
    let first = tool(
        "first",
        serde_json::json!({"type":"object","properties":{"a":{"type":"string"},"b":{"type":"number"}}}),
    );
    let reordered = tool(
        "first",
        serde_json::json!({"properties":{"b":{"type":"number"},"a":{"type":"string"}},"type":"object"}),
    );
    let changed = tool("first", serde_json::json!({"type":"string"}));
    let second = tool("second", serde_json::json!({"type":"object"}));
    let fingerprint = |tools| {
        StaticPlanProvider::new(tools, vec![])
            .registration_fingerprint(&context)
            .unwrap()
    };
    let original = fingerprint(vec![first.clone(), second.clone()]);
    assert_eq!(original, fingerprint(vec![reordered, second.clone()]));
    assert_ne!(original, fingerprint(vec![changed, second.clone()]));
    assert_ne!(original, fingerprint(vec![second, first]));
}
