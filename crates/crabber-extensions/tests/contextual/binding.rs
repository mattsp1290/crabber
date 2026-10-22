use super::{agent_support, fixture::*, json_output::JsonOutputExecutor, options};
use crabber::{
    core::{RunId, SessionId, ToolCallId},
    extension::{
        ComponentIdentity, Extension, ExtensionError, ExtensionFactory, FactoryContext,
        FactoryScope, HostServices, Registry, Scope, ToolContext, WorkspaceContext,
    },
};
use crabber_extensions::{background_jobs as jobs, python_repl as python};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct Capture {
    factory: Arc<dyn ExtensionFactory>,
    instance: Mutex<Option<Arc<dyn Extension>>>,
}
#[async_trait::async_trait]
impl ExtensionFactory for Capture {
    fn identity(&self) -> ComponentIdentity {
        self.factory.identity()
    }
    fn config_hash(&self) -> String {
        self.factory.config_hash()
    }
    fn scope(&self) -> FactoryScope {
        self.factory.scope()
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        let instance = self.factory.create(context).await?;
        *self.instance.lock().unwrap() = Some(instance.clone());
        Ok(instance)
    }
}
async fn forged_contexts_refuse(
    factory: Arc<dyn ExtensionFactory>,
    root: &std::path::Path,
    report: impl Fn() -> jobs::OwnerReport,
) {
    let capture = Arc::new(Capture {
        factory,
        instance: Mutex::new(None),
    });
    let agent = agent_support::builder()
        .memory()
        .provider(Arc::new(crabber::FakeProvider::scripted(vec![done()])))
        .config(crabber::AgentConfig::new(selection()))
        .extension_factory(capture.clone())
        .build()
        .unwrap();
    let ctx = context(root, "ws", "session");
    agent_support::first_prompt(&agent, &ctx, "admit")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let registry = Registry::new();
    let instance = capture.instance.lock().unwrap().clone().unwrap();
    let handle = registry.mount(instance, Scope::Global).await.unwrap();
    let plan = registry.acquire(&SessionId::from("session"));

    for tool in &plan.tools {
        for (session, workspace) in [("foreign", "ws"), ("session", "foreign"), ("session", "")] {
            let forged = ToolContext::new(
                session.into(),
                RunId::new(),
                ToolCallId::new(),
                CancellationToken::new(),
                HostServices::default(),
                WorkspaceContext::from_persisted(workspace, root.to_str().unwrap()),
                Arc::new(|_| {}),
            );
            let arguments = if tool.info.name == python::EXECUTE_TOOL {
                json!({"code":"1"})
            } else {
                json!({})
            };
            assert!(
                tool.executor
                    .execute_json_with_context(forged, arguments)
                    .await
                    .unwrap_err()
                    .to_string()
                    .ends_with("owner"),
                "{}",
                tool.info.name
            );
        }
    }
    assert_eq!(report().tracked, 0);
    drop(plan);
    handle.close().await.unwrap();
    assert!(
        agent
            .close(std::time::Duration::from_secs(5))
            .await
            .local_drained()
    );
}
#[tokio::test(flavor = "multi_thread")]
async fn bound_jobs_refuse_every_tool_for_forged_owners() {
    let root = tempfile::tempdir().unwrap();
    let factory =
        Arc::new(jobs::Factory::new(options::jobs(root.path()), FactoryScope::Session).unwrap());
    forged_contexts_refuse(factory.clone(), root.path(), || {
        factory
            .owner_report("ws", Some(&SessionId::from("session")))
            .unwrap()
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn bound_python_refuses_every_tool_for_forged_owners() {
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("temporary")).unwrap();
    let Some(options) = options::python(root.path()) else {
        return;
    };
    let factory = Arc::new(python::Factory::new(options, FactoryScope::Session).unwrap());
    forged_contexts_refuse(factory.clone(), root.path(), || {
        factory
            .owner_report("ws", Some(&SessionId::from("session")))
            .unwrap()
    })
    .await;
}
#[tokio::test(flavor = "multi_thread")]
async fn bound_delegation_refuses_every_tool_for_forged_owners() {
    use crabber_extensions::delegate_task;
    let root = tempfile::tempdir().unwrap();
    let factory = Arc::new(
        delegate_task::Factory::new(options::delegate(runner()), FactoryScope::Session).unwrap(),
    );
    forged_contexts_refuse(factory.clone(), root.path(), || {
        factory
            .owner_report("ws", Some(&SessionId::from("session")))
            .unwrap()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_workspace_or_directory_refuses_without_business_effects() {
    let Some(h) = Harness::new(FactoryScope::Session, runner()) else {
        return;
    };
    let missing = context(&h.root.path().join("a"), "", "missing-workspace");
    let provider = h.scripts.set(vec![done()]);
    let error = agent_support::first_prompt(&h.agent, &missing, "refuse")
        .await
        .err()
        .expect("missing workspace must refuse factory admission");
    assert!(
        matches!(error, crabber::RuntimeError::ExtensionPlan(
        ExtensionError::ContextInitializationFailed { ref component }
    ) if component == "crabber-extensions/background-jobs-factory"),
        "{error:?}"
    );
    assert!(provider.requests().is_empty());
    let no_root = context(std::path::Path::new(""), "ws", "missing-directory");
    let records = h
        .first(
            &no_root,
            &[
                (jobs::START_TOOL, json!({"command":"true"})),
                (python::EXECUTE_TOOL, json!({"code":"1"})),
            ],
        )
        .await;
    for record in &records {
        assert!(value(record).as_str().unwrap().ends_with("workspace-root"));
    }
    assert_eq!(
        std::fs::read_dir(h.root.path().join("temporary"))
            .unwrap()
            .count(),
        0
    );
    h.close().await;
}
