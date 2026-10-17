use async_trait::async_trait;
use crabber_core::SessionId;
use crabber_extension::{
    Extension, ExtensionError, HostServices, Registrar, Registry, Scope, Subprocess, WorkspaceFs,
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

struct Services {
    subprocess: Arc<dyn Subprocess>,
    filesystem: Arc<dyn WorkspaceFs>,
}
#[async_trait]
impl Extension for Services {
    fn id(&self) -> &'static str {
        "owned-host-services"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.host_services(HostServices {
            subprocess: Some(self.subprocess.clone()),
            workspace_fs: Some(self.filesystem.clone()),
        });
        Ok(())
    }
}
struct Counter(Arc<AtomicUsize>);
#[async_trait]
impl Subprocess for Counter {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!([program, arguments]))
    }
}
#[async_trait]
impl WorkspaceFs for Counter {
    async fn read(&self, relative_path: &str) -> Result<Vec<u8>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(relative_path.as_bytes().to_vec())
    }
}
#[tokio::test]
async fn extracted_host_services_refuse_after_actual_owned_mount_close() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(Counter(calls.clone()));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(
            Arc::new(Services {
                subprocess: service.clone(),
                filesystem: service,
            }),
            Scope::Global,
        )
        .await
        .unwrap();
    let plan = registry.acquire(&SessionId::new());
    let services = plan.host_services.clone();
    drop(plan);
    mount.close(Duration::from_secs(1)).await.unwrap();
    let process = services
        .subprocess
        .unwrap()
        .run("program", &["argument".into()])
        .await;
    let file = services.workspace_fs.unwrap().read("file").await;
    assert!(
        process.is_err() && file.is_err(),
        "process={process:?}, file={file:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[path = "owned_host_service_invocations/retention.rs"]
mod retention;

#[tokio::test]
async fn services_reap_actual_joins_preserve_borrowed_inputs_and_legacy_behavior() {
    for owned in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(Counter(calls.clone()));
        let registry = Registry::new();
        let extension = Arc::new(Services {
            subprocess: service.clone(),
            filesystem: service,
        });
        if owned {
            registry
                .mount_owned(extension, Scope::Global)
                .await
                .unwrap();
        } else {
            registry.mount(extension, Scope::Global).await.unwrap();
        }
        let plan = registry.acquire(&SessionId::new());
        assert_eq!(
            plan.components.iter().any(|component| {
                component.id == "registry.mount.service_invocations"
                    && component.version == "crabber.registry.mount.service-invocations.v1"
            }),
            owned
        );
        let services = plan.host_services.clone();
        let process = services.subprocess.unwrap();
        let filesystem = services.workspace_fs.unwrap();
        for index in 0..130 {
            let argument = format!("argument-{index}");
            let path = format!("file-{index}");
            assert_eq!(
                process
                    .run("program", std::slice::from_ref(&argument))
                    .await
                    .unwrap(),
                json!(["program", [argument]])
            );
            assert_eq!(filesystem.read(&path).await.unwrap(), path.as_bytes());
        }
        drop(plan);
        registry.close_all().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 260);
        assert_eq!(process.run("program", &[]).await.is_err(), owned);
        assert_eq!(filesystem.read("file").await.is_err(), owned);
    }
}
