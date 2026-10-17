//! Invocation-only facades; held host objects may outlive their owned mount.
use super::InvocationSource;
use crate::{HostServices, Subprocess, WorkspaceFs};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

/// Projection only clones captured references; it invokes no user trait methods.
pub(in crate::registry) fn project(
    services: &HostServices,
    source: &InvocationSource,
) -> HostServices {
    HostServices {
        subprocess: services.subprocess.as_ref().map(|inner| {
            Arc::new(Process {
                inner: inner.clone(),
                source: source.clone(),
            }) as Arc<dyn Subprocess>
        }),
        workspace_fs: services.workspace_fs.as_ref().map(|inner| {
            Arc::new(Filesystem {
                inner: inner.clone(),
                source: source.clone(),
            }) as Arc<dyn WorkspaceFs>
        }),
    }
}
struct Process {
    inner: Arc<dyn Subprocess>,
    source: InvocationSource,
}
#[async_trait]
impl Subprocess for Process {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        let program = program.to_owned();
        let arguments = arguments.to_vec();
        let inner = self.inner.clone();
        self.source
            .invoke(move || Box::pin(async move { inner.run(&program, &arguments).await }))
            .await
            .map_err(|error| error.to_string())?
    }
}
struct Filesystem {
    inner: Arc<dyn WorkspaceFs>,
    source: InvocationSource,
}
#[async_trait]
impl WorkspaceFs for Filesystem {
    async fn read(&self, relative_path: &str) -> Result<Vec<u8>, String> {
        let path = relative_path.to_owned();
        let inner = self.inner.clone();
        self.source
            .invoke(move || Box::pin(async move { inner.read(&path).await }))
            .await
            .map_err(|error| error.to_string())?
    }
}
