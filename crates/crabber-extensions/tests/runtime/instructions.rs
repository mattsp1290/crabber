use super::*;
use crabber::{
    ExecutionContext,
    core::{SessionContext, SessionId},
    extension::{
        WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind as Kind, WorkspaceReader,
        WorkspaceReaderResolver,
    },
};
use crabber_extensions::workspace_instructions::{
    Limits, Options, ReaderRoot, WorkspaceInstructions,
    host::{AllowListResolver, BoundaryReader, WorkspaceRootReader},
};
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

fn options(root: ReaderRoot, boundaries: Vec<String>) -> Options {
    Options {
        order: 100,
        file_names: vec!["AGENTS.md".into()],
        reader_root: root,
        boundaries,
        limits: Limits {
            max_file_names: 4,
            max_chain_depth: 8,
            max_file_bytes: 1024,
            max_section_bytes: 4096,
            max_in_flight: 4,
            max_wait: Duration::from_secs(1),
        },
    }
}
fn builder(
    provider: Arc<FakeProvider>,
    options: Options,
    resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
) -> crabber::AgentBuilder {
    let b = agent_support::builder()
        .memory()
        .provider(provider)
        .config(config("/unused-host-default"))
        .shared_extension(
            Arc::new(WorkspaceInstructions::new(options).unwrap()),
            Scope::Global,
        );
    if let Some(resolver) = resolver {
        b.workspace_reader_resolver(resolver)
    } else {
        b
    }
}
fn execution(workspace: &str, directory: &str) -> ExecutionContext {
    ExecutionContext::new(
        SessionContext::new(SessionId::new(), workspace, directory),
        Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        },
    )
}
async fn run(
    agent: &Agent,
    workspace: &str,
    directory: &str,
) -> Result<crabber::runtime::RunResult, crabber::RuntimeError> {
    agent_support::first_prompt(agent, &execution(workspace, directory), "fixture")
        .await?
        .done()
        .await
}
#[derive(Default)]
struct MemoryReader {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    reads: Mutex<Vec<String>>,
}
impl MemoryReader {
    fn new(files: &[(&str, &[u8])]) -> Arc<Self> {
        Arc::new(Self {
            files: Mutex::new(
                files
                    .iter()
                    .map(|(p, b)| ((*p).into(), b.to_vec()))
                    .collect(),
            ),
            ..Self::default()
        })
    }
}
#[async_trait]
impl WorkspaceReader for MemoryReader {
    async fn read_limited(
        &self,
        path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        self.reads.lock().unwrap().push(path.into());
        let bytes = self
            .files
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .ok_or_else(|| WorkspaceReadError::new(Kind::NotFound))?;
        if bytes.len() > max_bytes {
            Err(WorkspaceReadError::new(Kind::TooLarge))
        } else {
            Ok(bytes)
        }
    }
}
struct Fixed(Arc<dyn WorkspaceReader>);
#[async_trait]
impl WorkspaceReaderResolver for Fixed {
    async fn resolve(
        &self,
        _: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        Ok(self.0.clone())
    }
}
fn fixed(reader: Arc<dyn WorkspaceReader>) -> Option<Arc<dyn WorkspaceReaderResolver>> {
    Some(Arc::new(Fixed(reader)))
}

#[path = "instructions/frames.rs"]
mod frames;
#[path = "instructions/lifecycle.rs"]
mod lifecycle;
#[path = "instructions/routing.rs"]
mod routing;
