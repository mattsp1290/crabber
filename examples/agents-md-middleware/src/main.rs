use async_trait::async_trait;
use crabber::{Agent, Scope, WorkspaceReaderResolver};
use crabber::{
    AgentConfig, FakeProvider, Selection, StreamDelta, WorkspaceReadError, WorkspaceReadErrorKind,
    WorkspaceReader, extension::WorkspaceContext,
};
use crabber_middleware::{AGENTS_MD_MAX_FILE_BYTES, AgentsMdExtension};
use std::error::Error;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

const PATH: &str = "AGENTS.md";
const BODY: &str = "Use host-authorized workspace instructions only.";

struct MemoryReader {
    reads: Arc<Mutex<Vec<(String, usize)>>>,
}

#[async_trait]
impl WorkspaceReader for MemoryReader {
    async fn read_limited(
        &self,
        relative_path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        self.reads
            .lock()
            .expect("read log lock")
            .push((relative_path.to_owned(), max_bytes));
        if relative_path != PATH {
            return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::InvalidPath));
        }
        if max_bytes != AGENTS_MD_MAX_FILE_BYTES || BODY.len() > max_bytes {
            return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::TooLarge));
        }
        Ok(BODY.as_bytes()[..BODY.len().min(max_bytes)].to_vec())
    }
}

struct MemoryResolver {
    expected: WorkspaceContext,
    accepted: Arc<Mutex<Vec<WorkspaceContext>>>,
    denied: Arc<AtomicUsize>,
    reader: Arc<MemoryReader>,
}

#[async_trait]
impl WorkspaceReaderResolver for MemoryResolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        if workspace != &self.expected {
            self.denied.fetch_add(1, Ordering::SeqCst);
            return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::Denied));
        }
        self.accepted
            .lock()
            .expect("resolve log lock")
            .push(workspace.clone());
        Ok(self.reader.clone())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let expected = WorkspaceContext::from_persisted("example-workspace", "/routing/metadata");
    let reads = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(Mutex::new(Vec::new()));
    let denied = Arc::new(AtomicUsize::new(0));
    let resolver = Arc::new(MemoryResolver {
        expected: expected.clone(),
        accepted: Arc::clone(&accepted),
        denied: Arc::clone(&denied),
        reader: Arc::new(MemoryReader {
            reads: Arc::clone(&reads),
        }),
    });

    let forged = WorkspaceContext::from_persisted("example-workspace", "/forged");
    let Err(error) = resolver.resolve(&forged).await else {
        panic!("forged context received a reader");
    };
    assert_eq!(error.kind(), WorkspaceReadErrorKind::Denied);

    let provider = FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]);
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = expected.workspace_id().expect("workspace id").to_owned();
    config.directory = expected.directory().expect("directory").to_owned();
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider.clone()))
        .config(config)
        .workspace_reader_resolver(resolver)
        .extension(Arc::new(AgentsMdExtension::default()), Scope::Global)
        .build()?;

    agent
        .prompt(None, "Apply the instructions")
        .await?
        .done()
        .await?;

    let frame = format!(
        "## Workspace instructions: {PATH}\n<!-- crabber:agentsmd bytes={} -->\n{BODY}\n## End workspace instructions: {PATH}\n",
        BODY.len()
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].system.as_deref(), Some(frame.as_str()));
    assert_eq!(&*accepted.lock().expect("resolve log lock"), &[expected]);
    assert_eq!(denied.load(Ordering::SeqCst), 1);
    assert_eq!(
        &*reads.lock().expect("read log lock"),
        &[(PATH.to_owned(), AGENTS_MD_MAX_FILE_BYTES)]
    );
    println!("verified one authorized resolve/read and one denied forged context");
    Ok(())
}
