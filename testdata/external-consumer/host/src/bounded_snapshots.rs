//! Separately compiled host validates custom Store compatibility and public caps.
use crabber::{SnapshotLimits, SnapshotOutcome, SnapshotRequest, core::*, session::*};
use std::{collections::BTreeMap, error::Error, sync::Arc};

struct LegacyStore(MemoryStore);
struct BoundedStore(MemoryStore);

macro_rules! forward_store {
    ($store:ty, $($extra:item)*) => {
        #[async_trait::async_trait]
        impl Store for $store {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.0.admit_run(request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.0.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.0.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.0.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.0.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.0.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.0.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.0.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.0.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.0.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.0.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.0.enqueue_inbox(session, kind, message).await
    }
            $($extra)*
        }
    };
}
forward_store!(LegacyStore,);
forward_store!(
    BoundedStore,
    async fn snapshot(&self, request: SnapshotRequest) -> Result<SnapshotOutcome, StoreError> {
        self.0.snapshot(request).await
    }
);

pub async fn run() -> Result<(), Box<dyn Error>> {
    let session = SessionId::from("external-bounded-session");
    let mut query = SnapshotRequest {
        session_id: session.clone(),
        continuation: None,
        limits: SnapshotLimits {
            messages: 1,
            tool_calls: 1,
            parts: 4,
            text_bytes: 4096,
            encoded_bytes: 8192,
        },
    };
    // Old implementations compile and fail closed rather than falling back to
    // unbounded reads. Custom implementations may delegate the entire contract.
    let legacy = LegacyStore(MemoryStore::new());
    assert!(matches!(
        legacy.snapshot(query.clone()).await,
        Err(CoreError::SnapshotUnsupported)
    ));
    let store = Arc::new(BoundedStore(MemoryStore::new()));
    let agent = crabber::Agent::builder()
        .store(store.clone())
        .provider(Arc::new(crabber::FakeProvider::scripted(vec![vec![
            crabber::StreamDelta::TextDelta("fake".into()),
            crabber::StreamDelta::Completed,
        ]])))
        .config(crabber::AgentConfig::new(crabber::Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()?;
    let handle = agent.prompt(None, "fake").await?;
    query.session_id = handle.session_id().clone();
    handle.done().await?;
    query.limits.messages = 0;
    let SnapshotOutcome::Limited {
        high_water: h,
        continuation,
        ..
    } = store.snapshot(query.clone()).await?
    else {
        panic!("limit")
    };
    query.continuation = Some(continuation);
    query.limits.messages = 1;
    let mut ids = Vec::new();
    let mut pages = 0;
    loop {
        let SnapshotOutcome::Page(page) = store.snapshot(query.clone()).await? else {
            panic!("page")
        };
        assert_eq!(page.high_water, h);
        assert!(page.usage.messages <= 1 && page.usage.parts <= 4);
        assert!(page.usage.text_bytes <= 4096 && page.usage.encoded_bytes <= 8192);
        assert_eq!(
            page.usage.encoded_bytes,
            page.messages
                .iter()
                .map(|m| serde_json::to_vec(m).unwrap().len())
                .sum::<usize>()
        );
        ids.extend(page.messages.into_iter().map(|m| m.id));
        pages += 1;
        let Some(token) = page.continuation else {
            break;
        };
        assert!(token.0.len() <= 2048);
        query.continuation = Some(token);
    }
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert_eq!(pages, 2);
    assert!(store.list_events(&query.session_id, Some(h), 1).await?.is_empty());
    println!(
        "External bounded snapshots: default unsupported; delegated custom Store H={} pages={pages} messages={} caps and continuation passed",
        h.0,
        ids.len()
    );
    Ok(())
}
