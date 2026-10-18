//! Public observers never receive raw/unspecified thinking or continuation data.
use super::*;
use crabber_core::ReasoningProvenance;
use crabber_session::{PublicHistoryOutcome, SnapshotLimits, SnapshotRequest};

const PRIVATE: &str = "PRIVATE_REASONING_SENTINEL";
const OPAQUE: &str = "OPAQUE_CONTINUITY_SENTINEL";
#[derive(Default)]
struct Capture {
    events: Mutex<Vec<EventRecord>>,
    entered: Notify,
}
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.events.lock().unwrap().push(event.clone());
        if event.kind == EventKind::TextDelta {
            self.entered.notify_one();
        }
    }
}
fn thought(id: &str, text: &str, provenance: ReasoningProvenance) -> StreamDelta {
    StreamDelta::Reasoning {
        item_id: id.into(),
        provenance,
        text: text.into(),
    }
}
fn script() -> Vec<StreamDelta> {
    vec![
        StreamDelta::ReasoningDelta(PRIVATE.into()),
        thought("anthropic:0", PRIVATE, ReasoningProvenance::Private),
        StreamDelta::ReasoningSignature {
            item_id: "anthropic:0".into(),
            signature: "SIGNATURE_A".into(),
        },
        StreamDelta::ProviderState {
            codec_id: "crabber.codex.reasoning-items/v1".into(),
            payload: json!({"type":"reasoning","id":"rs_private","encrypted_content":OPAQUE}),
        },
        thought(
            "summary:0",
            "public summary",
            ReasoningProvenance::PublicSummary,
        ),
        thought(
            "anthropic:2",
            "second private",
            ReasoningProvenance::Private,
        ),
        StreamDelta::ReasoningSignature {
            item_id: "anthropic:2".into(),
            signature: "SIGNATURE_B".into(),
        },
    ]
}
fn runtime(
    store: Arc<MemoryStore>,
    fake: FakeProvider,
    capture: Arc<Capture>,
) -> crate::OrchestratorBuilder {
    Orchestrator::builder()
        .behavior_version("reasoning-provenance.v1")
        .store(store)
        .resolver(Arc::new(fake))
        .observer(capture)
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            vec![],
        )))
}
fn assert_live_private_absent(capture: &Capture) {
    let events = capture.events.lock().unwrap();
    let encoded = serde_json::to_string(&*events).unwrap();
    for secret in [
        PRIVATE,
        OPAQUE,
        "second private",
        "SIGNATURE_A",
        "SIGNATURE_B",
    ] {
        assert!(
            !encoded.contains(secret),
            "private source escaped into public event"
        );
    }
    for event in events
        .iter()
        .filter(|event| event.kind == EventKind::ReasoningDelta)
    {
        let delta: crabber_core::event_payload::ReasoningDelta =
            serde_json::from_value(event.payload.clone()).unwrap();
        assert_eq!(delta.provenance, ReasoningProvenance::PublicSummary);
        assert_eq!(delta.item_id, "summary:0");
        assert_eq!(delta.text, "public summary");
    }
}
async fn assert_public_history_private_absent(store: &MemoryStore, session: SessionId) {
    let limits = SnapshotLimits {
        messages: 100,
        tool_calls: 100,
        parts: 1000,
        text_bytes: 1_000_000,
        encoded_bytes: 2_000_000,
    };
    let PublicHistoryOutcome::Page(page) = store
        .public_history(SnapshotRequest {
            session_id: session.clone(),
            continuation: None,
            limits,
        })
        .await
        .unwrap()
    else {
        panic!("bounded public history")
    };
    let wire = serde_json::to_string(&page).unwrap();
    for secret in [
        PRIVATE,
        OPAQUE,
        "second private",
        "SIGNATURE_A",
        "SIGNATURE_B",
    ] {
        assert!(!wire.contains(secret));
    }
    let crabber_session::PublicSessionOutcomeV2::Page(page) = store
        .public_session_snapshot_v2(crabber_session::PublicSessionRequestV2 {
            session_id: session,
            continuation: None,
            limits: crabber_session::PublicSessionLimitsV2 {
                header_bytes: 1_000_000,
                proof_bytes: 32 << 20,
                approval_interactions: 64,
                tool_requirements: 64,
                queue_entries: 64,
                registered_owners: 1024,
                run_controls: 64,
                history: limits,
            },
        })
        .await
        .unwrap()
    else {
        panic!("bounded aggregate public snapshot")
    };
    let wire = serde_json::to_string(&page).unwrap();
    for secret in [
        PRIVATE,
        OPAQUE,
        "second private",
        "SIGNATURE_A",
        "SIGNATURE_B",
    ] {
        assert!(!wire.contains(secret));
    }
}

#[tokio::test]
async fn distinct_items_persist_privately_and_reach_the_next_provider_without_live_leaks() {
    let store = Arc::new(MemoryStore::new());
    let capture = Arc::new(Capture::default());
    let mut first = script();
    first.extend(call_script(ToolCallId::new(), r#"{"text":"visible"}"#));
    let fake = FakeProvider::scripted(vec![first, text_script("done")]);
    let runtime = runtime(store.clone(), fake.clone(), capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_live_private_absent(&capture);
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    let raw = serde_json::to_string(&requests[1].messages).unwrap();
    assert!(raw.contains(PRIVATE) && raw.contains(OPAQUE));
    let order: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match &part.content {
            ContentBlock::Reasoning { item_id, .. } => {
                Some(item_id.as_deref().unwrap_or("unspecified"))
            }
            ContentBlock::ProviderState { codec_id, .. } => Some(codec_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        [
            "unspecified",
            "anthropic:0",
            "crabber.codex.reasoning-items/v1",
            "summary:0",
            "anthropic:2"
        ]
    );
    let blocks: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match &part.content {
            ContentBlock::Reasoning {
                item_id,
                provenance,
                text,
                provider_state,
            } => Some((item_id, provenance, text, provider_state)),
            _ => None,
        })
        .collect();
    assert_eq!(blocks.len(), 4);
    assert_eq!(*blocks[0].1, ReasoningProvenance::Unspecified);
    assert_eq!(blocks[1].3.as_ref().unwrap()["signature"], "SIGNATURE_A");
    assert_eq!(*blocks[2].1, ReasoningProvenance::PublicSummary);
    assert_eq!(blocks[2].2, "public summary");
    assert_eq!(blocks[3].3.as_ref().unwrap()["signature"], "SIGNATURE_B");
    assert_public_history_private_absent(&store, session).await;
}

struct Partial;
#[async_trait]
impl ModelStream for Partial {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let _ = next.stream(request).await?;
        let mut items = script();
        items.push(StreamDelta::TextDelta("ready".into()));
        Ok(Box::pin(
            futures::stream::iter(items).chain(futures::stream::pending()),
        ))
    }
}
#[tokio::test]
async fn interruption_preserves_private_continuity_but_not_in_public_history_or_live_events() {
    let store = Arc::new(MemoryStore::new());
    let capture = Arc::new(Capture::default());
    let runtime = runtime(
        store.clone(),
        FakeProvider::scripted(vec![text_script("unused")]),
        capture.clone(),
    )
    .model_stream(Arc::new(Partial))
    .build()
    .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    capture.entered.notified().await;
    handle.interrupt();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
    let raw = serde_json::to_string(&store.list_messages(&session, None).await.unwrap()).unwrap();
    assert!(raw.contains(PRIVATE) && raw.contains(OPAQUE) && raw.contains("SIGNATURE_A"));
    assert_live_private_absent(&capture);
    assert_public_history_private_absent(&store, session).await;
}

#[tokio::test]
async fn retry_and_provenance_reclassification_cannot_publish_private_reasoning() {
    for reclassify in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let capture = Arc::new(Capture::default());
        let first = if reclassify {
            vec![
                thought("same", PRIVATE, ReasoningProvenance::Private),
                thought("same", PRIVATE, ReasoningProvenance::PublicSummary),
            ]
        } else {
            vec![
                StreamDelta::ReasoningDelta(PRIVATE.into()),
                StreamDelta::Error(ProviderError {
                    kind: crabber_providers::ProviderErrorKind::Server,
                    message: "safe failure".into(),
                    retryable: true,
                }),
            ]
        };
        let fake = FakeProvider::scripted(vec![first, text_script("done")]);
        let runtime = runtime(store.clone(), fake.clone(), capture.clone())
            .build()
            .unwrap();
        let handle = runtime.start(request()).await.unwrap();
        let session = handle.session_id().clone();
        let result = handle.done().await;
        assert_eq!(result.is_err(), reclassify);
        assert!(!format!("{result:?}").contains(PRIVATE));
        assert_eq!(fake.requests().len(), if reclassify { 1 } else { 2 });
        assert_live_private_absent(&capture);
        assert_public_history_private_absent(&store, session).await;
    }
}
