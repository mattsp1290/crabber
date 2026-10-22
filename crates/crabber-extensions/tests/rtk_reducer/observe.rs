use super::*;
use crabber::{
    core::{ContentBlock, EventKind, ToolResultStatus},
    extension::{Registrar, ToolOutcomeClass, TransformOutput},
};
use std::sync::Mutex;

#[derive(Default)]
pub(super) struct ResultProbe {
    pub classes: Mutex<Vec<(ToolCallId, ToolOutcomeClass)>>,
    pub final_results: Mutex<Vec<(ToolCallId, Value)>>,
}
pub(super) struct ProbeMount(pub Arc<ResultProbe>);
#[async_trait::async_trait]
impl Extension for ProbeMount {
    fn id(&self) -> &str {
        "fixture/result-probe"
    }
    fn version(&self) -> &str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture/result-probe-v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let probe = self.0.clone();
        registrar.on_result_transform(
            // A preceding ordinary probe would establish an accepted original
            // value, which Crabber deliberately retains on interruption.
            DEFAULT_ORDER + 1,
            "fixture/result-probe",
            Arc::new(move |context, value| {
                let probe = probe.clone();
                Box::pin(async move {
                    probe
                        .classes
                        .lock()
                        .unwrap()
                        .push((context.call_id().clone(), context.class()));
                    Ok(TransformOutput::new(value))
                })
            }),
        );
        let probe = self.0.clone();
        registrar.on_final_redaction(
            100,
            "fixture/result-probe-final",
            Arc::new(move |context, value| {
                let probe = probe.clone();
                Box::pin(async move {
                    probe
                        .final_results
                        .lock()
                        .unwrap()
                        .push((context.call_id().clone(), value.json_value().unwrap()));
                    Ok(TransformOutput::new(value))
                })
            }),
        );
        Ok(())
    }
}

pub(super) async fn assert_settled(
    store: &MemoryStore,
    session: &SessionId,
    records: &[ToolCallRecord],
) {
    let events = store.list_events(session, None, 100).await.unwrap();
    for record in records {
        let event = events
            .iter()
            .find(|event| {
                event.kind == EventKind::ToolCallSettled
                    && event.payload["call_id"] == record.id.to_string()
            })
            .expect("settled event for durable call");
        let result = record.result.as_ref().unwrap();
        assert_eq!(
            event.payload["content"],
            serde_json::to_value(&result.content).unwrap()
        );
        assert_eq!(
            event.payload["is_error"],
            result.status != ToolResultStatus::Completed
        );
        assert_eq!(
            event.payload["status"],
            serde_json::to_value(record.status).unwrap()
        );
    }
}
pub(super) fn assert_provider_record(
    provider: &FakeProvider,
    index: usize,
    record: &ToolCallRecord,
) {
    let requests = provider.requests();
    let result = record.result.as_ref().unwrap();
    let block = requests[index]
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .find_map(|part| match &part.content {
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } if *call_id == record.id => Some((content, is_error)),
            _ => None,
        })
        .expect("durable tool result in next provider request");
    assert_eq!(*block.0, result.content);
    assert_eq!(*block.1, result.status != ToolResultStatus::Completed);
}

pub(super) async fn assert_three_surfaces(
    store: &MemoryStore,
    session: &SessionId,
    provider: &FakeProvider,
    call_id: &ToolCallId,
    expected: &Value,
) {
    let records = snapshot_tools(store, session).await;
    let record = records.iter().find(|r| &r.id == call_id).unwrap();
    assert_eq!(result_value(record), *expected);
    assert_settled(store, session, std::slice::from_ref(record)).await;
    assert_provider_record(provider, provider.requests().len() - 1, record);
}
