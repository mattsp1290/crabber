use super::*;
use crabber::extension::{Extension, Registrar, TransformOutput};

pub(super) struct Protection(pub Arc<AtomicUsize>);
#[async_trait]
impl Extension for Protection {
    fn id(&self) -> &'static str {
        "checkpoint-result-protection"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixed".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let transformed = self.0.clone();
        registrar.on_result_transform(
            0,
            "transform",
            Arc::new(move |_, output| {
                let transformed = transformed.clone();
                Box::pin(async move {
                    assert!(
                        !serde_json::to_string(&output)
                            .unwrap()
                            .contains("never result")
                    );
                    transformed.fetch_add(1, Ordering::SeqCst);
                    Ok(TransformOutput::new(output))
                })
            }),
        );
        let redacted = self.0.clone();
        registrar.on_final_redaction(
            0,
            "redact",
            Arc::new(move |_, output| {
                let redacted = redacted.clone();
                Box::pin(async move {
                    assert!(
                        !serde_json::to_string(&output)
                            .unwrap()
                            .contains("never result")
                    );
                    redacted.fetch_add(1, Ordering::SeqCst);
                    Ok(TransformOutput::new(output))
                })
            }),
        );
        Ok(())
    }
}
pub(super) fn make(
    store: Arc<dyn Store>,
    provider: FakeProvider,
    tool: Arc<dyn ResumableTool>,
    counter: Arc<AtomicUsize>,
) -> Agent {
    super::builder(store, provider, tool)
        .shared_extension(Arc::new(Protection(counter)), Scope::Global)
        .build()
        .unwrap()
}
