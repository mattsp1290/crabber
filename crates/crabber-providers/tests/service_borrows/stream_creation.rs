//! A not-yet-returned stream is still an actual borrowed reference.
use super::*;
use crabber_providers::{DeltaStream, ModelRequest, ProviderError, Resolver, Selection, Streamer};
struct StreamFactory {
    disposed: Arc<AtomicBool>,
    drop_panic: bool,
}
impl Future for StreamFactory {
    type Output = Result<DeltaStream, ProviderError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(self.drop_panic, "actual stream constructor poll failed");
        Poll::Pending
    }
}
impl Drop for StreamFactory {
    fn drop(&mut self) {
        self.disposed.store(true, Ordering::SeqCst);
        assert!(
            !self.drop_panic,
            "actual stream constructor destructor failed"
        );
    }
}
struct Backend {
    disposed: Arc<AtomicBool>,
    drop_panic: bool,
}
#[async_trait::async_trait]
impl Streamer for Backend {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        StreamFactory {
            disposed: self.disposed.clone(),
            drop_panic: self.drop_panic,
        }
        .await
    }
}
fn request(selection: Selection) -> ModelRequest {
    ModelRequest {
        context: vec![],
        identity: crabber_providers::RequestIdentity {
            session_id: crabber_core::SessionId::new(),
            run_id: crabber_core::RunId::new(),
            turn_id: crabber_core::TurnId::new(),
        },
        selection,
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        tool_choice: None,
    }
}
#[test]
fn stream_constructor_failure_never_acknowledges_reference_disposal() {
    for drop_panic in [true, false] {
        let disposed = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicUsize::new(0));
        let selection = Selection {
            provider_id: "fake".into(),
            model_id: "model".into(),
        };
        let fake = crabber_providers::FakeProvider::scripted(vec![]);
        let descriptor = fake.binding_descriptor(&selection).unwrap();
        let (bound, observer) = BoundProvider::new(
            &selection,
            descriptor,
            Arc::new(Backend {
                disposed: disposed.clone(),
                drop_panic,
            }),
        )
        .unwrap()
        .retain_service_lease(Arc::new(Lease {
            disposed: disposed.clone(),
            released: released.clone(),
        }))
        .observe_release();
        let streamer = bound.streamer();
        let mut creating = Box::pin(streamer.stream(request(selection)));
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            creating.as_mut().poll(&mut context)
        }));
        if drop_panic {
            assert!(polled.unwrap().is_pending());
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(creating))).is_err()
            );
        } else {
            assert!(polled.is_err());
            drop(creating);
        }
        drop(streamer);
        drop(bound);
        assert!(disposed.load(Ordering::SeqCst));
        assert_eq!(
            released.load(Ordering::SeqCst),
            0,
            "failed stream construction cannot release its original service reference"
        );
        assert!(
            !observer.is_released(),
            "failed stream construction cannot acknowledge provider disposal"
        );
    }
}
