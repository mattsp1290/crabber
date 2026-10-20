use super::*;
use crate::{RunPlanProvider, StaticPlanProvider, ToolResourceOwner};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

fn context(owner: &ToolResourceOwner) -> PreparationResourceContext {
    PreparationResourceContext::from_host(
        crabber_core::SessionContext::new("session".into(), "workspace", "/workspace"),
        owner.resources(),
    )
    .unwrap()
}

#[tokio::test]
async fn capacity_is_reserved_before_invoking_acquisition_factory() {
    let owner = ToolResourceOwner::new();
    for _ in 0..crate::MAX_TOOL_RESOURCE_TASKS {
        owner.resources().spawn(async { Ok(()) }).unwrap();
    }
    let context = context(&owner);
    let invoked = AtomicBool::new(false);
    let result = context
        .acquire_plan(|| {
            invoked.store(true, Ordering::SeqCst);
            async { Err(ExtensionError::ContextOwnerClosed) }
        })
        .await;
    assert!(matches!(
        result,
        Err(ExtensionError::PreparationRegistration(
            crate::ResourceRegistrationError::Capacity
        ))
    ));
    assert!(!invoked.load(Ordering::SeqCst));
    assert_eq!(
        owner.drain(Duration::from_secs(1)).await.joined,
        crate::MAX_TOOL_RESOURCE_TASKS
    );
}

#[tokio::test]
async fn cleanup_joins_last_acquired_plan_reference() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    let provider = StaticPlanProvider::new(vec![], vec![]);
    let plan = context
        .acquire_plan(|| provider.acquire_plan(context.session().session_id()))
        .await
        .unwrap();
    let clone = plan.clone();
    assert_eq!(owner.drain(Duration::ZERO).await.pending, 1);
    plan.release();
    assert_eq!(owner.drain(Duration::ZERO).await.pending, 1);
    clone.release();
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.joined, 1);
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, []);
}

#[tokio::test]
async fn no_plan_does_not_acknowledge_independent_resource_cleanup() {
    let owner = ToolResourceOwner::new();
    let context = context(&owner);
    let release = Arc::new(tokio::sync::Notify::new());
    let held = release.clone();
    context
        .cleanup()
        .defer(|| async move {
            held.notified().await;
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        context
            .acquire_plan(|| async { Err(ExtensionError::ContextOwnerClosed) })
            .await,
        Err(ExtensionError::ContextOwnerClosed)
    ));
    let pending = owner.drain(Duration::ZERO).await;
    assert!(pending.pending >= 1);
    assert_eq!(pending.pending + pending.joined, 2);
    release.notify_one();
    let joined = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.joined, 2);
}

struct HeldFuture {
    ticket: Arc<Ticket>,
    destroyed: Arc<AtomicBool>,
    panic: bool,
}
impl Future for HeldFuture {
    type Output = Result<RunPlan, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for HeldFuture {
    fn drop(&mut self) {
        assert!(!self.ticket.finished.is_cancelled());
        assert!(!self.panic, "factory future destructor failed");
        self.destroyed.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_acquisition_destroys_actual_future_before_no_plan_ack() {
    for panic in [false, true] {
        let ticket = Arc::new(Ticket::default());
        let destroyed = Arc::new(AtomicBool::new(false));
        let mut acquisition = Box::pin(Acquiring {
            future: Some(Box::pin(HeldFuture {
                ticket: ticket.clone(),
                destroyed: destroyed.clone(),
                panic,
            })),
            ticket: ticket.clone(),
        });
        assert!(futures::poll!(acquisition.as_mut()).is_pending());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(acquisition)));
        assert_eq!(result.is_err(), panic);
        assert_eq!(destroyed.load(Ordering::SeqCst), !panic);
        assert_eq!(ticket.finished.is_cancelled(), !panic);
    }
}
