//! Poll durable controls independently of original extension callback disposal.
use super::{ObservationLifetime, RuntimeError};
use crabber_core::RunFence;
use crabber_session::{ExecutionStore, RunControlError, Store};
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::Instrument;

// None is cooperative retirement, never an observed cancellation.
pub(super) type Receiver = oneshot::Receiver<Option<Result<(), RuntimeError>>>;

pub(super) fn spawn(
    owner: &crabber_extension::ToolResourceOwner,
    execution: Arc<dyn ExecutionStore>,
    store: Arc<dyn Store>,
    fence: RunFence,
    lifetime: Arc<ObservationLifetime>,
) -> Result<Receiver, RuntimeError> {
    let closing = owner
        .resources()
        .closing()
        .ok_or(RuntimeError::PreparationRecoveryRequired)?;
    let (sender, receiver) = oneshot::channel();
    let parent = tracing::Span::current();
    owner
        .resources()
        .spawn(
            async move {
                tokio::pin!(closing);
                let result = observe(
                    execution.as_ref(),
                    store.as_ref(),
                    &fence,
                    &lifetime,
                    closing,
                )
                .await;
                let _ = sender.send(result);
                Ok(())
            }
            .instrument(parent),
        )
        .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
    Ok(receiver)
}

pub(super) async fn receive(
    receiver: &mut Option<Receiver>,
    lifetime: &ObservationLifetime,
    owner: &crabber_extension::ToolResourceOwner,
) -> Result<(), RuntimeError> {
    // Retain the receiver if another select branch wins while this await is pending.
    let result = match receiver.as_mut() {
        Some(receiver) => receiver.await,
        None => return std::future::pending().await,
    };
    *receiver = None;
    // Delivery can be queued before an original native commit begins waiting
    // for its ACK. Revalidate here as well as after the producer's Store await.
    if !lifetime.active() || owner.snapshot().sealed {
        return std::future::pending().await;
    }
    match result {
        Ok(Some(result)) => result,
        Ok(None) => std::future::pending().await,
        Err(_) => Err(RuntimeError::PreparationRecoveryRequired),
    }
}

#[cfg_attr(
    test,
    tracing::instrument(name = "crabber.test.durable_cancel", skip_all, level = "debug")
)]
async fn observe(
    execution: &dyn ExecutionStore,
    store: &dyn Store,
    fence: &RunFence,
    lifetime: &ObservationLifetime,
    mut closing: std::pin::Pin<&mut impl std::future::Future<Output = ()>>,
) -> Option<Result<(), RuntimeError>> {
    let mut timer = tokio::time::interval(std::time::Duration::from_millis(100));
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = closing.as_mut() => return None,
            _ = timer.tick() => (),
        }
        if futures::poll!(closing.as_mut()).is_ready() || !lifetime.active() {
            return None;
        }
        // Closing never cancels an original Store future. Its owning background
        // resource retains and polls it through the genuine return and join.
        let observed = execution.observe_run_controls(None).await;
        if futures::poll!(closing.as_mut()).is_ready() || !lifetime.active() {
            return None;
        }
        match observed {
            Ok(boundary) if boundary.cancel_observed => return Some(Ok(())),
            Ok(_) => (),
            Err(RunControlError::StaleOwner) => {
                let settled = store.get_run(&fence.run_id).await;
                if futures::poll!(closing.as_mut()).is_ready() || !lifetime.active() {
                    return None;
                }
                if settled.ok().flatten().is_some_and(|run| {
                    run.id == fence.run_id
                        && run.claim_token == fence.claim_token
                        && run.status.is_terminal()
                }) {
                    return None;
                }
                return Some(Err(RuntimeError::LeaseLost));
            }
            Err(error) => return Some(Err(error.into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cooperative_retirement_is_pending_but_unexpected_sender_loss_fails() {
        let lifetime = ObservationLifetime::default();
        let owner = crabber_extension::ToolResourceOwner::new();
        let (sender, receiver) = oneshot::channel();
        sender.send(None).unwrap();
        let mut receiver = Some(receiver);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                receive(&mut receiver, &lifetime, &owner),
            )
            .await
            .is_err()
        );
        assert!(receiver.is_none());
        let (sender, receiver) = oneshot::channel();
        drop(sender);
        let mut receiver = Some(receiver);
        assert!(matches!(
            receive(&mut receiver, &lifetime, &owner).await,
            Err(RuntimeError::PreparationRecoveryRequired)
        ));
    }

    #[tokio::test]
    async fn losing_a_select_race_retains_the_original_control_receiver() {
        let lifetime = ObservationLifetime::default();
        let owner = crabber_extension::ToolResourceOwner::new();
        let (sender, original) = oneshot::channel();
        let mut receiver = Some(original);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(10),
                receive(&mut receiver, &lifetime, &owner),
            )
            .await
            .is_err()
        );
        assert!(receiver.is_some());
        sender.send(Some(Ok(()))).unwrap();
        receive(&mut receiver, &lifetime, &owner).await.unwrap();
        assert!(receiver.is_none());
    }
    #[tokio::test]
    async fn queued_results_cannot_supersede_quiescent_ack_or_cooperative_close() {
        for close_owner in [false, true] {
            for outcome in 0..3 {
                let lifetime = ObservationLifetime::default();
                let owner = crabber_extension::ToolResourceOwner::new();
                let (sender, receiver) = oneshot::channel();
                match outcome {
                    0 => sender.send(Some(Ok(()))).unwrap(),
                    1 => sender.send(Some(Err(RuntimeError::LeaseLost))).unwrap(),
                    _ => drop(sender),
                }
                let mut receiver = Some(receiver);
                // Queue while Active, then revoke observation before delivery.
                let _quiescent = if close_owner {
                    owner.close();
                    None
                } else {
                    Some(lifetime.quiescent_commit())
                };
                assert!(
                    tokio::time::timeout(
                        std::time::Duration::from_millis(10),
                        receive(&mut receiver, &lifetime, &owner),
                    )
                    .await
                    .is_err()
                );
                assert!(receiver.is_none());
            }
        }
    }
}
