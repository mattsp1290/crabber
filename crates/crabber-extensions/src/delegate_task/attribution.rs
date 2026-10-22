//! Bounded display-only publications through the runtime-selected source.
use crabber::{
    core::{ControlId, PublicActivity, PublicPatchOperation, PublicSourceChange},
    extension::{PublicSourceError, PublicSourceWriter, ToolContext},
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::oneshot;

pub(super) const ACTIVITY_TYPE: &str = "crabber-extensions.delegate-task";
#[derive(Clone)]
pub(super) struct Attribution {
    writer: PublicSourceWriter,
    id: String,
    failures: Arc<AtomicU64>,
}
impl Attribution {
    pub(super) fn new(
        context: &ToolContext,
        task: Option<&str>,
        writer: PublicSourceWriter,
        failures: Arc<AtomicU64>,
    ) -> Self {
        let hash = crate::config_hash(&(
            &context.session_id,
            &context.run_id,
            &context.call_id,
            task.unwrap_or("inline"),
        ));
        Self {
            writer,
            id: format!("delegate-task.{}", &hash[..32]),
            failures,
        }
    }
    pub(super) fn new_from_request(
        request: &super::Request,
        task: &str,
        writer: PublicSourceWriter,
        failures: Arc<AtomicU64>,
    ) -> Self {
        let hash = crate::config_hash(&(
            request.session_id(),
            request.run_id(),
            request.call_id(),
            task,
        ));
        Self {
            writer,
            id: format!("delegate-task.{}", &hash[..32]),
            failures,
        }
    }
    fn failed(&self) {
        let _ = self
            .failures
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
    }
    pub(super) async fn start(&self, mode: &str, profile: &str) {
        let Ok(activity) = PublicActivity::new(
            &self.id,
            ACTIVITY_TYPE,
            json!({"status":"running","mode":mode,"profile":profile}),
        ) else {
            self.failed();
            return;
        };
        self.publish("start", PublicSourceChange::ActivitySnapshot { activity })
            .await;
    }
    pub(super) async fn status(&self, operation: &str, status: &str) {
        let Ok(delta) =
            PublicPatchOperation::new(json!({"op":"replace","path":"/status","value":status}))
        else {
            self.failed();
            return;
        };
        self.publish(
            operation,
            PublicSourceChange::ActivityDelta {
                activity_id: self.id.clone(),
                activity_type: ACTIVITY_TYPE.into(),
                delta: vec![delta],
            },
        )
        .await;
    }
    async fn publish(&self, suffix: &str, change: PublicSourceChange) {
        let mut revision = match self.writer.snapshot().await {
            Ok(snapshot) => snapshot.revision,
            Err(PublicSourceError::Unsupported) => return,
            Err(_) => {
                self.failed();
                return;
            }
        };
        let id = ControlId::from(format!("{}.{}", self.id, suffix));
        let mut refreshed = false;
        let mut unknown = false;
        loop {
            match self
                .writer
                .publish(id.clone(), revision, change.clone())
                .await
            {
                Ok(_) | Err(PublicSourceError::Unsupported) => return,
                Err(PublicSourceError::RevisionConflict) if !refreshed && !unknown => {
                    refreshed = true;
                    match self.writer.snapshot().await {
                        Ok(snapshot) => revision = snapshot.revision,
                        Err(PublicSourceError::Unsupported) => return,
                        Err(_) => {
                            self.failed();
                            return;
                        }
                    }
                }
                // Retain the exact original operation, revision and payload.
                // A subsequent conflict cannot justify changing this unknown CAS.
                Err(PublicSourceError::UnknownOutcome) if !unknown => unknown = true,
                Err(_) => {
                    self.failed();
                    return;
                }
            }
        }
    }
    /// Register before runner work so cancellation/drop still attempts a settle
    /// publication inside the phase's resource drain. Standalone contexts have
    /// no resource scope and use the direct completion path instead.
    pub(super) fn settlement(&self, context: &ToolContext) -> Settlement {
        let (send, recv) = oneshot::channel();
        let attribution = self.clone();
        let cancelled = context.cancel.clone();
        let registered = context.resources().spawn(async move {
            let status = tokio::select! {
                biased;
                status = recv => status.unwrap_or("cancelled"),
                _ = cancelled.cancelled() => "cancelled",
            };
            attribution.status("settle", status).await;
            Ok(())
        });
        let send = match registered {
            Ok(()) => Some(send),
            Err(crabber::extension::ResourceRegistrationError::Unsupported) => None,
            Err(_) => {
                self.failed();
                None
            }
        };
        Settlement {
            send,
            attribution: self.clone(),
        }
    }
}
pub(super) struct Settlement {
    send: Option<oneshot::Sender<&'static str>>,
    attribution: Attribution,
}
impl Settlement {
    pub(super) async fn finish(mut self, status: &'static str) {
        if let Some(send) = self.send.take() {
            if send.send(status).is_err() {
                self.attribution.failed();
            }
        } else {
            self.attribution.status("settle", status).await;
        }
    }
}
