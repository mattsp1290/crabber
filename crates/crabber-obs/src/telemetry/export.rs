//! Preserve export failures that the SDK's shutdown acknowledgement discards.
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{SpanData, SpanExporter},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Debug)]
pub(super) struct TrackedExporter<E> {
    inner: E,
    failed: Arc<AtomicBool>,
}
impl<E> TrackedExporter<E> {
    pub(super) fn new(inner: E, failed: Arc<AtomicBool>) -> Self {
        Self { inner, failed }
    }
}
impl<E: SpanExporter> SpanExporter for TrackedExporter<E> {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self.inner.export(batch).await;
        self.record(result)
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.record(self.inner.shutdown())
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.record(self.inner.shutdown_with_timeout(timeout))
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.record(self.inner.force_flush())
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
impl<E> TrackedExporter<E> {
    fn record(&self, result: OTelSdkResult) -> OTelSdkResult {
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
}
