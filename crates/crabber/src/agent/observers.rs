//! A failing host observer cannot suppress another observer or the caller.
use std::sync::Arc;

use crabber_core::{EventRecord, RunId, TraceContext};
use crabber_runtime::Observer;

#[cfg(test)]
mod tests;

// Panic payloads are user-owned too. Their Drop can panic; contain that second
// unwind and deliberately retain its payload rather than run another destructor.
fn isolated(callback: impl FnOnce()) {
    let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) else {
        return;
    };
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(payload);
    }
}

pub(super) struct AgentObservers {
    pub(super) observers: Vec<Arc<dyn Observer>>,
    #[cfg(feature = "datadog")]
    pub(super) datadog: Option<crabber_obs::DatadogObserver>,
}

impl Observer for AgentObservers {
    fn mount_close_timed_out(&self, timeout: &crabber_extension::MountCloseTimeout) {
        for observer in &self.observers {
            isolated(|| observer.mount_close_timed_out(timeout));
        }
    }
    fn operational_completed(&self, observation: &crabber_runtime::OperationalObservation) {
        for observer in &self.observers {
            isolated(|| observer.operational_completed(observation));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.operational_completed(observation));
        }
    }
    fn operational_completed_in_attempt(
        &self,
        observation: &crabber_runtime::OperationalObservation,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        for observer in &self.observers {
            isolated(|| observer.operational_completed_in_attempt(observation, context, attempt));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.operational_completed_in_attempt(observation, context, attempt));
        }
    }
    fn emit(&self, event: &EventRecord) {
        self.emit_with_context(event, None);
    }
    fn model_completed(&self, event: &EventRecord) {
        self.model_completed_with_context(event, None);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        for observer in &self.observers {
            isolated(|| observer.emit_with_context(event, context));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.emit_with_context(event, context));
            isolated(|| crabber_obs::tracing_bridge::emit_with_context(event, context));
        }
    }
    fn model_completed_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        for observer in &self.observers {
            isolated(|| observer.model_completed_with_context(event, context));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.model_completed_with_context(event, context));
        }
    }
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        for observer in &self.observers {
            isolated(|| observer.emit_in_attempt(event, context, attempt));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.emit_in_attempt(event, context, attempt));
            isolated(|| crabber_obs::tracing_bridge::emit_with_context(event, context));
        }
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        for observer in &self.observers {
            isolated(|| observer.model_completed_in_attempt(event, context, attempt));
        }
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            isolated(|| datadog.model_completed_in_attempt(event, context, attempt));
        }
    }
}
