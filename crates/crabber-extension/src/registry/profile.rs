//! Capture registration metadata without plan leases, initialization or execution.
use super::{
    Registry, Scope, append_registration_components, finish_contributors, freeze_contributors,
};
use crate::dispatch::{HandlerFn, Mode};
use crate::{ExtensionError, Registrar, TransformPhase};
use crabber_core::SessionContext;
use serde_json::Value;
use std::panic::AssertUnwindSafe;

#[derive(Clone)]
pub(crate) struct RegistrationSnapshot {
    mounts: Vec<(Scope, Value)>,
}
impl RegistrationSnapshot {
    pub(crate) fn all_declarations(&self) -> Value {
        Value::Array(self.mounts.iter().map(|(_, value)| value.clone()).collect())
    }

    pub(crate) fn for_context(&self, context: &SessionContext) -> Value {
        Value::Array(
            self.mounts
                .iter()
                .filter(|(scope, _)| scope.applies(context.session_id(), Some(context)))
                .map(|(_, value)| value.clone())
                .collect(),
        )
    }
}
pub(super) fn capture(registry: &Registry) -> Result<RegistrationSnapshot, ExtensionError> {
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        let mut mounts = {
            let inner = registry.inner.lock().unwrap();
            if inner.closed {
                return Err(ExtensionError::RegistryClosed);
            }
            inner
                .mounts
                .iter()
                .filter(|mount| *mount.active.lock().unwrap())
                .cloned()
                .collect::<Vec<_>>()
        };
        mounts.sort_by_key(|mount| (mount.scope.rank(), mount.seq));
        let mut snapshots = Vec::new();
        for mount in mounts {
            let registrar = {
                let r = mount.registrar.lock().unwrap();
                r.profile_fields()
            };
            let mut components = Vec::new();
            let mut contributors = Vec::new();
            // Global mount sequence is intentionally absent. The outer ordered
            // list records precedence among registrations relevant to a context.
            let identity = mount.registration_identity();
            append_registration_components(
                &identity,
                mount.scope.rank(),
                &registrar,
                0,
                &mut components,
                mount.registration.as_ref(),
            );
            super::owned::append_contract(&mount, &mut components);
            freeze_contributors(
                &registrar.contributors,
                &registrar.product_prompts,
                &mut contributors,
                &mut components,
                0,
                None,
            )?;
            finish_contributors(&mut contributors, &mut components);
            let handlers = registrar
                .handlers
                .iter()
                .map(|handler| {
                    serde_json::json!([
                        handler.point,
                        handler.id,
                        handler.order,
                        match handler.mode {
                            Mode::Notify => "notify",
                            Mode::Hook => "hook",
                            Mode::Transform => "transform",
                            Mode::Gate => "gate",
                            Mode::Around => "around",
                        },
                        match handler.phase {
                            TransformPhase::Ordinary => "ordinary",
                            TransformPhase::FinalRedaction => "final_redaction",
                        },
                        match handler.callback {
                            HandlerFn::Ordinary(_) => "ordinary",
                            HandlerFn::Around(_) => "around",
                            HandlerFn::ResultTransform(_) => "result_transform",
                        },
                    ])
                })
                .collect::<Vec<_>>();
            snapshots.push((
                mount.scope.clone(),
                serde_json::to_value((
                    mount.scope.declaration_value(),
                    &identity.id,
                    &identity.version,
                    crate::plan::registration_config(&identity.config),
                    components,
                    handlers,
                ))
                .map_err(|_| ExtensionError::RegistrationDescriptorUnavailable)?,
            ));
        }
        Ok(RegistrationSnapshot { mounts: snapshots })
    }))
    .unwrap_or_else(|payload| {
        crate::dispatch::discard_panic_payload(payload);
        Err(ExtensionError::RegistrationDescriptorUnavailable)
    })
}

#[cfg(test)]
#[path = "profile_tests.rs"]
mod tests;

impl Registrar {
    // Pure field projection only; caller holds the original registrar lock.
    fn profile_fields(&self) -> Self {
        Self {
            services: self.services.clone(),
            tools: self.tools.clone(),
            product_tools: std::collections::BTreeMap::new(),
            product_guards: std::collections::BTreeMap::new(),
            product_prompts: std::collections::BTreeMap::new(),
            product_workspace_fs: None,
            product_subprocess: None,
            product_workspace_reader_resolver: None,
            product_providers: self.product_providers.clone(),
            prompts: self.prompts.clone(),
            guards: self.guards.clone(),
            restrictions: self.restrictions.clone(),
            handlers: self.handlers.clone(),
            contributors: self.contributors.clone(),
            providers: self.providers.clone(),
            cleanups: vec![],
        }
    }
}
