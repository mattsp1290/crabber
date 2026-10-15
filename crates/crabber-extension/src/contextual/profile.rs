//! Frozen declarations for metadata-only admission reconciliation. This is
//! separate from the actual instance plan fingerprint checked before execution.
use super::{FactoryRegistration, FactoryScope};
use crate::{ComponentIdentity, Extension, ExtensionError, Scope};
use crabber_core::SessionContext;
use serde_json::{Value, json};
use std::{panic::AssertUnwindSafe, sync::Arc};

#[derive(Clone)]
pub(super) struct SharedIdentity {
    pub identity: ComponentIdentity,
    pub config_hash: String,
}
pub(super) struct SharedRegistration {
    pub extension: Arc<dyn Extension>,
    pub scope: Scope,
    pub metadata: Result<SharedIdentity, ExtensionError>,
}
impl SharedRegistration {
    pub(super) fn capture(extension: Arc<dyn Extension>, scope: Scope) -> Self {
        let metadata = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let identity = ComponentIdentity {
                id: extension.id().into(),
                version: extension.version().into(),
            };
            let config_hash = extension.config_hash();
            if identity.id.is_empty()
                || identity.id.len() > 128
                || identity.id.chars().any(char::is_control)
                || identity.version.is_empty()
                || identity.version.len() > 128
                || identity.version.chars().any(char::is_control)
                || config_hash.len() > 16384
            {
                return Err(invalid_shared());
            }
            Ok(SharedIdentity {
                identity,
                config_hash,
            })
        }))
        .unwrap_or_else(|payload| {
            crate::dispatch::discard_panic_payload(payload);
            Err(invalid_shared())
        });
        Self {
            extension,
            scope,
            metadata,
        }
    }
}
fn invalid_shared() -> ExtensionError {
    ExtensionError::ContextInitializationFailed {
        component: "shared-component".into(),
    }
}
pub(super) fn declarations(
    shared: &[SharedRegistration],
    factories: &[FactoryRegistration],
    context: &SessionContext,
) -> Result<Value, ExtensionError> {
    let mut shared = shared
        .iter()
        .filter(|r| r.scope.applies(context.session_id(), Some(context)))
        .collect::<Vec<_>>();
    shared.sort_by_key(|r| r.scope.rank());
    let shared = shared
        .into_iter()
        .map(|r| {
            let identity = r.metadata.as_ref().map_err(Clone::clone)?;
            Ok(json!([
                r.scope.declaration_value(),
                identity.identity,
                crate::plan::registration_config(&identity.config_hash)
            ]))
        })
        .collect::<Result<Vec<_>, ExtensionError>>()?;
    // Workspace factories are installed before session factories, preserving
    // registration order within each scope, independently of other owners.
    let mut factories = factories.iter().collect::<Vec<_>>();
    factories.sort_by_key(|r| match r.scope {
        FactoryScope::Workspace => 0,
        FactoryScope::Session => 1,
    });
    let factories = factories
        .into_iter()
        .map(|r| {
            json!([
                match r.scope {
                    FactoryScope::Workspace => "workspace",
                    FactoryScope::Session => "session",
                },
                r.identity,
                crate::plan::registration_config(&r.config_hash),
            ])
        })
        .collect::<Vec<_>>();
    Ok(json!([shared, factories]))
}
