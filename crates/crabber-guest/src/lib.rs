//! Rust guest SDK for `crabber:extensions@0.1.0` components.
//!
//! Build a guest with `cargo build --target wasm32-wasip2 --release`. The
//! resulting `.wasm` is a Component Model component.

pub use crabber_guest_macros::generate;
pub use serde_json;
pub use wit_bindgen;
pub use wit_bindgen::rt;

/// Maps an SDK role name to its generated WIT enum variant.
#[macro_export]
macro_rules! role {
    (tool) => {
        crabber::extensions::types::Role::Tool
    };
    (permissions_policy) => {
        crabber::extensions::types::Role::PermissionsPolicy
    };
    (context_source) => {
        crabber::extensions::types::Role::ContextSource
    };
    (prompt_section) => {
        crabber::extensions::types::Role::PromptSection
    };
    (event_sink) => {
        crabber::extensions::types::Role::EventSink
    };
    (hook) => {
        crabber::extensions::types::Role::Hook
    };
    (tool_middleware) => {
        crabber::extensions::types::Role::ToolMiddleware
    };
    (model_controls) => {
        crabber::extensions::types::Role::ModelControls
    };
}

/// Implements the manifest and exports a type whose role API traits are
/// implemented in the guest crate.
#[macro_export]
macro_rules! export_extension {
    ($ty:ident; roles = [$($role:ident),* $(,)?]) => {
        $crate::export_extension!($ty; id = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), roles = [$($role),*]);
    };
    ($ty:ident; id = $id:expr, version = $version:expr, roles = [$($role:ident),* $(,)?]) => {
        impl exports::crabber::extensions::manifest_api::Guest for $ty {
            fn describe() -> crabber::extensions::types::ExtensionManifest {
                crabber::extensions::types::ExtensionManifest {
                    id: ($id).into(), version: ($version).into(),
                    roles: vec![$($crate::role!($role)),*], config_json_schema: None,
                }
            }
            fn configure(_config_json: String) -> Result<(), crabber::extensions::types::StructuredError> { Ok(()) }
        }
        export!($ty);
    };
}
