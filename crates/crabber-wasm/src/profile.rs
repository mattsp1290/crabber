//! Pure nonsecret declaration; loading and hash-verifying bytes remains a
//! separate execution preparation step.
use crate::{InstanceMode, Limits, ModuleConfig};
use sha2::{Digest, Sha256};

pub(super) fn config_hash(config: &ModuleConfig) -> String {
    let ModuleConfig {
        name,
        // Artifact location is routing/authorization data. The mandatory digest
        // identifies its bytes; preparation still validates the supplied path.
        path: _,
        allowed_root: _,
        expected_sha256,
        config_json,
        limits,
        instance_mode,
    } = config;
    let Limits {
        max_module_bytes,
        max_memory_bytes,
        max_input_bytes,
        max_output_bytes,
        call_timeout,
        close_drain,
        max_state_entries,
        max_state_value_bytes,
    } = limits;
    let duration = |value: &std::time::Duration| (value.as_secs(), value.subsec_nanos());
    let value = serde_json::json!([
        "crabber.wasm-registration.v1",
        name,
        expected_sha256,
        // The guest receives this exact string; it may inspect whitespace and
        // ordering. Do not normalize it by guessing how the guest parses it.
        config_json,
        match instance_mode {
            InstanceMode::PerCall => "per_call",
            InstanceMode::Persistent => "persistent",
        },
        max_module_bytes,
        max_memory_bytes,
        max_input_bytes,
        max_output_bytes,
        duration(call_timeout),
        duration(close_drain),
        max_state_entries,
        max_state_value_bytes,
    ]);
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Extension, WasmExtension};
    use std::{path::PathBuf, time::Duration};

    fn config() -> ModuleConfig {
        ModuleConfig {
            name: "declared-guest".into(),
            path: PathBuf::from("unloaded/module.wasm"),
            allowed_root: PathBuf::from("unloaded"),
            expected_sha256: [1; 32],
            config_json: r#"{"tool_name":"first"}"#.into(),
            limits: Limits::default(),
            instance_mode: InstanceMode::PerCall,
        }
    }

    #[test]
    fn guest_configuration_is_bound_without_loading_an_unavailable_module() {
        let original = config();
        let fingerprint = WasmExtension::new(original.clone()).config_hash();
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(fingerprint, config_hash(&original));
        let mut changed = original.clone();
        changed.config_json = r#"{"tool_name":"second"}"#.into();
        assert_ne!(fingerprint, WasmExtension::new(changed).config_hash());
        let mut changed = original.clone();
        changed.config_json.push(' ');
        assert_ne!(fingerprint, config_hash(&changed));
        let mut relocated = original;
        relocated.path = PathBuf::from("elsewhere/module.wasm");
        relocated.allowed_root = PathBuf::from("elsewhere");
        assert_eq!(fingerprint, config_hash(&relocated));
    }

    #[test]
    fn resource_and_instance_behavior_changes_reject_the_old_declaration() {
        let original = config();
        let fingerprint = config_hash(&original);
        let variants: Vec<ModuleConfig> = vec![
            ModuleConfig {
                expected_sha256: [2; 32],
                ..original.clone()
            },
            ModuleConfig {
                instance_mode: InstanceMode::Persistent,
                ..original.clone()
            },
            ModuleConfig {
                limits: Limits {
                    call_timeout: Duration::new(u64::MAX, 999_999_999),
                    ..original.limits.clone()
                },
                ..original.clone()
            },
            ModuleConfig {
                limits: Limits {
                    max_memory_bytes: original.limits.max_memory_bytes + 1,
                    ..original.limits.clone()
                },
                ..original.clone()
            },
            ModuleConfig {
                limits: Limits {
                    max_state_entries: original.limits.max_state_entries + 1,
                    ..original.limits.clone()
                },
                ..original
            },
        ];
        for changed in variants {
            assert_ne!(fingerprint, config_hash(&changed));
        }
    }
}
