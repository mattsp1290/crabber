//! Pure declarations for built-in HTTP backends. Opaque custom transports
//! require the host's explicit VersionedResolver/VersionedAdapter declaration.
use super::{AdapterKind, HttpAdapter, Selection, endpoint_url};
use crate::ProviderBindingError;
use crabber_core::{InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits};
use sha2::{Digest, Sha256};

impl HttpAdapter {
    pub(super) fn builtin_binding(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        if selection.provider_id != self.id || selection.model_id.is_empty() {
            return Err(ProviderBindingError::InvalidDescriptor);
        }
        let backend: Option<&str> = match self.kind {
            #[cfg(feature = "anthropic")]
            AdapterKind::Anthropic => Some("anthropic"),
            #[cfg(feature = "openai")]
            AdapterKind::OpenAi => Some("openai"),
            #[cfg(feature = "codex")]
            AdapterKind::Codex => Some("codex"),
            #[cfg(feature = "opencode-go")]
            AdapterKind::OpenCode => Some("opencode-go"),
            #[cfg(feature = "custom-http")]
            AdapterKind::Custom => None,
        };
        let backend = backend.ok_or(ProviderBindingError::Unavailable)?;
        let endpoint = endpoint_url(&self.base_url, self.protocol)
            .map_err(|_| ProviderBindingError::InvalidDescriptor)?;
        if !endpoint.username().is_empty() || endpoint.password().is_some() {
            return Err(ProviderBindingError::InvalidDescriptor);
        }
        // Built-in adapters have fixed client/header policies and token encoding.
        // Credential source contents and refreshed tokens are deliberately absent.
        let config = serde_json::json!([
            "crabber.builtin-http.v1",
            backend,
            endpoint.as_str(),
            self.user_agent
                .to_str()
                .map_err(|_| ProviderBindingError::InvalidDescriptor)?,
        ]);
        // Codex CLI has a distinct account-header and credential-profile policy,
        // even when a host routes both stacks through the same proxy endpoint.
        // Leave legacy SIWC and other provider declarations byte-for-byte stable.
        #[cfg(feature = "codex")]
        let config = if self.codex_profile == Some(crabber_auth::CredentialProfile::CodexCli) {
            let mut values = config.as_array().expect("binding is an array").clone();
            values.push(serde_json::json!("codex-cli"));
            serde_json::Value::Array(values)
        } else {
            config
        };
        let config =
            serde_json::to_vec(&config).map_err(|_| ProviderBindingError::InvalidDescriptor)?;
        let config = InputFingerprint::new(format!("{:x}", Sha256::digest(config)))
            .map_err(|_| ProviderBindingError::InvalidDescriptor)?;
        ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            backend,
            concat!(env!("CARGO_PKG_VERSION"), "/http-binding.v3"),
            config,
            ProviderBindingLimits {
                context_tokens: None,
                output_tokens: None,
            },
        )
        .map_err(|_| ProviderBindingError::InvalidDescriptor)
    }
}

#[cfg(all(test, feature = "opencode-go"))]
mod tests {
    use super::*;
    use crate::{HttpResolver, Protocol, ProviderAdapter, Resolver};

    #[test]
    fn builtin_metadata_binds_route_protocol_and_user_agent_without_credentials() {
        let selection = Selection {
            provider_id: "opencode-go".into(),
            model_id: "model".into(),
        };
        let adapter =
            HttpAdapter::opencode_go(Protocol::Responses).with_base_url("http://127.0.0.1:1/v1");
        let original = adapter.binding_descriptor(&selection).unwrap();
        assert_eq!(
            original.limits(),
            ProviderBindingLimits {
                context_tokens: None,
                output_tokens: None
            }
        );
        // No server exists and no environment credential is required for metadata.
        let resolver = HttpResolver::new().with_adapter(adapter.clone());
        assert_eq!(resolver.binding_descriptor(&selection).unwrap(), original);
        for changed in [
            adapter.clone().with_base_url("http://127.0.0.1:2/v1"),
            HttpAdapter::opencode_go(Protocol::ChatCompletions)
                .with_base_url("http://127.0.0.1:1/v1"),
            adapter
                .clone()
                .try_with_user_agent_product("host/2")
                .unwrap(),
        ] {
            assert_ne!(changed.binding_descriptor(&selection).unwrap(), original);
        }
        for value in ["fixture-token-one", "fixture-token-two"] {
            assert_eq!(
                adapter
                    .clone()
                    .with_api_key(value)
                    .binding_descriptor(&selection)
                    .unwrap(),
                original
            );
        }
        let credential_url =
            adapter.with_base_url("http://fixture-user:fixture-password@127.0.0.1:1/v1");
        assert_eq!(
            credential_url.binding_descriptor(&selection),
            Err(ProviderBindingError::InvalidDescriptor)
        );
    }
}

#[cfg(all(test, feature = "codex"))]
mod codex_tests {
    use super::*;
    #[test]
    fn codex_profiles_have_distinct_route_fingerprints() {
        let store = std::sync::Arc::new(crabber_auth::MemoryCredentialStore::default());
        let selection = Selection {
            provider_id: "codex".into(),
            model_id: "model".into(),
        };
        let siwc = HttpAdapter::codex(store.clone())
            .builtin_binding(&selection)
            .unwrap();
        let cli = HttpAdapter::codex_cli(store)
            .builtin_binding(&selection)
            .unwrap();
        assert_eq!(siwc.backend_id(), "codex");
        assert_eq!(cli.backend_id(), "codex");
        assert_ne!(siwc.config_fingerprint(), cli.config_fingerprint());
    }

    #[test]
    fn same_proxy_preserves_profile_identity_without_binding_tokens() {
        use crabber_auth::{CredentialProfile, CredentialStore, OAuthCredentials};
        let store = std::sync::Arc::new(crabber_auth::MemoryCredentialStore::default());
        let selection = Selection {
            provider_id: "codex".into(),
            model_id: "model".into(),
        };
        let proxy = "http://127.0.0.1:1/codex-proxy";
        let siwc = HttpAdapter::codex(store.clone()).with_base_url(proxy);
        let cli = HttpAdapter::codex_cli(store.clone()).with_base_url(proxy);
        let original = cli.builtin_binding(&selection).unwrap();
        assert_ne!(siwc.builtin_binding(&selection).unwrap(), original);
        for token in ["first-token", "rotated-token"] {
            store
                .save(&OAuthCredentials {
                    profile: CredentialProfile::CodexCli,
                    client_id: "client".into(),
                    host_id: "host".into(),
                    account_id: "account".into(),
                    access_token: token.into(),
                    refresh_token: None,
                    id_token: String::new(),
                    scopes: vec![],
                    expires_unix_ms: u64::MAX,
                })
                .unwrap();
            assert_eq!(cli.builtin_binding(&selection).unwrap(), original);
        }
    }
}
