//! Codex CLI device login. A successful login replaces any prior account/profile.
use super::{
    CODEX_CLI_CLIENT_ID, CODEX_CLI_TOKEN_URL, DEVICE_REDIRECT_URI, DEVICE_TOKEN_URL,
    DEVICE_USERCODE_URL, DEVICE_VERIFICATION_URL, JWKS_URL, account_id_from_claims,
    validated_claims,
};
use crate::{AuthError, CredentialProfile, CredentialStore, OAuthCredentials, now_ms};
use serde_json::{Value, json};
use std::{future::Future, sync::Arc, time::Duration};

#[derive(Clone, Debug)]
pub struct DeviceEndpoints {
    pub usercode_url: String,
    pub token_url: String,
    pub exchange_url: String,
    pub jwks_url: String,
    pub verification_url: String,
    pub client_id: String,
}
impl Default for DeviceEndpoints {
    fn default() -> Self {
        Self {
            usercode_url: DEVICE_USERCODE_URL.into(),
            token_url: DEVICE_TOKEN_URL.into(),
            exchange_url: CODEX_CLI_TOKEN_URL.into(),
            jwks_url: JWKS_URL.into(),
            verification_url: DEVICE_VERIFICATION_URL.into(),
            client_id: CODEX_CLI_CLIENT_ID.into(),
        }
    }
}
#[derive(Debug)]
pub struct DevicePrompt {
    pub verification_url: String,
    pub user_code: String,
    pub expires_at_unix_ms: u64,
}
pub struct DeviceLogin {
    store: Arc<dyn CredentialStore>,
    endpoints: DeviceEndpoints,
    poll_cap: Duration,
    poll_floor: Duration,
    client: reqwest::Client,
}
impl DeviceLogin {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self {
            store,
            endpoints: DeviceEndpoints::default(),
            poll_cap: Duration::from_mins(15),
            poll_floor: Duration::from_secs(3),
            client: reqwest::Client::new(),
        }
    }
    #[must_use]
    pub fn with_endpoints(mut self, endpoints: DeviceEndpoints) -> Self {
        self.endpoints = endpoints;
        self
    }
    #[must_use]
    pub fn with_poll_cap(mut self, cap: Duration) -> Self {
        self.poll_cap = cap;
        self
    }
    #[must_use]
    pub fn with_poll_floor(mut self, floor: Duration) -> Self {
        self.poll_floor = floor;
        self
    }
    pub async fn run(
        self,
        on_prompt: impl FnOnce(&DevicePrompt) + Send,
        cancel: impl Future<Output = ()> + Send,
    ) -> Result<OAuthCredentials, AuthError> {
        tokio::pin!(cancel);
        // Cancellation and the cap also cover slow HTTP responses and verification.
        let expires_at_unix_ms =
            now_ms().saturating_add(u64::try_from(self.poll_cap.as_millis()).unwrap_or(u64::MAX));
        let expiry = tokio::time::sleep(self.poll_cap);
        tokio::pin!(expiry);
        tokio::select! {
            biased;
            () = &mut cancel => Err(AuthError::Cancelled),
            () = &mut expiry => Err(AuthError::DeviceExpired),
            result = self.login(on_prompt, expires_at_unix_ms) => result,
        }
    }
    async fn login(
        &self,
        on_prompt: impl FnOnce(&DevicePrompt) + Send,
        expires_at_unix_ms: u64,
    ) -> Result<OAuthCredentials, AuthError> {
        let response = self
            .client
            .post(&self.endpoints.usercode_url)
            .json(&json!({"client_id": self.endpoints.client_id}))
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(AuthError::Authentication("device authorization refused"));
        }
        let body: Value = crate::http_body::json(response).await?;
        let device_auth_id = required(&body, "device_auth_id")?;
        let user_code = required(&body, "user_code")?;
        if !(4..=16).contains(&user_code.len())
            || !user_code
                .bytes()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'-')
        {
            return Err(AuthError::Authentication("invalid device user code"));
        }
        let seconds = body["interval"]
            .as_u64()
            .or_else(|| body["interval"].as_str()?.parse().ok())
            .unwrap_or(5);
        let interval = Duration::from_secs(seconds)
            .saturating_add(self.poll_floor)
            .min(self.poll_cap);
        on_prompt(&DevicePrompt {
            verification_url: self.endpoints.verification_url.clone(),
            user_code: user_code.into(),
            expires_at_unix_ms,
        });
        let authorization = loop {
            tokio::time::sleep(interval).await;
            let response = self
                .client
                .post(&self.endpoints.token_url)
                .json(&json!({"device_auth_id": device_auth_id, "user_code": user_code}))
                .send()
                .await
                .map_err(|_| AuthError::Transport)?;
            let status = response.status();
            let bytes = crate::http_body::bytes(response).await?;
            let body: Value = serde_json::from_slice(&bytes).unwrap_or_default();
            if body["error"] == "access_denied" {
                return Err(AuthError::DeviceDenied);
            }
            if status == reqwest::StatusCode::GONE || body["error"] == "expired_token" {
                return Err(AuthError::DeviceExpired);
            }
            match status.as_u16() {
                200 => break body,
                403 | 404 => {}
                _ => return Err(AuthError::Authentication("device poll rejected")),
            }
        };
        let response = self
            .client
            .post(&self.endpoints.exchange_url)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", self.endpoints.client_id.as_str()),
                ("code", required(&authorization, "authorization_code")?),
                ("code_verifier", required(&authorization, "code_verifier")?),
                ("redirect_uri", DEVICE_REDIRECT_URI),
            ])
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(AuthError::Authentication("token exchange rejected"));
        }
        let token: Value = crate::http_body::json(response).await?;
        let id_token = required(&token, "id_token")?;
        let claims = validated_claims(
            &self.client,
            &self.endpoints.jwks_url,
            id_token,
            &self.endpoints.client_id,
            None,
        )
        .await?;
        let creds = OAuthCredentials {
            profile: CredentialProfile::CodexCli,
            client_id: self.endpoints.client_id.clone(),
            host_id: self.store.host_id()?,
            account_id: account_id_from_claims(&claims)
                .ok_or(AuthError::Authentication("missing account id"))?,
            access_token: required(&token, "access_token")?.into(),
            refresh_token: Some(required(&token, "refresh_token")?.into()),
            id_token: id_token.into(),
            scopes: token["scope"]
                .as_str()
                .unwrap_or_default()
                .split_whitespace()
                .map(str::to_owned)
                .collect(),
            expires_unix_ms: now_ms().saturating_add(
                token["expires_in"]
                    .as_u64()
                    .unwrap_or(3600)
                    .saturating_mul(1000),
            ),
        };
        self.store.save(&creds)?;
        Ok(creds)
    }
}
fn required<'a>(body: &'a Value, field: &str) -> Result<&'a str, AuthError> {
    body[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or(AuthError::Authentication("device response missing field"))
}
#[cfg(test)]
mod tests;
