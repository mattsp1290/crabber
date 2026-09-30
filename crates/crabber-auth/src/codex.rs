//! Sign in with ChatGPT for open-source agents.
//! Current protocol: https://developers.openai.com/siwc/token-sharing-open-source/sign-in
#![allow(clippy::too_many_lines)]
use crate::{AuthError, CredentialStore, OAuthCredentials, now_ms, pkce_challenge, random_urlsafe};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use url::Url;

pub const ISSUER: &str = "https://auth.openai.com";
pub const AUTHORIZE_URL: &str = "https://auth.openai.com/api/accounts/authorize";
pub const TOKEN_URL: &str = "https://auth.openai.com/api/accounts/oauth/token";
pub const JWKS_URL: &str = "https://auth.openai.com/.well-known/jwks.json";
pub const RESOURCE: &str = "https://api.openai.com/v1";
pub const RESPONSES_URL: &str = "https://api.openai.com/v1/responses";
pub const SCOPE: &str =
    "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    NotLoggedIn,
    Valid,
    Refreshable,
}
pub fn status(store: &dyn CredentialStore) -> Result<Status, AuthError> {
    match store.load()? {
        None => Ok(Status::NotLoggedIn),
        Some(c) if c.expires_unix_ms > now_ms() + 60_000 => Ok(Status::Valid),
        Some(_) => Ok(Status::Refreshable),
    }
}
pub fn logout(store: &dyn CredentialStore) -> Result<(), AuthError> {
    store.clear()
}

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: String,
    exp: u64,
    nonce: String,
}
async fn validated_subject(
    client: &reqwest::Client,
    id_token: &str,
    client_id: &str,
    nonce: &str,
) -> Result<String, AuthError> {
    let header =
        decode_header(id_token).map_err(|_| AuthError::Authentication("invalid ID token"))?;
    let kid = header
        .kid
        .ok_or(AuthError::Authentication("missing ID token key"))?;
    let keys: JwkSet = client
        .get(JWKS_URL)
        .send()
        .await
        .map_err(|_| AuthError::Transport)?
        .json()
        .await
        .map_err(|_| AuthError::Transport)?;
    let key = keys
        .find(&kid)
        .ok_or(AuthError::Authentication("untrusted ID token key"))?;
    let decoding = DecodingKey::from_jwk(key)
        .map_err(|_| AuthError::Authentication("invalid ID token key"))?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[client_id]);
    validation.set_issuer(&[ISSUER]);
    let claims = decode::<Claims>(id_token, &decoding, &validation)
        .map_err(|_| AuthError::Authentication("ID token validation failed"))?
        .claims;
    if claims.iss != ISSUER
        || claims.aud != client_id
        || claims.exp * 1000 <= now_ms()
        || claims.nonce != nonce
    {
        return Err(AuthError::Authentication("ID token claims mismatch"));
    }
    Ok(claims.sub)
}

/// Listen for the loopback callback and hand the authorization URL to the host.
/// The host should open it in a browser after the listener is ready.
pub async fn login_browser(
    store: Arc<dyn CredentialStore>,
    on_auth_url: impl FnOnce(&str) + Send,
) -> Result<OAuthCredentials, AuthError> {
    let listener = TcpListener::bind("127.0.0.1:1455")
        .await
        .map_err(|_| AuthError::PortInUse)?;
    let redirect = "http://127.0.0.1:1455/auth/callback";
    let existing = store.load()?;
    let client_id = existing
        .as_ref()
        .map_or("dynamic_agent_client", |c| &c.client_id);
    let host_id = match &existing {
        Some(c) => c.host_id.clone(),
        None => store.host_id()?,
    };
    let verifier = random_urlsafe(32);
    let state = random_urlsafe(32);
    let nonce = random_urlsafe(32);
    let mut url = Url::parse(AUTHORIZE_URL).map_err(|_| AuthError::Transport)?;
    url.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", redirect)
        .append_pair("scope", SCOPE)
        .append_pair("resource", RESOURCE)
        .append_pair("code_challenge", &pkce_challenge(&verifier))
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state)
        .append_pair("nonce", &nonce)
        .append_pair("ext_agent_host_id", &host_id)
        .append_pair("agent_name_hint", "Crabber");
    if let Some(c) = &existing {
        url.query_pairs_mut()
            .append_pair("id_token_hint", &c.id_token);
    }
    on_auth_url(url.as_str());
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(300), listener.accept())
        .await
        .map_err(|_| AuthError::Authentication("callback timed out"))?
        .map_err(|_| AuthError::Transport)?;
    let mut buffer = [0; 8192];
    let n = socket
        .read(&mut buffer)
        .await
        .map_err(|_| AuthError::Transport)?;
    let line = std::str::from_utf8(&buffer[..n])
        .map_err(|_| AuthError::Authentication("invalid callback"))?
        .lines()
        .next()
        .ok_or(AuthError::Authentication("empty callback"))?;
    let path = line
        .strip_prefix("GET ")
        .and_then(|s| s.split_whitespace().next())
        .ok_or(AuthError::Authentication("invalid callback"))?;
    let callback = Url::parse(&format!("http://127.0.0.1:1455{path}"))
        .map_err(|_| AuthError::Authentication("invalid callback"))?;
    if callback.path() != "/auth/callback" {
        return Err(AuthError::Authentication("invalid callback path"));
    }
    let query: std::collections::HashMap<_, _> = callback.query_pairs().into_owned().collect();
    let result: Result<_, AuthError> = async {
        if query.get("state") != Some(&state) {
            return Err(AuthError::Authentication("state mismatch"));
        }
        if query.contains_key("error") {
            return Err(AuthError::Authentication("authorization denied"));
        }
        let code = query
            .get("code")
            .ok_or(AuthError::Authentication("missing code"))?;
        let issued_id = match (&existing, query.get("client_id")) {
            (None, Some(id)) if id != "dynamic_agent_client" => id.clone(),
            (Some(c), None) => c.client_id.clone(),
            (Some(c), Some(id)) if id == &c.client_id => id.clone(),
            _ => return Err(AuthError::Authentication("client registration mismatch")),
        };
        let client = reqwest::Client::new();
        let response = client
            .post(TOKEN_URL)
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", issued_id.as_str()),
                ("code", code.as_str()),
                ("code_verifier", verifier.as_str()),
                ("redirect_uri", redirect),
                ("resource", RESOURCE),
            ])
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if !response.status().is_success() {
            return Err(AuthError::Authentication("token exchange rejected"));
        }
        let token: serde_json::Value = response.json().await.map_err(|_| AuthError::Transport)?;
        let id_token = token["id_token"]
            .as_str()
            .ok_or(AuthError::Authentication("missing ID token"))?;
        let subject = validated_subject(&client, id_token, &issued_id, &nonce).await?;
        if existing.as_ref().is_some_and(|c| c.account_id != subject) {
            return Err(AuthError::Authentication("account changed"));
        }
        let scopes: Vec<String> = token["scope"]
            .as_str()
            .unwrap_or_default()
            .split_whitespace()
            .map(str::to_owned)
            .collect();
        if !scopes.iter().any(|s| s == "chatgpt.tokens.use.direct") {
            return Err(AuthError::Authentication("plan usage not granted"));
        }
        let creds = OAuthCredentials {
            client_id: issued_id,
            host_id,
            account_id: subject,
            access_token: token["access_token"]
                .as_str()
                .ok_or(AuthError::Transport)?
                .into(),
            refresh_token: token["refresh_token"]
                .as_str()
                .ok_or(AuthError::Transport)?
                .into(),
            id_token: id_token.into(),
            scopes,
            expires_unix_ms: now_ms() + token["expires_in"].as_u64().unwrap_or(3600) * 1000,
        };
        store.save(&creds)?;
        Ok(creds)
    }
    .await;
    let body = if result.is_ok() {
        "Sign-in complete. You may close this tab."
    } else {
        "Sign-in failed. Return to the terminal."
    };
    let reply = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = socket.write_all(reply.as_bytes()).await;
    result
}
