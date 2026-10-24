//! Explicit Codex CLI model discovery using an already-current access token.
use super::CODEX_BACKEND_URL;
use crate::{AuthError, CredentialProfile, OAuthCredentials};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct CatalogReasoningLevel {
    pub effort: String,
    pub description: String,
}
#[derive(Clone, Debug, Deserialize)]
pub struct CatalogModel {
    pub slug: String,
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    pub default_reasoning_level: Option<String>,
    pub supported_reasoning_levels: Vec<CatalogReasoningLevel>,
    pub priority: i64,
}
pub struct CatalogRequest<'a> {
    pub client_version: &'a str,
    pub base_url: Option<&'a str>,
    pub user_agent: Option<&'a str>,
}
#[derive(Deserialize)]
struct VisibleModel {
    #[serde(flatten)]
    model: CatalogModel,
    visibility: String,
}
#[derive(Deserialize)]
struct Catalog {
    models: Vec<VisibleModel>,
}

pub async fn list_models(
    client: &reqwest::Client,
    credentials: &OAuthCredentials,
    request: CatalogRequest<'_>,
) -> Result<Vec<CatalogModel>, AuthError> {
    if credentials.profile != CredentialProfile::CodexCli {
        return Err(AuthError::ProfileMismatch);
    }
    if !(1..=128).contains(&request.client_version.len())
        || !request
            .client_version
            .bytes()
            .all(|b| (0x20..=0x7e).contains(&b))
    {
        return Err(AuthError::Authentication("invalid client version"));
    }
    let mut url = url::Url::parse(&format!(
        "{}/models",
        request
            .base_url
            .unwrap_or(CODEX_BACKEND_URL)
            .trim_end_matches('/')
    ))
    .map_err(|_| AuthError::Transport)?;
    url.query_pairs_mut()
        .append_pair("client_version", request.client_version);
    let mut call = client
        .get(url)
        .bearer_auth(&credentials.access_token)
        .header("originator", "advisor")
        .header("accept", "application/json")
        .header(
            "user-agent",
            request
                .user_agent
                .unwrap_or(concat!("crabber-auth/", env!("CARGO_PKG_VERSION"))),
        );
    if !credentials.account_id.is_empty() {
        call = call.header("chatgpt-account-id", &credentials.account_id);
    }
    let mut response = call.send().await.map_err(|_| AuthError::Transport)?;
    if matches!(response.status().as_u16(), 401 | 403) {
        return Err(AuthError::Authentication("catalog rejected"));
    }
    if !response.status().is_success() {
        return Err(AuthError::Transport);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| AuthError::Transport)? {
        if bytes.len().saturating_add(chunk.len()) > 8 * 1024 * 1024 {
            return Err(AuthError::Transport);
        }
        bytes.extend_from_slice(&chunk);
    }
    let catalog: Catalog = serde_json::from_slice(&bytes).map_err(|_| AuthError::Transport)?;
    let mut models: Vec<_> = catalog
        .models
        .into_iter()
        .filter(|m| m.visibility == "list")
        .map(|m| m.model)
        .collect();
    models.sort_by_key(|m| m.priority);
    Ok(models)
}
#[cfg(test)]
mod tests;
