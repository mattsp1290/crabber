//! Finite authentication responses, including error bodies and JWKS.
use crate::AuthError;
use serde::de::DeserializeOwned;

pub(crate) const MAX_AUTH_BODY_BYTES: usize = 1 << 20;

pub(crate) async fn bytes(mut response: reqwest::Response) -> Result<Vec<u8>, AuthError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_AUTH_BODY_BYTES as u64)
    {
        return Err(AuthError::Authentication(
            "authentication response too large",
        ));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| AuthError::Transport)? {
        if chunk.len() > MAX_AUTH_BODY_BYTES.saturating_sub(body.len()) {
            return Err(AuthError::Authentication(
                "authentication response too large",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(crate) async fn json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, AuthError> {
    serde_json::from_slice(&bytes(response).await?).map_err(|_| AuthError::Transport)
}
