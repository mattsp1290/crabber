//! Read-only broker projection. The broker owns token refresh and file rotation.
use crate::{AuthError, CredentialProfile, CredentialStore, OAuthCredentials, codex};
use serde::Deserialize;
use std::{fs::File, io::Read, path::PathBuf};

#[derive(Debug)]
pub struct AccessFileCredentialStore {
    path: PathBuf,
}
impl AccessFileCredentialStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccessFileV1 {
    schema_version: u32,
    state: String,
    access: Option<String>,
    account_id: Option<String>,
    expires_unix_ms: Option<u64>,
    #[serde(rename = "issued_unix_ms")]
    _issued_unix_ms: Option<u64>,
    #[serde(rename = "generation")]
    _generation: u64,
}
impl CredentialStore for AccessFileCredentialStore {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(AuthError::Store),
        };
        let mut bytes = Vec::new();
        file.take(65_537)
            .read_to_end(&mut bytes)
            .map_err(|_| AuthError::Store)?;
        if bytes.len() > 65_536 {
            return Err(AuthError::Store);
        }
        let value: AccessFileV1 = serde_json::from_slice(&bytes).map_err(|_| AuthError::Store)?;
        if value.schema_version != 1 {
            return Err(AuthError::Store);
        }
        match value.state.as_str() {
            "unavailable" if value.access.is_none() && value.account_id.is_none() => Ok(None),
            "available" => {
                let access = value
                    .access
                    .filter(|s| !s.is_empty())
                    .ok_or(AuthError::Store)?;
                let expiry = value
                    .expires_unix_ms
                    .filter(|v| *v > 0)
                    .ok_or(AuthError::Store)?;
                Ok(Some(OAuthCredentials {
                    profile: CredentialProfile::CodexCli,
                    client_id: codex::CODEX_CLI_CLIENT_ID.into(),
                    host_id: String::new(),
                    account_id: value.account_id.unwrap_or_default(),
                    access_token: access,
                    refresh_token: None,
                    id_token: String::new(),
                    scopes: Vec::new(),
                    expires_unix_ms: expiry,
                }))
            }
            _ => Err(AuthError::Store),
        }
    }
    fn save(&self, _: &OAuthCredentials) -> Result<(), AuthError> {
        Err(AuthError::Store)
    }
    fn clear(&self) -> Result<(), AuthError> {
        Err(AuthError::Store)
    }
    fn compare_exchange(
        &self,
        _: &OAuthCredentials,
        _: Option<&OAuthCredentials>,
    ) -> Result<bool, AuthError> {
        Err(AuthError::Store)
    }
    fn host_id(&self) -> Result<String, AuthError> {
        Ok("urn:crabber:access-file".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TokenManager, now_ms};
    use serde_json::{Value, json};
    use std::sync::Arc;
    fn available(token: &str, expiry: u64) -> Value {
        json!({"schema_version":1,"state":"available","access":token,"account_id":"account","expires_unix_ms":expiry,"issued_unix_ms":1,"generation":2})
    }
    #[tokio::test]
    async fn access_only_store_refuses_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("access.json");
        let store = Arc::new(AccessFileCredentialStore::new(path.clone()));
        assert!(store.load().unwrap().is_none());
        std::fs::write(&path, available("stale", now_ms() + 120_000).to_string()).unwrap();
        // An unused listener proves no token HTTP request occurs.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let manager = TokenManager::new(store.clone())
            .with_token_url(format!("http://{}", listener.local_addr().unwrap()));
        assert_eq!(manager.credentials().await.unwrap().access_token, "stale");
        assert!(matches!(
            manager.force_refresh("stale").await,
            Err(AuthError::AccessOnly)
        ));
        std::fs::write(&path, available("rotated", now_ms() + 120_000).to_string()).unwrap();
        assert_eq!(
            manager.force_refresh("stale").await.unwrap().access_token,
            "rotated"
        );
        std::fs::write(&path, available("expired", 1).to_string()).unwrap();
        assert!(matches!(
            manager.credentials().await,
            Err(AuthError::AccessOnly)
        ));
        std::fs::write(
            &path,
            json!({"schema_version":1,"state":"unavailable","generation":3}).to_string(),
        )
        .unwrap();
        assert!(matches!(
            manager.credentials().await,
            Err(AuthError::NoCredentials)
        ));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        let credential = serde_json::from_value::<Value>(available("redacted-secret", 1)).unwrap();
        assert!(!format!("{store:?}").contains(credential["access"].as_str().unwrap()));
        assert!(store.clear().is_err());
        assert_eq!(store.host_id().unwrap(), "urn:crabber:access-file");
    }
    #[test]
    fn access_file_parse_rules() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("access.json");
        let store = AccessFileCredentialStore::new(path.clone());
        let valid = available("access", 1);
        for value in [
            {
                let mut v = valid.clone();
                v["extra"] = json!(true);
                v
            },
            {
                let mut v = valid.clone();
                v["schema_version"] = json!(2);
                v
            },
            {
                let mut v = valid.clone();
                v.as_object_mut().unwrap().remove("access");
                v
            },
            {
                let mut v = valid.clone();
                v["state"] = json!("unavailable");
                v
            },
            {
                let mut v = valid.clone();
                v["access"] = json!("");
                v
            },
            {
                let mut v = valid.clone();
                v["expires_unix_ms"] = json!(0);
                v
            },
        ] {
            std::fs::write(&path, value.to_string()).unwrap();
            assert!(matches!(store.load(), Err(AuthError::Store)));
        }
        std::fs::write(&path, vec![b' '; 65_537]).unwrap();
        assert!(matches!(store.load(), Err(AuthError::Store)));
        std::fs::write(&path, valid.to_string()).unwrap();
        let credential = store.load().unwrap().unwrap();
        assert!(credential.is_access_only());
        assert!(store.save(&credential).is_err());
        assert!(
            store
                .compare_exchange(&credential, Some(&credential))
                .is_err()
        );
        assert_eq!(format!("{credential:?}"), "OAuthCredentials([REDACTED])");
    }
}
