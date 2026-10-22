use crate::{
    AuthError, CredentialProfile, CredentialStore, OAuthCredentials, acquire_refresh_lock, now_ms,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct TokenManager {
    store: Arc<dyn CredentialStore>,
    client: reqwest::Client,
    refresh: Mutex<()>,
    token_url: Option<String>,
    expected_profile: Option<CredentialProfile>,
}
impl TokenManager {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self {
            store,
            client: reqwest::Client::new(),
            refresh: Mutex::new(()),
            token_url: None,
            expected_profile: None,
        }
    }
    #[must_use]
    pub fn with_token_url(mut self, url: impl Into<String>) -> Self {
        self.token_url = Some(url.into());
        self
    }
    /// Reject another profile before attempting a refresh or returning credentials.
    #[must_use]
    pub fn with_profile(mut self, profile: CredentialProfile) -> Self {
        self.expected_profile = Some(profile);
        self
    }
    fn check_profile(&self, credentials: &OAuthCredentials) -> Result<(), AuthError> {
        if self
            .expected_profile
            .is_some_and(|profile| profile != credentials.profile)
        {
            return Err(AuthError::ProfileMismatch);
        }
        Ok(())
    }
    pub async fn credentials(&self) -> Result<OAuthCredentials, AuthError> {
        self.ensure(false, None).await
    }
    pub async fn force_refresh(
        &self,
        previous_access: &str,
    ) -> Result<OAuthCredentials, AuthError> {
        self.ensure(true, Some(previous_access)).await
    }
    async fn ensure(
        &self,
        force: bool,
        previous_access: Option<&str>,
    ) -> Result<OAuthCredentials, AuthError> {
        let existing = self.store.load()?.ok_or(AuthError::NoCredentials)?;
        self.check_profile(&existing)?;
        if !force && existing.expires_unix_ms > now_ms() + 60_000 {
            return Ok(existing);
        }
        if existing.is_access_only() && !force {
            return Err(AuthError::AccessOnly);
        }
        let _guard = self.refresh.lock().await;
        if existing.is_access_only() {
            let current = self.store.load()?.ok_or(AuthError::NoCredentials)?;
            self.check_profile(&current)?;
            return if previous_access.is_some_and(|previous| current.access_token != previous) {
                Ok(current)
            } else {
                Err(AuthError::AccessOnly)
            };
        }
        // Keep this OS advisory lock across reload, network exchange and CAS.
        // Rotating refresh tokens must never be used concurrently by processes.
        let _process_guard = acquire_refresh_lock(self.store.refresh_lock()).await?;
        let current = self.store.load()?.ok_or(AuthError::NoCredentials)?;
        self.check_profile(&current)?;
        if (!force && current.expires_unix_ms > now_ms() + 60_000)
            || (force && previous_access.is_some_and(|previous| current.access_token != previous))
        {
            return Ok(current);
        }
        if current.is_access_only() {
            return Err(AuthError::AccessOnly);
        }
        let response = self
            .client
            .post(
                self.token_url
                    .as_deref()
                    .unwrap_or(current.profile.token_url()),
            )
            .form(&current.profile.refresh_form(&current))
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if !response.status().is_success() {
            let body: serde_json::Value = response.json().await.unwrap_or_default();
            if body["error"] == "invalid_grant" && !self.store.compare_exchange(&current, None)? {
                return self.store.load()?.ok_or(AuthError::NoCredentials);
            }
            return Err(AuthError::Authentication("refresh rejected"));
        }
        let body: serde_json::Value = response.json().await.map_err(|_| AuthError::Transport)?;
        let mut updated = current.clone();
        updated.access_token = body["access_token"]
            .as_str()
            .ok_or(AuthError::Transport)?
            .into();
        updated.refresh_token = body["refresh_token"]
            .as_str()
            .map(str::to_owned)
            .or(updated.refresh_token);
        updated.expires_unix_ms = now_ms() + body["expires_in"].as_u64().unwrap_or(3600) * 1000;
        if self.store.compare_exchange(&current, Some(&updated))? {
            Ok(updated)
        } else {
            self.store.load()?.ok_or(AuthError::NoCredentials)
        }
    }
}

#[cfg(test)]
mod refresh_tests {
    use super::*;
    use crate::{FileCredentialStore, MemoryCredentialStore, codex};
    use fs4::fs_std::FileExt;
    use std::{fs::OpenOptions, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    fn expired() -> OAuthCredentials {
        OAuthCredentials {
            profile: CredentialProfile::Siwc,
            client_id: "client".into(),
            host_id: "host".into(),
            account_id: "account".into(),
            access_token: "old".into(),
            refresh_token: Some("refresh".into()),
            id_token: "id".into(),
            scopes: vec!["chatgpt.tokens.use.direct".into()],
            expires_unix_ms: 1,
        }
    }
    async fn server(body: &'static str, status: &'static str) -> String {
        server_delayed(body, status, Duration::ZERO).await
    }
    async fn server_delayed(body: &'static str, status: &'static str, delay: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = [0; 4096];
            let _ = socket.read(&mut input).await;
            tokio::time::sleep(delay).await;
            let reply = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
        });
        format!("http://{addr}/token")
    }
    #[tokio::test]
    async fn wrong_profile_fails_before_refresh_io() {
        let store = Arc::new(MemoryCredentialStore::default());
        store.save(&expired()).unwrap();
        let manager = TokenManager::new(store)
            .with_profile(CredentialProfile::CodexCli)
            .with_token_url("http://127.0.0.1:1/token");
        assert!(matches!(
            manager.credentials().await,
            Err(AuthError::ProfileMismatch)
        ));
        assert!(matches!(
            manager.force_refresh("old").await,
            Err(AuthError::ProfileMismatch)
        ));
    }
    #[tokio::test]
    async fn refresh_without_rotation_preserves_identity_and_refresh_token() {
        let store = Arc::new(MemoryCredentialStore::default());
        let mut original = expired();
        original.profile = CredentialProfile::CodexCli;
        store.save(&original).unwrap();
        let url = server(r#"{"access_token":"new"}"#, "200 OK").await;
        let updated = TokenManager::new(store)
            .with_token_url(url)
            .credentials()
            .await
            .unwrap();
        assert_eq!(updated.refresh_token, original.refresh_token);
        assert_eq!(updated.profile, original.profile);
        assert_eq!(updated.client_id, original.client_id);
        assert_eq!(updated.account_id, original.account_id);
        assert_eq!(updated.host_id, original.host_id);
        assert_eq!(updated.scopes, original.scopes);
        assert_eq!(updated.id_token, original.id_token);
    }
    #[tokio::test]
    async fn refresh_is_single_flight() {
        let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::default());
        store.save(&expired()).unwrap();
        let url = server(
            r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#,
            "200 OK",
        )
        .await;
        let manager = Arc::new(TokenManager::new(store).with_token_url(url));
        let (a, b) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(manager.credentials(), manager.credentials())
        })
        .await
        .unwrap();
        assert_eq!(a.unwrap().access_token, "new");
        assert_eq!(b.unwrap().refresh_token.as_deref(), Some("rotated"));
    }
    #[tokio::test]
    async fn independent_managers_share_file_refresh_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth");
        let first_store: Arc<dyn CredentialStore> =
            Arc::new(FileCredentialStore::new(path.clone()));
        let second_store: Arc<dyn CredentialStore> = Arc::new(FileCredentialStore::new(path));
        first_store.save(&expired()).unwrap();
        // This server handles exactly one refresh. A second request cannot finish.
        let url = server_delayed(
            r#"{"access_token":"new","refresh_token":"rotated","expires_in":3600}"#,
            "200 OK",
            Duration::from_millis(100),
        )
        .await;
        let first = TokenManager::new(Arc::clone(&first_store)).with_token_url(url.clone());
        let second = TokenManager::new(Arc::clone(&second_store)).with_token_url(url);
        let (a, b) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(first.credentials(), second.credentials())
        })
        .await
        .unwrap();
        assert_eq!(a.unwrap().access_token, "new");
        assert_eq!(b.unwrap().refresh_token.as_deref(), Some("rotated"));
        assert_eq!(
            first_store
                .load()
                .unwrap()
                .unwrap()
                .refresh_token
                .as_deref(),
            Some("rotated")
        );
    }
    #[test]
    fn refresh_form_per_profile() {
        let current = expired();
        let siwc: std::collections::HashMap<_, _> = CredentialProfile::Siwc
            .refresh_form(&current)
            .into_iter()
            .collect();
        let cli: std::collections::HashMap<_, _> = CredentialProfile::CodexCli
            .refresh_form(&current)
            .into_iter()
            .collect();
        assert_eq!(
            siwc.get("resource").map(String::as_str),
            Some(codex::RESOURCE)
        );
        assert!(!siwc.contains_key("scope"));
        assert!(!cli.contains_key("resource"));
        assert_eq!(
            cli.get("scope").map(String::as_str),
            Some(codex::CODEX_CLI_SCOPE)
        );
        assert_ne!(
            CredentialProfile::Siwc.token_url(),
            CredentialProfile::CodexCli.token_url()
        );
        let mut old = serde_json::to_value(current).unwrap();
        old.as_object_mut().unwrap().remove("profile");
        assert_eq!(
            serde_json::from_value::<OAuthCredentials>(old)
                .unwrap()
                .profile,
            CredentialProfile::Siwc
        );
    }
    struct CompetingStore {
        inner: MemoryCredentialStore,
    }
    impl CredentialStore for CompetingStore {
        fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
            self.inner.load()
        }
        fn save(&self, value: &OAuthCredentials) -> Result<(), AuthError> {
            self.inner.save(value)
        }
        fn clear(&self) -> Result<(), AuthError> {
            self.inner.clear()
        }
        fn compare_exchange(
            &self,
            expected: &OAuthCredentials,
            next: Option<&OAuthCredentials>,
        ) -> Result<bool, AuthError> {
            let mut competitor = expected.clone();
            competitor.access_token = "competitor".into();
            competitor.refresh_token = Some("competitor-refresh".into());
            self.inner.save(&competitor)?;
            self.inner.compare_exchange(expected, next)
        }
    }
    #[tokio::test]
    async fn refresh_rotation_survives_lost_write_cas() {
        let store = Arc::new(CompetingStore {
            inner: MemoryCredentialStore::default(),
        });
        store.save(&expired()).unwrap();
        let url = server(
            r#"{"access_token":"discarded","refresh_token":"discarded-refresh"}"#,
            "200 OK",
        )
        .await;
        let result = TokenManager::new(store.clone())
            .with_token_url(url)
            .credentials()
            .await
            .unwrap();
        assert_eq!(result.access_token, "competitor");
        assert_eq!(result.refresh_token.as_deref(), Some("competitor-refresh"));
        assert_eq!(store.load().unwrap().unwrap().access_token, "competitor");
    }
    #[tokio::test]
    async fn invalid_grant_wipes_file() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn CredentialStore> =
            Arc::new(FileCredentialStore::new(dir.path().join("auth")));
        store.save(&expired()).unwrap();
        let url = server(r#"{"error":"invalid_grant"}"#, "400 Bad Request").await;
        let manager = TokenManager::new(Arc::clone(&store)).with_token_url(url);
        assert!(manager.credentials().await.is_err());
        assert!(store.load().unwrap().is_none());
    }
    #[test]
    fn host_id_persists_and_lock_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileCredentialStore::new(dir.path().join("auth"))
            .with_lock_timeout(Duration::from_millis(30));
        assert_eq!(store.host_id().unwrap(), store.host_id().unwrap());
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(store.dir.join("auth.json.lock"))
            .unwrap();
        lock.lock_exclusive().unwrap();
        assert!(store.load().is_err());
        lock.unlock().unwrap();
    }
}
