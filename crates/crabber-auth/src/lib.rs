//! ChatGPT subscription sign-in and protected local credentials.
#![allow(
    clippy::missing_errors_doc,
    clippy::must_use_candidate,
    clippy::doc_markdown
)]
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fs4::fs_std::FileExt;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;

pub mod codex;

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("no ChatGPT credentials; run codex-login")]
    NoCredentials,
    #[error("authentication failed: {0}")]
    Authentication(&'static str),
    #[error("authentication service unavailable")]
    Transport,
    #[error("callback port in use")]
    PortInUse,
    #[error("credential store failed")]
    Store,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthCredentials {
    pub client_id: String,
    pub host_id: String,
    pub account_id: String,
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
    pub scopes: Vec<String>,
    pub expires_unix_ms: u64,
}
impl std::fmt::Debug for OAuthCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OAuthCredentials([REDACTED])")
    }
}

pub trait CredentialStore: Send + Sync {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError>;
    fn save(&self, value: &OAuthCredentials) -> Result<(), AuthError>;
    fn clear(&self) -> Result<(), AuthError>;
    fn host_id(&self) -> Result<String, AuthError> {
        Ok(format!("urn:uuid:{}", uuid::Uuid::new_v4()))
    }
}

#[derive(Clone)]
pub struct FileCredentialStore {
    dir: PathBuf,
    lock_timeout: Duration,
}
impl FileCredentialStore {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            lock_timeout: Duration::from_secs(30),
        }
    }
    #[must_use]
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }
    pub fn default_crabber() -> Self {
        let base = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
            .unwrap_or_else(|| PathBuf::from("."));
        Self::new(base.join("crabber"))
    }
    fn locked<T>(&self, f: impl FnOnce(&PathBuf) -> Result<T, AuthError>) -> Result<T, AuthError> {
        fs::create_dir_all(&self.dir).map_err(|_| AuthError::Store)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.dir.join("auth.json.lock"))
            .map_err(|_| AuthError::Store)?;
        let start = std::time::Instant::now();
        loop {
            if lock.try_lock_exclusive().map_err(|_| AuthError::Store)? {
                break;
            }
            if start.elapsed() >= self.lock_timeout {
                return Err(AuthError::Store);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let result = f(&self.dir.join("auth.json"));
        let _ = lock.unlock();
        result
    }
}
impl CredentialStore for FileCredentialStore {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
        self.locked(|path| {
            let mut file = match OpenOptions::new().read(true).open(path) {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(_) => return Err(AuthError::Store),
            };
            let mut data = String::new();
            file.read_to_string(&mut data)
                .map_err(|_| AuthError::Store)?;
            serde_json::from_str(&data)
                .map(Some)
                .map_err(|_| AuthError::Store)
        })
    }
    fn save(&self, value: &OAuthCredentials) -> Result<(), AuthError> {
        self.locked(|path| {
            let temp = path.with_extension("json.tmp");
            let mut opts = OpenOptions::new();
            opts.create(true).truncate(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut file = opts.open(&temp).map_err(|_| AuthError::Store)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(fs::Permissions::from_mode(0o600))
                    .map_err(|_| AuthError::Store)?;
            }
            serde_json::to_writer(&mut file, value).map_err(|_| AuthError::Store)?;
            file.flush().map_err(|_| AuthError::Store)?;
            file.sync_all().map_err(|_| AuthError::Store)?;
            fs::rename(temp, path).map_err(|_| AuthError::Store)
        })
    }
    fn clear(&self) -> Result<(), AuthError> {
        self.locked(|path| match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(AuthError::Store),
        })
    }
    fn host_id(&self) -> Result<String, AuthError> {
        self.locked(|path| {
            let host_path = path.with_file_name("host-id");
            if let Ok(value) = fs::read_to_string(&host_path) {
                return Ok(value);
            }
            let value = format!("urn:uuid:{}", uuid::Uuid::new_v4());
            let mut opts = OpenOptions::new();
            opts.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut file = opts.open(host_path).map_err(|_| AuthError::Store)?;
            file.write_all(value.as_bytes())
                .map_err(|_| AuthError::Store)?;
            file.sync_all().map_err(|_| AuthError::Store)?;
            Ok(value)
        })
    }
}

#[derive(Default)]
pub struct MemoryCredentialStore(StdMutex<Option<OAuthCredentials>>);
impl CredentialStore for MemoryCredentialStore {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
        Ok(self.0.lock().map_err(|_| AuthError::Store)?.clone())
    }
    fn save(&self, value: &OAuthCredentials) -> Result<(), AuthError> {
        *self.0.lock().map_err(|_| AuthError::Store)? = Some(value.clone());
        Ok(())
    }
    fn clear(&self) -> Result<(), AuthError> {
        *self.0.lock().map_err(|_| AuthError::Store)? = None;
        Ok(())
    }
}

pub fn random_urlsafe(bytes: usize) -> String {
    let mut out = vec![0; bytes];
    rand::rng().fill_bytes(&mut out);
    URL_SAFE_NO_PAD.encode(out)
}
pub fn pkce_challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub struct TokenManager {
    store: Arc<dyn CredentialStore>,
    client: reqwest::Client,
    refresh: Mutex<()>,
    token_url: String,
}
impl TokenManager {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self {
            store,
            client: reqwest::Client::new(),
            refresh: Mutex::new(()),
            token_url: codex::TOKEN_URL.into(),
        }
    }
    #[must_use]
    pub fn with_token_url(mut self, url: impl Into<String>) -> Self {
        self.token_url = url.into();
        self
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
        if !force && existing.expires_unix_ms > now_ms() + 60_000 {
            return Ok(existing);
        }
        let _guard = self.refresh.lock().await;
        let current = self.store.load()?.ok_or(AuthError::NoCredentials)?;
        if (!force && current.expires_unix_ms > now_ms() + 60_000)
            || (force && previous_access.is_some_and(|previous| current.access_token != previous))
        {
            return Ok(current);
        }
        let response = self
            .client
            .post(&self.token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", current.client_id.as_str()),
                ("refresh_token", current.refresh_token.as_str()),
                ("resource", codex::RESOURCE),
            ])
            .send()
            .await
            .map_err(|_| AuthError::Transport)?;
        if !response.status().is_success() {
            let body: serde_json::Value = response.json().await.unwrap_or_default();
            if body["error"] == "invalid_grant" {
                self.store.clear()?;
            }
            return Err(AuthError::Authentication("refresh rejected"));
        }
        let body: serde_json::Value = response.json().await.map_err(|_| AuthError::Transport)?;
        let mut updated = current;
        updated.access_token = body["access_token"]
            .as_str()
            .ok_or(AuthError::Transport)?
            .into();
        updated.refresh_token = body["refresh_token"]
            .as_str()
            .unwrap_or(&updated.refresh_token)
            .into();
        updated.expires_unix_ms = now_ms() + body["expires_in"].as_u64().unwrap_or(3600) * 1000;
        self.store.save(&updated)?;
        Ok(updated)
    }
}

pub fn device_poll_interval(value: &str) -> Result<Duration, AuthError> {
    let seconds: u64 = value
        .parse()
        .map_err(|_| AuthError::Authentication("invalid device interval"))?;
    Ok(Duration::from_secs(seconds.saturating_add(3)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pkce_vector() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }
    #[test]
    fn interval() {
        assert_eq!(device_poll_interval("5").unwrap(), Duration::from_secs(8));
    }
    #[test]
    fn file_mode() {
        let store = FileCredentialStore::new(tempfile::tempdir().unwrap().path().join("auth"));
        let value = OAuthCredentials {
            client_id: "c".into(),
            host_id: "h".into(),
            account_id: "a".into(),
            access_token: "secret".into(),
            refresh_token: "secret".into(),
            id_token: "secret".into(),
            scopes: vec![],
            expires_unix_ms: 1,
        };
        store.save(&value).unwrap();
        assert_eq!(store.load().unwrap().unwrap().client_id, "c");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(store.dir.join("auth.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }
}

#[cfg(test)]
mod refresh_tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    fn expired() -> OAuthCredentials {
        OAuthCredentials {
            client_id: "client".into(),
            host_id: "host".into(),
            account_id: "account".into(),
            access_token: "old".into(),
            refresh_token: "refresh".into(),
            id_token: "id".into(),
            scopes: vec!["chatgpt.tokens.use.direct".into()],
            expires_unix_ms: 1,
        }
    }
    async fn server(body: &'static str, status: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut input = [0; 4096];
            let _ = socket.read(&mut input).await;
            let reply = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(reply.as_bytes()).await.unwrap();
        });
        format!("http://{addr}/token")
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
        assert_eq!(b.unwrap().refresh_token, "rotated");
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
