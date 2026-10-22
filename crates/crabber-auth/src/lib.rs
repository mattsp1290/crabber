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
    sync::Mutex as StdMutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
mod http_body;
mod token_manager;
pub use token_manager::TokenManager;

pub mod access_file;
pub mod codex;
pub use access_file::AccessFileCredentialStore;

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("device authorization denied")]
    DeviceDenied,
    #[error("device authorization expired")]
    DeviceExpired,
    #[error("sign-in cancelled")]
    Cancelled,
    #[error("credential is access-only; refresh is owned by the broker")]
    AccessOnly,
    #[error("credential profile does not match the configured provider")]
    ProfileMismatch,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialProfile {
    #[default]
    Siwc,
    CodexCli,
}
impl CredentialProfile {
    pub fn client_id(self) -> &'static str {
        match self {
            Self::Siwc => "dynamic_agent_client",
            Self::CodexCli => codex::CODEX_CLI_CLIENT_ID,
        }
    }
    pub fn token_url(self) -> &'static str {
        match self {
            Self::Siwc => codex::TOKEN_URL,
            Self::CodexCli => codex::CODEX_CLI_TOKEN_URL,
        }
    }
    pub fn inference_base_url(self) -> &'static str {
        match self {
            Self::Siwc => codex::RESOURCE,
            Self::CodexCli => codex::CODEX_BACKEND_URL,
        }
    }
    pub fn refresh_form(self, current: &OAuthCredentials) -> Vec<(&'static str, String)> {
        let mut form = vec![
            ("grant_type", "refresh_token".into()),
            ("client_id", current.client_id.clone()),
            (
                "refresh_token",
                current.refresh_token.clone().unwrap_or_default(),
            ),
        ];
        form.push(match self {
            Self::Siwc => ("resource", codex::RESOURCE.into()),
            Self::CodexCli => ("scope", codex::CODEX_CLI_SCOPE.into()),
        });
        form
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OAuthCredentials {
    #[serde(default)]
    pub profile: CredentialProfile,
    pub client_id: String,
    pub host_id: String,
    pub account_id: String,
    pub access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    pub id_token: String,
    pub scopes: Vec<String>,
    pub expires_unix_ms: u64,
}
impl OAuthCredentials {
    pub fn is_access_only(&self) -> bool {
        self.refresh_token.is_none()
    }
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
    /// An advisory lock shared by processes refreshing this store, if needed.
    fn refresh_lock(&self) -> Option<RefreshLock> {
        None
    }
    /// Atomically replace or clear only the credential version used for the refresh grant.
    ///
    /// The comparison and mutation must share the same lock or storage transaction
    /// as `save` and `clear`. Return `false` when another writer has rotated the
    /// credential. Stores that do not support refresh writes must return an error.
    /// Separate calls to `load` and `save`/`clear` cannot implement this contract.
    fn compare_exchange(
        &self,
        expected: &OAuthCredentials,
        next: Option<&OAuthCredentials>,
    ) -> Result<bool, AuthError>;
}

#[derive(Clone)]
pub struct RefreshLock {
    path: PathBuf,
    timeout: Duration,
}
async fn acquire_refresh_lock(
    config: Option<RefreshLock>,
) -> Result<Option<std::fs::File>, AuthError> {
    let Some(config) = config else {
        return Ok(None);
    };
    fs::create_dir_all(config.path.parent().ok_or(AuthError::Store)?)
        .map_err(|_| AuthError::Store)?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(config.path).map_err(|_| AuthError::Store)?;
    let start = std::time::Instant::now();
    loop {
        if file.try_lock_exclusive().map_err(|_| AuthError::Store)? {
            return Ok(Some(file));
        }
        if start.elapsed() >= config.timeout {
            return Err(AuthError::Store);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
fn same_rotation(left: &OAuthCredentials, right: &OAuthCredentials) -> bool {
    left.profile == right.profile
        && left.client_id == right.client_id
        && left.account_id == right.account_id
        && left.access_token == right.access_token
        && left.refresh_token == right.refresh_token
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
    fn read_path(path: &PathBuf) -> Result<Option<OAuthCredentials>, AuthError> {
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
    }
    fn save_path(path: &PathBuf, value: &OAuthCredentials) -> Result<(), AuthError> {
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
    }
    fn clear_path(path: &PathBuf) -> Result<(), AuthError> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(AuthError::Store),
        }
    }
}
impl CredentialStore for FileCredentialStore {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
        self.locked(Self::read_path)
    }
    fn save(&self, value: &OAuthCredentials) -> Result<(), AuthError> {
        self.locked(|path| Self::save_path(path, value))
    }
    fn clear(&self) -> Result<(), AuthError> {
        self.locked(Self::clear_path)
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
    fn refresh_lock(&self) -> Option<RefreshLock> {
        Some(RefreshLock {
            path: self.dir.join("auth.refresh.lock"),
            timeout: self.lock_timeout,
        })
    }
    fn compare_exchange(
        &self,
        expected: &OAuthCredentials,
        next: Option<&OAuthCredentials>,
    ) -> Result<bool, AuthError> {
        self.locked(|path| {
            let Some(current) = Self::read_path(path)? else {
                return Ok(false);
            };
            if !same_rotation(&current, expected) {
                return Ok(false);
            }
            match next {
                Some(value) => Self::save_path(path, value)?,
                None => Self::clear_path(path)?,
            }
            Ok(true)
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
    fn compare_exchange(
        &self,
        expected: &OAuthCredentials,
        next: Option<&OAuthCredentials>,
    ) -> Result<bool, AuthError> {
        let mut current = self.0.lock().map_err(|_| AuthError::Store)?;
        if !current
            .as_ref()
            .is_some_and(|value| same_rotation(value, expected))
        {
            return Ok(false);
        }
        *current = next.cloned();
        Ok(true)
    }
}

#[cfg(test)]
mod memory_tests;

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
            profile: CredentialProfile::Siwc,
            client_id: "c".into(),
            host_id: "h".into(),
            account_id: "a".into(),
            access_token: "secret".into(),
            refresh_token: Some("secret".into()),
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
