use super::{
    refresh_tests::{expired, server},
    *,
};
use crate::{MemoryCredentialStore, codex};

struct ProfileReplacement(MemoryCredentialStore);
impl CredentialStore for ProfileReplacement {
    fn load(&self) -> Result<Option<OAuthCredentials>, AuthError> {
        self.0.load()
    }
    fn save(&self, credentials: &OAuthCredentials) -> Result<(), AuthError> {
        self.0.save(credentials)
    }
    fn clear(&self) -> Result<(), AuthError> {
        self.0.clear()
    }
    fn compare_exchange(
        &self,
        expected: &OAuthCredentials,
        next: Option<&OAuthCredentials>,
    ) -> Result<bool, AuthError> {
        // A concurrent login changes only the profile. Every other identity and
        // token field deliberately matches the stale refresh's expected value.
        let mut replacement = expected.clone();
        replacement.profile = CredentialProfile::CodexCli;
        self.0.save(&replacement)?;
        self.0.compare_exchange(expected, next)
    }
}
#[tokio::test]
async fn competing_profile_is_preserved_after_refresh_success_or_rejection() {
    for (status, body) in [
        ("200 OK", r#"{"access_token":"discarded"}"#),
        ("400 Bad Request", r#"{"error":"invalid_grant"}"#),
    ] {
        let store = Arc::new(ProfileReplacement(MemoryCredentialStore::default()));
        let original = expired();
        store.save(&original).unwrap();
        let manager = TokenManager::new(store.clone())
            .with_profile(CredentialProfile::Siwc)
            .with_token_url(server(body, status).await);
        assert!(matches!(
            manager.credentials().await,
            Err(AuthError::ProfileMismatch)
        ));
        let saved = store.load().unwrap().unwrap();
        assert_eq!(saved.profile, CredentialProfile::CodexCli);
        assert_eq!(saved.access_token, original.access_token);
        assert_eq!(saved.refresh_token, original.refresh_token);
    }
}
#[tokio::test]
async fn expired_and_near_expiry_broker_rotations_are_not_returned() {
    for expiry in [1, crate::now_ms() + 30_000] {
        let store = Arc::new(MemoryCredentialStore::default());
        let mut credential = expired();
        credential.refresh_token = None;
        credential.access_token = "rotated".into();
        credential.expires_unix_ms = expiry;
        store.save(&credential).unwrap();
        let manager =
            TokenManager::new(store.clone()).with_token_url("http://127.0.0.1:1/unreachable");
        assert!(matches!(
            manager.force_refresh("old").await,
            Err(AuthError::AccessOnly)
        ));
        assert_eq!(store.load().unwrap().unwrap().access_token, "rotated");
    }
}
#[tokio::test]
async fn browser_rejects_cli_profile_before_listener_or_callback() {
    let store = Arc::new(MemoryCredentialStore::default());
    let mut credential = expired();
    credential.profile = CredentialProfile::CodexCli;
    store.save(&credential).unwrap();
    let result =
        codex::login_browser(store.clone(), |_| panic!("wrong-profile browser callback")).await;
    assert!(matches!(result, Err(AuthError::ProfileMismatch)));
    assert_eq!(
        store.load().unwrap().unwrap().profile,
        CredentialProfile::CodexCli
    );
}
#[tokio::test]
async fn overflowing_refresh_expiry_is_transport_failure_without_rotation() {
    let store = Arc::new(MemoryCredentialStore::default());
    store.save(&expired()).unwrap();
    let manager = TokenManager::new(store.clone()).with_token_url(
        server(
            r#"{"access_token":"discarded","expires_in":18446744073709551615}"#,
            "200 OK",
        )
        .await,
    );
    assert!(matches!(
        manager.credentials().await,
        Err(AuthError::Transport)
    ));
    assert_eq!(store.load().unwrap().unwrap().access_token, "old");
}
