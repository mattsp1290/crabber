use super::*;
use std::sync::Barrier;

fn credential(access: &str) -> OAuthCredentials {
    OAuthCredentials {
        profile: CredentialProfile::CodexCli,
        client_id: "test-client".into(),
        host_id: "test-host".into(),
        account_id: "test-account".into(),
        access_token: access.into(),
        refresh_token: Some("test-refresh".into()),
        id_token: String::new(),
        scopes: vec![],
        expires_unix_ms: u64::MAX,
    }
}

#[test]
fn broker_rotation_survives_stale_refresh_write_and_rejection() {
    for clear in [false, true] {
        let store = MemoryCredentialStore::default();
        let original = credential("original");
        store.save(&original).unwrap();
        let rotated = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                store.save(&credential("broker-rotation")).unwrap();
                rotated.wait();
            });
            scope.spawn(|| {
                let refreshed = credential("stale-refresh");
                rotated.wait();
                assert!(
                    !store
                        .compare_exchange(&original, (!clear).then_some(&refreshed))
                        .unwrap()
                );
            });
        });
        assert_eq!(
            store.load().unwrap().unwrap().access_token,
            "broker-rotation"
        );
    }
}

#[test]
fn competing_refreshes_can_consume_each_credential_version_only_once() {
    for clear in [false, true] {
        // Exercise the real store on two OS threads with a shared starting gate.
        // One thread clears or refreshes; the other refreshes the same version.
        for _ in 0..64 {
            let store = MemoryCredentialStore::default();
            let original = credential("original");
            store.save(&original).unwrap();
            let start = Barrier::new(2);
            let (first, second) = std::thread::scope(|scope| {
                let first = scope.spawn(|| {
                    let next = credential("first");
                    start.wait();
                    store
                        .compare_exchange(&original, (!clear).then_some(&next))
                        .unwrap()
                });
                let second = scope.spawn(|| {
                    let next = credential("second");
                    start.wait();
                    store.compare_exchange(&original, Some(&next)).unwrap()
                });
                (first.join().unwrap(), second.join().unwrap())
            });
            assert_ne!(first, second, "exactly one CAS must succeed");
            let saved = store.load().unwrap();
            if second {
                assert_eq!(saved.unwrap().access_token, "second");
            } else if clear {
                assert!(saved.is_none());
            } else {
                assert_eq!(saved.unwrap().access_token, "first");
            }
        }
    }
}
