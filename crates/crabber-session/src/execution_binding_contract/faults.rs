use super::fixture;
use crate::{BoundAdmitRequest, KeyedAdmitOutcome, Store, StoreError};
use crabber_core::{SessionId, SessionRevision};

pub(crate) async fn run(store: &impl Store, arm: impl Fn(u8)) {
    let now = time::OffsetDateTime::now_utc();
    for keyed in [false, true] {
        for stage in [1, 2] {
            let session = SessionId::new();
            let original = fixture(&session, now, "fault-backend", 'b');
            arm(stage);
            let error = if keyed {
                store
                    .admit_bound_keyed_run(original.clone())
                    .await
                    .unwrap_err()
            } else {
                let mut request = original.keyed.request.clone();
                request.session_id = None;
                store
                    .admit_bound_run(BoundAdmitRequest {
                        request,
                        binding: original.binding.clone(),
                    })
                    .await
                    .unwrap_err()
            };
            assert!(matches!(error, StoreError::Validation(_)));
            let receipt = store
                .lookup_admission(&session, &original.keyed.options.key)
                .await
                .unwrap();
            let runs = store
                .list_unfinished_runs()
                .await
                .unwrap()
                .into_iter()
                .filter(|r| r.session_id == session)
                .collect::<Vec<_>>();
            if stage == 1 {
                assert_eq!(runs.len(), 0);
                assert_eq!(receipt, None);
                assert_eq!(store.get_session(&session).await.unwrap(), None);
                assert_eq!(
                    store.session_revision(&session).await.unwrap_err(),
                    StoreError::NotFound
                );
                let KeyedAdmitOutcome::Started { receipt, .. } =
                    store.admit_bound_keyed_run(original.clone()).await.unwrap()
                else {
                    panic!("rolled back admission must be absent")
                };
                assert_eq!(
                    store
                        .execution_binding(&session, &receipt.run_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .capsule,
                    original.binding
                );
            } else {
                assert_eq!(runs.len(), 1);
                let retained = store
                    .execution_binding(&session, &runs[0].id)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(retained.capsule, original.binding);
                assert_eq!(
                    store.session_revision(&session).await.unwrap(),
                    SessionRevision::new(1).unwrap()
                );
                assert_eq!(store.list_all_messages(&session).await.unwrap().len(), 1);
                if keyed {
                    let original_receipt = receipt.unwrap();
                    assert!(
                        matches!(store.admit_bound_keyed_run(original).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == original_receipt)
                    );
                } else {
                    assert_eq!(receipt, None);
                    let mut request = original.keyed.request;
                    request.session_id = None;
                    // Unknown unkeyed admission is reconciled, never blindly
                    // treated as rollback or upgraded into keyed authority.
                    assert_eq!(
                        store
                            .admit_bound_run(BoundAdmitRequest {
                                request,
                                binding: original.binding
                            })
                            .await
                            .unwrap_err(),
                        StoreError::Busy
                    );
                }
                assert_eq!(
                    store.session_revision(&session).await.unwrap(),
                    SessionRevision::new(1).unwrap()
                );
            }
        }
    }
}
