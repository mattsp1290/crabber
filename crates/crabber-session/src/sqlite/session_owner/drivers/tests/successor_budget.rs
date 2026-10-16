use crate::public_session::v2::proof::Proof;
use crate::{SqliteStore, Store};
pub(super) async fn run(store: &SqliteStore, clock: &crabber_core::ManualClock) {
    for family in 0..4 {
        let request = if family < 2 {
            crate::successor_driver_contract::fixture::request(store, clock, family == 0).await
        } else {
            crate::successor_driver_contract::approval::fixture(store, clock, family == 3)
                .await
                .request
        };
        let before = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        let mut tx = store.begin_write().await.unwrap();
        let mut proof = Proof::bounded(32 << 20);
        Box::pin(super::super::successor::fresh::fresh(
            &mut tx,
            &request,
            None,
            store.clock.now(),
            &mut proof,
        ))
        .await
        .unwrap();
        let used = proof.used();
        assert!(used > 1000);
        assert!(
            Box::pin(super::super::successor::fresh::fresh(
                &mut tx,
                &request,
                None,
                store.clock.now(),
                &mut Proof::bounded(used - 1)
            ))
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            before
        );
    }
}
