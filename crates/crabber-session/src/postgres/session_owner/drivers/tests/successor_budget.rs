use crate::public_session::v2::proof::Proof;
use crate::{PostgresStore, Store};
pub(super) async fn run(store: &PostgresStore, clock: &crabber_core::ManualClock) {
    for admission in [true, false] {
        let request =
            crate::successor_driver_contract::fixture::request(store, clock, admission).await;
        let before = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        let mut tx = store
            .session_transaction(request.context.session_id())
            .await
            .unwrap();
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
