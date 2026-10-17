use super::*;

#[tokio::test]
async fn cancelled_actual_begin_write_keeps_next_writer_usable() {
    use sqlx::TransactionManager;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Wake, Waker},
    };
    struct Signal(tokio::sync::Notify);
    impl Wake for Signal {
        fn wake(self: Arc<Self>) {
            self.0.notify_one();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.notify_one();
        }
    }
    fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }
    let (store, path, _directory) = temp_store().await;
    // Cancel only the actual Store future at real worker wake boundaries. The
    // different depths include pool checkout, Begin ACK and handle-unlock waits.
    for depth in 1..=12 {
        for repetition in 0..8 {
            let signal = Arc::new(Signal(tokio::sync::Notify::new()));
            let waker = Waker::from(signal.clone());
            let mut original = Box::pin(store.begin_write());
            for poll in 0..depth {
                match poll_once(original.as_mut(), &waker) {
                    Poll::Ready(result) => {
                        result.unwrap().rollback().await.unwrap();
                        break;
                    }
                    Poll::Pending if poll + 1 < depth => {
                        tokio::time::timeout(Duration::from_secs(5), signal.0.notified())
                            .await
                            .unwrap();
                    }
                    Poll::Pending => {}
                }
            }
            drop(original);
            let mut observed = store.writer.acquire().await.unwrap();
            // A worker command flushes queued rollback before depth observation.
            sqlx::query("SELECT 1")
                .execute(&mut *observed)
                .await
                .unwrap();
            let actual_depth =
                <Sqlite as sqlx::Database>::TransactionManager::get_transaction_depth(&observed);
            let mut contender =
                sqlx::SqliteConnection::connect_with(&options(&path).busy_timeout(Duration::ZERO))
                    .await
                    .unwrap();
            let probe = sqlx::query("BEGIN IMMEDIATE").execute(&mut contender).await;
            let actual_write_lock = probe
                .as_ref()
                .err()
                .and_then(primary_code)
                .is_some_and(|code| code & 0xff == 5);
            if probe.is_ok() {
                sqlx::query("ROLLBACK")
                    .execute(&mut contender)
                    .await
                    .unwrap();
            }
            drop(contender);
            drop(observed);
            let next = tokio::time::timeout(Duration::from_secs(5), store.begin_write())
                .await
                .unwrap();
            let usable = if let Ok(tx) = next {
                tx.rollback().await.unwrap();
                true
            } else {
                // Complete teardown before asserting on the original failure.
                let mut connection = store.writer.acquire().await.unwrap();
                <Sqlite as sqlx::Database>::TransactionManager::rollback(&mut connection)
                    .await
                    .unwrap();
                false
            };
            assert!(
                usable,
                "actual original Begin cancellation after {depth} polls ({repetition}) left depth={actual_depth} write_lock={actual_write_lock}"
            );
        }
    }
}
