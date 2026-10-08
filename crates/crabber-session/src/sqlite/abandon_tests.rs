use super::records::{decode_row, load_run, save_run};
use super::*;
use crate::abandonment_contract::FixtureStore;
use async_trait::async_trait;
use crabber_core::{ManualClock, Run, RunId, SessionId, ToolCallRecord};
use time::OffsetDateTime;

#[async_trait]
impl FixtureStore for SqliteStore {
    async fn seed_run(&self, run: Run) {
        let mut tx = self.begin_write().await.unwrap();
        load_run(&mut tx, &run.id).await.unwrap();
        save_run(&mut tx, &run).await.unwrap();
        tx.commit().await.unwrap();
    }
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord> {
        sqlx::query("SELECT data FROM tool_calls WHERE run_id=$1 ORDER BY id")
            .bind(&run.0)
            .fetch_all(&self.readers)
            .await
            .unwrap()
            .into_iter()
            .map(|row| decode_row(&row, "data").unwrap())
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbox WHERE session_id=$1 AND consumed_by_run IS NULL",
        )
        .bind(&session.0)
        .fetch_one(&self.readers)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    }
}

#[tokio::test]
async fn shared_abandonment_contract() {
    let (store, _, _directory) = super::tests::temp_store().await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    crate::abandonment_contract::run_contract(store, clock).await;
}

#[tokio::test]
async fn untrusted_terminal_markers_are_never_replay_authority() {
    let (store, _, _directory) = super::tests::temp_store().await;
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = store.with_clock(clock.clone());
    crate::abandonment_contract::untrusted_terminal_contract(&store, &clock).await;
}
