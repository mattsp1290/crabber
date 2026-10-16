use crate::session_owner_contract;
#[tokio::test]
async fn session_owner_contract() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    session_owner_contract::run_contract(|clock| store.clone().with_clock(clock)).await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = store.with_clock(clock.clone());
    session_owner_contract::faults::run(&store, &clock, |value| {
        store
            .session_owner_fault
            .store(value, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    session_owner_contract::parent::run(&store, &clock).await;
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    super::corruption::run(&store).await;
    session_owner_contract::process::run(
        &store,
        "sqlite::session_owner::tests::session_owner_process_helper",
        Some(&path),
    )
    .await;
}

#[tokio::test]
async fn session_owner_process_helper() {
    let Some(command) = session_owner_contract::process::command() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = crate::SqliteStore::connect(path).await.unwrap();
    let store = store.with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10),
    )));
    session_owner_contract::process::execute(&store, command).await;
}
