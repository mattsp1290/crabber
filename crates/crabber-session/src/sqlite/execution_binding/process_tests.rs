use crate::execution_binding_contract::process;
const HELPER: &str = "sqlite::execution_binding::process_tests::process_helper";

#[tokio::test]
async fn process_helper() {
    let Some(target) = process::settings() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = if let Ok(marker) = std::env::var("CRABBER_BINDING_BARRIER") {
        crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await
    } else {
        crate::SqliteStore::connect(&path).await.unwrap()
    };
    if std::env::var_os("CRABBER_BINDING_WRITE").is_some() {
        process::write(&store, &target, false).await;
    } else {
        process::verify(&store, &target).await;
    }
}
#[tokio::test]
async fn bindings_survive_lost_ack_in_fresh_process() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    for (keyed, typed) in [(false, false), (true, false), (false, true), (true, true)] {
        let target = process::Fixture::new(keyed, typed);
        store
            .binding_fault
            .store(2, std::sync::atomic::Ordering::SeqCst);
        process::write(&store, &target, true).await;
        process::spawn(
            HELPER,
            &target,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            false,
            None,
        )
        .wait()
        .await;
    }
}
#[tokio::test]
async fn killed_binding_insert_rolls_back_entire_admission() {
    let _guard = crate::sqlite::test_support::BARRIER_LOCK.lock().await;
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    for (index, (keyed, typed)) in [(false, false), (true, false), (false, true), (true, true)]
        .into_iter()
        .enumerate()
    {
        let target = process::Fixture::new(keyed, typed);
        let marker = directory.path().join(format!("binding-{index}.marker"));
        sqlx::query("CREATE TRIGGER binding_barrier AFTER INSERT ON run_execution_bindings BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END").execute(&store.writer).await.unwrap();
        let mut child = process::spawn(
            HELPER,
            &target,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            true,
            Some(&marker),
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !marker.exists() {
                assert!(child.running(), "writer exited before binding insert");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child must reach binding insert before COMMIT");
        process::absent(&store, &target).await;
        child.kill();
        sqlx::query("DROP TRIGGER binding_barrier")
            .execute(&store.writer)
            .await
            .unwrap();
        process::absent(&store, &target).await;
        process::write(&store, &target, false).await;
        process::verify(&store, &target).await;
    }
}
