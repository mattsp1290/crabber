mod result_transform_support;

use crabber::session::MemoryStore;
use result_transform_support::{Probes, agent_builder};
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reduction_child_starts_and_reaps() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let probes = Probes::new(true);
        let agent = agent_builder(Arc::new(MemoryStore::new()), probes.clone(), false)
            .build()
            .unwrap();
        let run = agent.prompt(None, "reduce fixture output").await.unwrap();
        assert_ne!(probes.ready().await, 0);
        assert_eq!(probes.permit_count(), 0);
        assert!(!probes.reaped.observed());
        probes.release_callback();
        run.done().await.unwrap();
        let (closed, ()) = tokio::join!(agent.close_extensions(), async {
            probes.kill_started.wait().await;
            probes.release_reap();
        });
        closed.unwrap();
        probes.reaped.wait().await;
        assert!(probes.kill_started.observed());
        assert!(probes.pipe_closed.observed());
        assert_eq!(probes.permit_count(), 1);
    })
    .await
    .expect("fixture smoke timeout");
}
