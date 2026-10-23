//! Keep worker I/O live while its heartbeat clock is frozen during integrity checks.
use super::provider::Gate;
use crabber::{
    Agent, AgentConfig, ExecutionContext,
    core::{RunId, RunStatus},
};
use std::{
    sync::{Arc, mpsc},
    thread,
};
use tokio::sync::oneshot;

pub(super) struct HeldRun {
    pub run_id: RunId,
    resume: Option<mpsc::Sender<()>>,
    done: Option<oneshot::Receiver<RunStatus>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl HeldRun {
    pub async fn start(host_url: String, context: ExecutionContext, gate: Gate) -> Self {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let worker = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .start_paused(true)
                .build()
                .unwrap();
            let status = runtime.block_on(async {
                // An outstanding blocking task inhibits Tokio's automatic time
                // advance while the paused runtime continues serving SQL I/O.
                let resume = tokio::task::spawn_blocking(move || {
                    let _ = resume_rx.recv();
                });
                // Connections and their I/O driver belong only to this runtime.
                // Controllers retain the fixture's independently connected pool.
                let native = crabber::session::PostgresStore::connect(&host_url)
                    .await
                    .unwrap();
                let mut config = AgentConfig::new(super::fixture::selection());
                config.workspace_id = context.session().workspace_id().into();
                config.directory = "/roles".into();
                let host = Agent::builder()
                    .behavior_version("workspace-roles.v1")
                    .store(Arc::new(native))
                    .provider(Arc::new(gate.clone()))
                    .config(config)
                    .build()
                    .unwrap();
                let handle = host.prompt_in(&context, "held", None).await.unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
                    .await
                    .unwrap();
                let _ = ready_tx.send(handle.run_id().clone());
                resume.await.unwrap();
                tokio::time::resume();
                gate.release.notify_one();
                let result = handle.done().await.unwrap();
                host.close_extensions().await.unwrap();
                result.status
            });
            let _ = done_tx.send(status);
        });
        Self {
            // This bound runs on the test's ordinary clock, independently of
            // the worker clock frozen above.
            run_id: tokio::time::timeout(std::time::Duration::from_secs(10), ready_rx)
                .await
                .unwrap()
                .unwrap(),
            resume: Some(resume_tx),
            done: Some(done_rx),
            worker: Some(worker),
        }
    }

    pub async fn finish(mut self) -> RunStatus {
        self.resume.take().unwrap().send(()).unwrap();
        let result = self.done.take().unwrap().await;
        let worker = self.worker.take().unwrap();
        tokio::task::spawn_blocking(move || worker.join().unwrap())
            .await
            .unwrap();
        result.unwrap()
    }
}

impl Drop for HeldRun {
    fn drop(&mut self) {
        // An assertion failure must still release the worker clock guard.
        if let Some(resume) = self.resume.take() {
            let _ = resume.send(());
        }
    }
}
