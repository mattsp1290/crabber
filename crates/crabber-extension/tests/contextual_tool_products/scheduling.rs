use super::*;
pub(super) struct Cleanup {
    pub(super) ready: BoxFuture<'static, ()>,
    pub(super) seen: Arc<Seen>,
}
impl Future for Cleanup {
    type Output = Result<(), ResourceFailure>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ready.as_mut().poll(cx).map(|()| Ok(()))
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        let graph = self.seen.cleanup_graph.lock().unwrap().take();
        if let Some(graph) = graph {
            graph.request_close().unwrap();
        }
        let facade = self.seen.cleanup_facade.lock().unwrap().take();
        drop(facade);
        if let Some(started) = self.seen.cleanup_started.lock().unwrap().take() {
            let _ = started.send(());
        }
        self.seen.drop_complete.block();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn factory_cleanup_final_reference_drop_can_unblock_original_box_drop() {
    cleanup_drop(1).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn factory_cleanup_without_invocation_can_unblock_original_box_drop() {
    cleanup_drop(0).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn factory_cleanup_after_reaped_invocations_can_unblock_original_box_drop() {
    cleanup_drop(80).await;
}
async fn cleanup_drop(invocations: usize) {
    let seen = Seen::new(Call::Ready);
    seen.cleanup_enabled.store(true, Ordering::SeqCst);
    let forced = Arc::new(AtomicBool::new(false));
    let (send, receive) = std::sync::mpsc::channel();
    *seen.cleanup_started.lock().unwrap() = Some(send);
    let rescue = seen.drop_complete.clone();
    let failed = forced.clone();
    let watchdog = std::thread::spawn(move || {
        let started = receive.recv_timeout(Duration::from_secs(3));
        let open = rescue.open.lock().unwrap();
        let (open, _) = rescue
            .changed
            .wait_timeout_while(open, Duration::from_millis(500), |open| !*open)
            .unwrap();
        let rescued = started.is_err() || !*open;
        drop(open);
        if rescued {
            failed.store(true, Ordering::SeqCst);
            rescue.release();
        }
    });
    let (provider, host) = setup(seen.clone(), FactoryScope::Session);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    for _ in 0..invocations {
        plan.tools[0].executor.execute(Value::Null).await.unwrap();
    }
    *seen.cleanup_facade.lock().unwrap() = Some(plan.tools[0].executor.clone());
    drop(plan);
    let graph = graph(&host);
    let node = graph.tool_product(0).unwrap();
    *seen.cleanup_graph.lock().unwrap() = Some(graph.clone());
    // The original registered cleanup owns the last facade. Its real disposal
    // requests actual generation close, releases that facade and waits for
    // the original Tool destructor body before its own disposal returns.
    seen.cleanup_release.add_permits(1);
    let disposed = node.disposed(Duration::from_secs(3)).await;
    let closed = provider.close_all(Duration::from_secs(3)).await;
    watchdog.join().unwrap();
    assert!(disposed.is_ok() && closed.is_ok());
    assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    assert!(
        !forced.load(Ordering::SeqCst),
        "actual cleanup required rescue before BoxDrop could start"
    );
}
