use super::*;
struct DisposalPanic;
impl Future for DisposalPanic {
    type Output = Result<Response, ExtensionError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(Response::Completed("discarded".into())))
    }
}
impl Drop for DisposalPanic {
    fn drop(&mut self) {
        panic!("synthetic disposal failure");
    }
}
#[tokio::test]
async fn disposal_failure_is_uncertain_cleanup_instead_of_a_successful_join() {
    let host: Runner = Arc::new(|_| Box::pin(DisposalPanic));
    let mut j = Journey::new(host, limits());
    let id = j.start().await;
    assert_eq!(j.wait(&[id]).await["tasks"][0]["status"], "error");
    let reports = j
        .agent
        .drain_session_invocations(j.session.as_ref().unwrap(), false, Duration::from_secs(2))
        .await;
    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0].status,
        crabber::SessionInvocationStatus::Uncertain
    );
    let (owner, _) = owner(&j).await;
    assert_eq!(
        j.store
            .get_session_owner(j.session.as_ref().unwrap(), &owner)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Uncertain
    );
    assert_eq!(j.ext.live_runners(), 0);
    let events = j
        .store
        .list_events(j.session.as_ref().unwrap(), None, 1000)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InvocationError)
            .count(),
        1
    );
    assert!(
        !events
            .iter()
            .any(|e| e.kind == EventKind::InvocationFinished)
    );
    let close = j.agent.close(Duration::from_secs(2)).await;
    assert!(
        !close.local_drained(),
        "a recorded cleanup failure remains visible"
    );
}
