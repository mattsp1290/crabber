use super::*;
use crabber::{
    core::{ControlId, PublicStateContents, SessionRevision},
    extension::{
        PublicSourceError as Error, PublicSourceSink, PublicSourceSnapshot, PublicSourceWriter,
    },
};
use std::{collections::VecDeque, sync::Mutex};
struct Sink {
    outcomes: Mutex<VecDeque<Result<SessionRevision, Error>>>,
    calls: Mutex<Vec<(ControlId, SessionRevision, PublicSourceChange)>>,
    snapshots: AtomicUsize,
}
impl Sink {
    fn new(outcomes: Vec<Result<SessionRevision, Error>>) -> Arc<Self> {
        Arc::new(Self {
            outcomes: Mutex::new(outcomes.into()),
            calls: Mutex::new(vec![]),
            snapshots: AtomicUsize::new(0),
        })
    }
}
#[async_trait::async_trait]
impl PublicSourceSink for Sink {
    async fn snapshot(&self) -> Result<PublicSourceSnapshot, Error> {
        let seen = self.snapshots.fetch_add(1, Ordering::SeqCst);
        let mut revision = SessionRevision::INITIAL;
        for _ in 0..seen {
            revision = revision.next().unwrap();
        }
        Ok(PublicSourceSnapshot {
            revision,
            contents: PublicStateContents::default(),
        })
    }
    async fn publish(
        &self,
        id: ControlId,
        revision: SessionRevision,
        change: PublicSourceChange,
    ) -> Result<SessionRevision, Error> {
        self.calls.lock().unwrap().push((id, revision, change));
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(revision))
    }
}
#[tokio::test]
async fn publication_retries_conflicts_and_unknown_identically_without_changing_business_result() {
    for fault in [Error::RevisionConflict, Error::UnknownOutcome] {
        let sink = Sink::new(vec![Err(fault), Ok(SessionRevision::INITIAL)]);
        let ext = Arc::new(extension(Response::Completed("ok".into())));
        let (_, handle, tools) = mounted(ext.clone()).await;
        let context = context(CancellationToken::new())
            .with_public_source(PublicSourceWriter::from_host(sink.clone()));
        let result = tools[TOOL_NAME]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap();
        assert_eq!(result["status"], "completed");
        assert_eq!(ext.owner_report().publication_failures, 0);
        {
            let calls = sink.calls.lock().unwrap();
            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0].0, calls[1].0);
            assert_eq!(calls[0].2, calls[1].2);
            if fault == Error::UnknownOutcome {
                assert_eq!(calls[0].1, calls[1].1);
            } else {
                assert_ne!(calls[0].1, calls[1].1);
            }
            assert_eq!(
                sink.snapshots.load(Ordering::SeqCst),
                if fault == Error::RevisionConflict {
                    3
                } else {
                    2
                }
            );
        }
        handle.close().await.unwrap();
    }
}
#[tokio::test]
async fn unavailable_or_failed_publication_is_nonfatal_and_diagnostics_are_truthful() {
    for error in [
        Error::Unsupported,
        Error::StaleOwner,
        Error::Rejected,
        Error::RevisionConflict,
        Error::UnknownOutcome,
    ] {
        let sink = Sink::new(vec![Err(error); 8]);
        let ext = Arc::new(extension(Response::Failed("business failure".into())));
        let (_, handle, tools) = mounted(ext.clone()).await;
        let context = context(CancellationToken::new())
            .with_public_source(PublicSourceWriter::from_host(sink.clone()));
        assert_eq!(
            tools[TOOL_NAME]
                .execute_json_with_context(context, arguments())
                .await
                .unwrap()["status"],
            "failed"
        );
        assert_eq!(
            ext.owner_report().publication_failures,
            if error == Error::Unsupported { 0 } else { 2 }
        );
        assert!(sink.calls.lock().unwrap().len() <= 4);
        handle.close().await.unwrap();
    }
}

#[tokio::test]
async fn unknown_then_conflict_never_changes_the_original_compare_and_swap() {
    let sink = Sink::new(vec![
        Err(Error::UnknownOutcome),
        Err(Error::RevisionConflict),
    ]);
    let ext = Arc::new(extension(Response::Completed("ok".into())));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let context = context(CancellationToken::new())
        .with_public_source(PublicSourceWriter::from_host(sink.clone()));
    assert_eq!(
        tools[TOOL_NAME]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap()["status"],
        "completed"
    );
    {
        let calls = sink.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], calls[1]);
        assert_eq!(sink.snapshots.load(Ordering::SeqCst), 2);
        assert_eq!(ext.owner_report().publication_failures, 1);
    }
    handle.close().await.unwrap();
}
