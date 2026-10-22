//! Synthetic host isolates permit lifetime at publication; actual runtime joins
//! are qualified by detached/attribution tests rather than inferred here.
use super::*;
use crabber::{
    core::{ControlId, PublicSourceChange, PublicStateContents, SessionRevision},
    extension::{PublicSourceError, PublicSourceSink, PublicSourceSnapshot, PublicSourceWriter},
};
use tokio::sync::Semaphore;

struct PublishingHost {
    publications_to_hold: AtomicUsize,
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait::async_trait]
impl PublicSourceSink for PublishingHost {
    async fn snapshot(&self) -> Result<PublicSourceSnapshot, PublicSourceError> {
        Ok(PublicSourceSnapshot {
            revision: SessionRevision::INITIAL,
            contents: PublicStateContents::default(),
        })
    }
    async fn publish(
        &self,
        _: ControlId,
        revision: SessionRevision,
        change: PublicSourceChange,
    ) -> Result<SessionRevision, PublicSourceError> {
        if matches!(change, PublicSourceChange::ActivityDelta { .. })
            && self
                .publications_to_hold
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(revision)
    }
}
struct JoinedPublisher(Arc<PublishingHost>);
#[async_trait::async_trait]
impl SessionInvocationSpawner for JoinedPublisher {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        let mut context = invocation();
        context.public_source = PublicSourceWriter::from_host(self.0.clone());
        task(context).await.unwrap();
        Ok(Admission::Started)
    }
}

#[tokio::test]
async fn final_publication_holds_capacity_and_completion_or_drop_releases_it() {
    for abort in [false, true] {
        let ext = Arc::new(configured(
            runner(Response::Completed("retained".into())),
            Limits {
                max_in_flight: 1,
                ..limits()
            },
        ));
        let (_, handle, tools) = mounted(ext.clone()).await;
        let publication = Arc::new(PublishingHost {
            publications_to_hold: AtomicUsize::new(1),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let context = context(CancellationToken::new()).with_session_invocations(
            SessionInvocations::from_host(Arc::new(JoinedPublisher(publication.clone()))),
        );
        let start = tools[START_TOOL].clone();
        let original = context.clone();
        let call =
            tokio::spawn(
                async move { start.execute_json_with_context(original, arguments()).await },
            );
        tokio::time::timeout(Duration::from_secs(3), publication.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert_eq!(ext.live_runners(), 1, "publication still owns capacity");
        assert_eq!(
            tools[START_TOOL]
                .execute_json_with_context(next(context.clone()), arguments())
                .await
                .unwrap()["status"],
            "unavailable"
        );
        if abort {
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
        } else {
            publication.release.add_permits(1);
            assert_eq!(call.await.unwrap().unwrap()["status"], "started");
        }
        assert_eq!(ext.live_runners(), 0);
        assert_eq!(
            tools[START_TOOL]
                .execute_json_with_context(next(context), arguments())
                .await
                .unwrap()["status"],
            "started",
            "capacity was released after the lifecycle ended"
        );
        handle.close().await.unwrap();
    }
}

struct AcknowledgedPublisher {
    publication: Arc<PublishingHost>,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for AcknowledgedPublisher {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        let mut context = invocation();
        context.public_source = PublicSourceWriter::from_host(self.publication.clone());
        self.workers.lock().unwrap().push(tokio::spawn(async move {
            task(context).await.unwrap();
        }));
        // Unlike JoinedPublisher, ACK precedes completion and permits pruning.
        Ok(Admission::Started)
    }
}

#[tokio::test]
async fn retention_bound_covers_evicted_outputs_held_by_acknowledged_publications() {
    let max_in_flight = 4;
    let max_result_bytes = 1024;
    let calls = Arc::new(AtomicUsize::new(0));
    let runner: Runner = Arc::new(move |_| {
        let index = calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(Response::Completed(format!(
                "{index:04}{}",
                "x".repeat(max_result_bytes - 4)
            )))
        })
    });
    let limits = Limits {
        max_tracked: 1,
        max_in_flight,
        max_result_bytes,
        ..limits()
    };
    let bound = limits.worst_case_retained_bytes();
    let ext = Arc::new(configured(runner, limits));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let publication = Arc::new(PublishingHost {
        publications_to_hold: AtomicUsize::new(max_in_flight),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let host = Arc::new(AcknowledgedPublisher {
        publication: publication.clone(),
        workers: Mutex::new(vec![]),
    });
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(host.clone()));
    let mut previous = None;
    for index in 0..max_in_flight {
        let started = tools[START_TOOL]
            .execute_json_with_context(next(context.clone()), arguments())
            .await
            .unwrap();
        assert_eq!(
            started["status"], "started",
            "positive ACK before next start"
        );
        tokio::time::timeout(Duration::from_secs(3), publication.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let status = tools[STATUS_TOOL]
            .execute_json_with_context(context.clone(), json!({"task_id":started["task_id"]}))
            .await
            .unwrap();
        assert_eq!(status["output"].as_str().unwrap().len(), max_result_bytes);
        assert!(
            status["output"]
                .as_str()
                .unwrap()
                .starts_with(&format!("{index:04}"))
        );
        if let Some(evicted) = previous {
            assert!(
                tools[STATUS_TOOL]
                    .execute_json_with_context(context.clone(), json!({"task_id":evicted}))
                    .await
                    .is_err(),
                "the previous terminal row was actually evicted"
            );
        }
        previous = Some(started["task_id"].clone());
        assert_eq!(ext.owner_report().tracked, 1);
        assert_eq!(ext.live_runners(), index + 1);
        assert!(
            bound >= (index + 1) * max_result_bytes,
            "live lifecycle outputs exceed bound"
        );
    }
    assert_eq!(
        tools[START_TOOL]
            .execute_json_with_context(next(context), arguments())
            .await
            .unwrap()["status"],
        "unavailable"
    );
    publication.release.add_permits(max_in_flight);
    let workers = std::mem::take(&mut *host.workers.lock().unwrap());
    for worker in workers {
        tokio::time::timeout(Duration::from_secs(3), worker)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(ext.live_runners(), 0);
    handle.close().await.unwrap();
}
