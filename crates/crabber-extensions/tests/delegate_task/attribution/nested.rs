use super::*;
use crate::background::Journey;
use crabber::{
    core::{ControlId, InputFingerprint, PublicActivity},
    extension::{
        PublicSourceError, PublicSourceWriter, SessionInvocationAdmission, SessionInvocationRequest,
    },
};
use std::sync::Mutex;

async fn nested_display(writer: &PublicSourceWriter) {
    let id = ControlId::from("host.nested.snapshot");
    let change = PublicSourceChange::ActivitySnapshot {
        activity: PublicActivity::new("host-nested", "host.nested", json!({"status":"running"}))
            .unwrap(),
    };
    for _ in 0..5 {
        let snapshot = writer.snapshot().await.unwrap();
        match writer
            .publish(id.clone(), snapshot.revision, change.clone())
            .await
        {
            Ok(_) => return,
            Err(PublicSourceError::RevisionConflict) => {}
            result => panic!("nested fixture publication: {result:?}"),
        }
    }
    panic!("nested fixture failed to obtain a public revision");
}
#[tokio::test]
async fn runner_handles_attribute_nested_jobs_to_the_actual_background_or_inline_parent() {
    for background in [true, false] {
        let ready = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(Mutex::new(None));
        let host: Runner = {
            let ready = ready.clone();
            let seen = seen.clone();
            Arc::new(move |request| {
                let ready = ready.clone();
                let seen = seen.clone();
                Box::pin(async move {
                    let nested = SessionInvocationRequest::new(
                        "host.nested".into(),
                        InputFingerprint::new("e".repeat(64)).unwrap(),
                    );
                    *seen.lock().unwrap() = Some(nested.clone());
                    assert_eq!(
                        request
                            .session_invocations()
                            .spawn(nested, move |inv| async move {
                                nested_display(&inv.public_source).await;
                                ready.notify_one();
                                inv.cancel.cancelled().await;
                                Ok(())
                            })
                            .await
                            .unwrap(),
                        SessionInvocationAdmission::Started
                    );
                    Ok(Response::Completed("nested registered".into()))
                })
            })
        };
        let mut j = Journey::new(host, limits());
        let call = j
            .call(
                if background {
                    crabber_extensions::delegate_task::START_TOOL
                } else {
                    TOOL_NAME
                },
                arguments(),
            )
            .await;
        tokio::time::timeout(Duration::from_secs(3), ready.notified())
            .await
            .unwrap();
        if background {
            j.wait(&[read_task_id(&call)]).await;
        }
        let events = j
            .store
            .list_events(j.session.as_ref().unwrap(), None, 1000)
            .await
            .unwrap();
        let publications = publications(&events);
        let nested = publications.iter().find(|p| matches!(&p.change, PublicSourceChange::ActivitySnapshot { activity } if activity.id() == "host-nested")).unwrap();
        let nested_id = seen.lock().unwrap().clone().unwrap().id;
        let invocation = nested.source.invocation.as_ref().unwrap();
        assert_eq!(invocation.id.0, nested_id.0);
        assert_eq!(invocation.parent_call_id.as_ref(), Some(&call.id));
        if background {
            let parent = publications.iter().find(|p| matches!(&p.change, PublicSourceChange::ActivitySnapshot { activity } if activity.activity_type() == "crabber-extensions.delegate-task")).unwrap();
            assert_eq!(
                invocation.parent_invocation.as_ref().unwrap().0,
                parent.source.invocation.as_ref().unwrap().id.0
            );
        } else {
            assert!(invocation.parent_invocation.is_none());
        }
        let reports = j
            .agent
            .drain_session_invocations(j.session.as_ref().unwrap(), true, Duration::from_secs(3))
            .await;
        assert!(reports.iter().all(|r| r.local_joined));
        j.close().await;
    }
}
