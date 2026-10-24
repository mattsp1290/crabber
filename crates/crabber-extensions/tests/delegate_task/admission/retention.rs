use super::*;
struct Refuse(Error);
#[async_trait::async_trait]
impl SessionInvocationSpawner for Refuse {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        _: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        Err(self.0)
    }
}
#[tokio::test]
async fn every_refused_registration_restores_the_full_registrys_original_result() {
    for error in [
        Error::Unsupported,
        Error::Capacity,
        Error::Rejected,
        Error::UnknownOutcome,
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let host: Runner = {
            let calls = calls.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Response::Completed("retained".into())) })
            })
        };
        let ext = Arc::new(configured(
            host,
            Limits {
                max_tracked: 1,
                ..limits()
            },
        ));
        let (_, handle, tools) = mounted(ext.clone()).await;
        let context = context(CancellationToken::new()).with_session_invocations(
            SessionInvocations::from_host(Arc::new(
                crate::invocation_support::JoinedSpawner::default(),
            )),
        );
        let first = tools[START_TOOL]
            .execute_json_with_context(context.clone(), arguments())
            .await
            .unwrap();
        let before = tools[LIST_TOOL]
            .execute_json_with_context(context.clone(), json!({}))
            .await
            .unwrap();
        let refused = next(context.clone())
            .with_session_invocations(SessionInvocations::from_host(Arc::new(Refuse(error))));
        let result = tools[START_TOOL]
            .execute_json_with_context(refused, arguments())
            .await;
        if matches!(error, Error::Capacity | Error::Rejected) {
            assert_eq!(result.unwrap()["status"], "unavailable");
        } else {
            assert!(result.is_err());
        }
        assert_eq!(
            tools[LIST_TOOL]
                .execute_json_with_context(context.clone(), json!({}))
                .await
                .unwrap(),
            before
        );
        let status = tools[STATUS_TOOL]
            .execute_json_with_context(context, json!({"task_id":first["task_id"]}))
            .await
            .unwrap();
        assert_eq!(status["output"], "retained");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(ext.owner_report().live, 0);
        assert_eq!(ext.owner_report().tracked, 1);
        assert_eq!(
            ext.owner_report().unresolved_invocations,
            usize::from(error == Error::UnknownOutcome)
        );
        handle.close().await.unwrap();
    }
}
