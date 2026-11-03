use super::*;
use crate::session_owner::owned_scope::{FixedInitialTestHooks, NativeInitialOperation};
use std::sync::Arc;

pub(crate) async fn assert_fixed_policy_sql_refusal(store: Arc<dyn Store>) {
    for operation in [NativeInitialOperation::Open, NativeInitialOperation::Seal] {
        let host = FixedMemoryInitialHost::new();
        let (context, revision) = managed(&host).await;
        let session = context.session_id().clone();
        let hooks = FixedInitialTestHooks::new();
        hooks.forward_native_once(operation, store.clone());
        let mut task = host
            .register_scripted_test(values(context, revision, false), source(), hooks.clone())
            .unwrap();
        task.start().unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(30), task.join())
            .await
            .unwrap()
            .unwrap();
        let disposition = observed.disposition();
        let failed = observed.observation_failed();
        let initial = observed.status().cloned();
        let mut retry = observed.take_retry();
        let retry_count = usize::from(retry.is_some());
        let owner = hooks.owner();
        let terminal = if let Some(retry) = &mut retry {
            tokio::time::timeout(Duration::from_secs(5), retry.retry())
                .await
                .unwrap()
                .unwrap()
        } else {
            initial
                .clone()
                .expect("actual original reached native closure")
        };
        drop((retry, task));
        tokio::time::timeout(Duration::from_secs(5), host.wait_for_supervision())
            .await
            .unwrap();
        let supervision = host
            .control
            .tasks()
            .observe_supervision(Duration::from_secs(5))
            .await;
        assert_eq!(supervision.pending_supervisors(), 0);
        assert_eq!(supervision.failed_supervisors(), 0);
        assert!(supervision.is_complete());
        assert_eq!(host.control.tasks().available_capacity(), 64);
        assert_eq!(disposition, OwnedMainDisposition::Returned);
        assert!(!failed);
        hooks.assert_native_refusal(operation);
        if operation == NativeInitialOperation::Seal {
            assert_eq!(initial, Some(PendingInitialProducerStatus::Unresolved));
            assert_eq!(retry_count, 1);
        }
        assert_eq!(
            terminal,
            match operation {
                NativeInitialOperation::Open => PendingInitialProducerStatus::ClosedNoPublication,
                NativeInitialOperation::Seal => PendingInitialProducerStatus::ClosedAssignment,
                NativeInitialOperation::Complete => unreachable!("only policy-bearing boundaries"),
            }
        );
        assert_eq!(
            owner.test_joined_disposition(),
            Some(OwnedMainDisposition::Returned)
        );
        assert!(!owner.resources_pending());
        lifetime::native_closed(&host, &session, &hooks);
        drop((owner, hooks, host));
    }
}
