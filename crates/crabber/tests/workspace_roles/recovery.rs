//! Recovery must discover authorized expired runs, including through callbacks.
use super::fixture::*;
use crabber::{core::*, session::*, *};

enum Sweep {
    Global,
    Scoped,
    GlobalContext,
    ScopedContext,
}

#[tokio::test]
async fn worker_recovery_reports_own_expired_candidates() {
    for sweep in [
        Sweep::Global,
        Sweep::Scoped,
        Sweep::GlobalContext,
        Sweep::ScopedContext,
    ] {
        let Some(f) = Fixture::new().await else {
            return;
        };
        let admitted = legacy(&f.worker_native, &f.workspace_a).await;
        let mut seen = Vec::new();
        let mut context_for = |run: &Run| {
            seen.push(run.id.clone());
            None
        };
        let report = match sweep {
            Sweep::Global => f.worker.recover().await,
            Sweep::Scoped => f.worker.recover_workspace(&f.workspace_a).await,
            Sweep::GlobalContext => f.worker.recover_with_context(&mut context_for).await,
            Sweep::ScopedContext => {
                f.worker
                    .recover_workspace_with_context(&f.workspace_a, &mut context_for)
                    .await
            }
        }
        .unwrap();
        assert_legacy_skip(&report, &admitted.run.id);
        if matches!(sweep, Sweep::GlobalContext | Sweep::ScopedContext) {
            assert_eq!(seen, vec![admitted.run.id]);
        } else {
            assert_eq!(seen, Vec::<RunId>::new());
        }
        assert_eq!(f.worker_provider.requests().len(), 0);
        f.cleanup().await;
    }
}

pub(super) fn assert_legacy_skip(report: &RecoverReport, run: &RunId) {
    assert_eq!(report.recovered, vec![]);
    assert_eq!(report.skipped.len(), 1);
    assert_eq!(&report.skipped[0].run_id, run);
    // Direct legacy admission intentionally has no native execution binding.
    // This exact skip proves discovery without accepting Busy/Unknown results.
    assert_eq!(
        report.skipped[0].reason,
        RecoverySkipReason::Store(StoreError::HostUnsupported)
    );
}
