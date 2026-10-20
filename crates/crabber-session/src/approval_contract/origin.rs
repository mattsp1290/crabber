//! Coherent private changes must disagree with the original committed origin.
use super::fixture::{answer, claim, staged};
use crate::{ApprovalError as Error, ApprovalReply, Store, approval::ApprovalRecord};
use crabber_core::{Clock, ManualClock, TurnId};
use std::future::Future;

pub(crate) async fn run<F, Fut>(store: &impl Store, clock: &ManualClock, mut save: F)
where
    F: FnMut(crabber_core::ContinuationId, usize) -> Fut,
    Fut: Future<Output = ()>,
{
    waiting(store, clock, &mut save).await;
    recovery(store, clock, &mut save).await;
}
async fn waiting<F, Fut>(store: &impl Store, clock: &ManualClock, save: &mut F)
where
    F: FnMut(crabber_core::ContinuationId, usize) -> Fut,
    Fut: Future<Output = ()>,
{
    for case in 0..6 {
        clock.set(time::OffsetDateTime::UNIX_EPOCH);
        let mut f = staged(store, clock).await;
        if case != 5 {
            f.batch.source_turn_id = Some(TurnId::new());
            f.batch.source_created_at = Some(clock.now() + time::Duration::nanoseconds(7));
        }
        let suspended = f
            .execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        save(f.batch.continuation.id.clone(), case).await;
        if case == 0 {
            clock.set(clock.now() + time::Duration::seconds(2));
        }
        let events = store
            .list_events(&f.run.session_id, None, 1000)
            .await
            .unwrap();
        let run = store.get_run(&f.run.id).await.unwrap();
        let revision = store.session_revision(&f.run.session_id).await.unwrap();
        aggregate_rejects(store, f.run.session_id.clone()).await;
        assert_eq!(
            store
                .approval_snapshot(&f.run.session_id, &f.run.id)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        assert_eq!(
            store
                .answer_approval(answer(store, &f, 0, ApprovalReply::Grant).await)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        assert_eq!(
            store
                .claim_approval_continuation(claim(store, &f).await)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        assert_eq!(
            store
                .terminate_approval(super::termination::request(store, &f, true).await)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        assert_eq!(
            store
                .terminate_approval(super::termination::request(store, &f, false).await)
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        // Original-key retries stay metadata-only even when private state is bad.
        let mut retry = f.batch.clone();
        retry.source_turn_id = Some(TurnId::new());
        retry.source_created_at = Some(clock.now() + time::Duration::hours(3));
        assert_eq!(
            f.execution
                .suspend_for_approval(retry, f.owner.clone())
                .await
                .unwrap(),
            suspended
        );
        assert_eq!(
            store
                .lookup_approval(&f.run.session_id, &f.batch.id)
                .await
                .unwrap(),
            Some(suspended)
        );
        assert_eq!(store.get_run(&f.run.id).await.unwrap(), run);
        assert_eq!(
            store
                .list_events(&f.run.session_id, None, 1000)
                .await
                .unwrap(),
            events
        );
        assert_eq!(
            store.session_revision(&f.run.session_id).await.unwrap(),
            revision
        );
    }
}
async fn recovery<F, Fut>(store: &impl Store, clock: &ManualClock, save: &mut F)
where
    F: FnMut(crabber_core::ContinuationId, usize) -> Fut,
    Fut: Future<Output = ()>,
{
    for case in [0, 1, 2, 4, 5] {
        let p = super::recovery::prepare(store, clock).await;
        save(p.original.continuation.id.clone(), case).await;
        clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(31));
        let events = store
            .list_events(&p.original.session_id, None, 1000)
            .await
            .unwrap();
        let run = store.get_run(&p.original.run_id).await.unwrap();
        let revision = store
            .session_revision(&p.original.session_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .reclaim_approval_continuation(p.request.clone())
                .await
                .unwrap_err(),
            Error::UnknownStoreOutcome
        );
        assert!(
            matches!(store.claim_approval_continuation(p.original.clone()).await.unwrap(), crate::ApprovalClaimOutcome::Replayed(receipt) if receipt == p.receipt)
        );
        assert_eq!(store.get_run(&p.original.run_id).await.unwrap(), run);
        assert_eq!(
            store
                .list_events(&p.original.session_id, None, 1000)
                .await
                .unwrap(),
            events
        );
        assert_eq!(
            store
                .session_revision(&p.original.session_id)
                .await
                .unwrap(),
            revision
        );
    }
}
pub(crate) fn corrupt(record: &mut ApprovalRecord, case: usize) {
    match case {
        0 | 1 => {
            let shift = time::Duration::hours(if case == 0 { 1 } else { -1 });
            record.created_at += shift;
            for q in &mut record.questions {
                q.view.expires_at = q.view.expires_at.map(|deadline| deadline + shift);
            }
        }
        2 => record.batch.source_turn_id = Some(TurnId::new()),
        3 => record.batch.source_turn_id = None,
        4 => {
            record.batch.source_created_at = Some(
                record.batch.source_created_at.unwrap_or(record.created_at)
                    + time::Duration::nanoseconds(1),
            );
        }
        5 => {
            record.batch.source_created_at =
                Some(record.created_at + time::Duration::nanoseconds(1));
        }
        _ => unreachable!(),
    }
}

/// Failed safe reads are evidence too: corruption must not trigger any write.
pub(crate) async fn evidence(
    store: &impl Store,
    p: &super::recovery::Prepared,
) -> serde_json::Value {
    let run = store.get_run(&p.original.run_id).await.unwrap().unwrap();
    let approval = match store.approval_snapshot(&run.session_id, &run.id).await {
        Ok(snapshot) => serde_json::to_value(snapshot).unwrap(),
        Err(error) => serde_json::json!({"error":format!("{error:?}")}),
    };
    serde_json::json!({"run":run, "attempt":store.latest_execution_attempt(&run.id).await.unwrap(),
        "approval":approval, "calls":store.list_unfinished_tool_calls(&run.id).await.unwrap(),
        "messages":store.list_all_messages(&run.session_id).await.unwrap(),
        "events":store.list_events(&run.session_id,None,1000).await.unwrap(),
        "revision":store.session_revision(&run.session_id).await.unwrap()})
}

async fn aggregate_rejects(store: &impl Store, session: crabber_core::SessionId) {
    assert_eq!(
        store
            .public_session_snapshot(crate::PublicSessionRequest {
                session_id: session,
                continuation: None,
                limits: crate::PublicSessionLimits {
                    header_bytes: 65536,
                    proof_bytes: 2 * 1024 * 1024,
                    interactions: 64,
                    history: crate::SnapshotLimits {
                        messages: 64,
                        tool_calls: 64,
                        parts: 1024,
                        text_bytes: 1024 * 1024,
                        encoded_bytes: 2 * 1024 * 1024
                    }
                }
            })
            .await
            .unwrap_err(),
        crate::PublicSessionError::UnknownStoreOutcome
    );
}
