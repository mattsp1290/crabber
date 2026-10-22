use super::fixture::*;
use crabber::{core::*, session::*, *};
use std::time::Duration;
#[tokio::test]
async fn foreign_queue_claim_prepare_dispatch_and_recovery_are_denied() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let b = f.create(false).await;
    let id = b.session().session_id();
    let queue = enqueue(&f, &b).await;
    let heads = QueueHeadsRequest {
        workspace_id: f.workspace_b.clone(),
        limit: 256,
        cursor: None,
    };
    assert_eq!(
        f.host
            .list_queue_heads(heads.clone())
            .await
            .unwrap()
            .heads
            .len(),
        1
    );
    let before = f.snapshot(id).await;
    assert_eq!(
        f.worker.list_queue_heads(heads).await.unwrap().heads,
        vec![]
    );
    let claim = QueueMutation {
        session_id: id.clone(),
        id: ControlId::new(),
        expected_revision: f.native.session_revision(id).await.unwrap(),
        action: QueueAction::Claim {
            owner: "host".into(),
            lease: Duration::from_secs(30),
        },
    };
    denied(f.worker.mutate_queue(claim.clone()).await);
    f.unchanged(id, before).await;
    let QueueOutcome::Applied {
        authority: Some(authority),
        receipt,
        ..
    } = f.host.mutate_queue(claim).await.unwrap()
    else {
        panic!("claim")
    };
    let control = QueueControl {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        authority,
    };
    let prepared = f
        .host
        .prepare_queue_in(b.session(), control.clone())
        .await
        .unwrap();
    let before = f.snapshot(id).await;
    denied(
        f.worker
            .prepare_queue_in(b.session(), control.clone())
            .await,
    );
    denied(
        f.worker
            .dispatch_queue_in(b.session(), control.clone(), ControlId::new(), None)
            .await,
    );
    f.unchanged(id, before).await;
    let admit = QueueControl {
        id: ControlId::new(),
        expected_revision: prepared.revision,
        authority: control.authority.clone(),
    };
    let Admission::Started { handle, .. } = f
        .host
        .admit_queue_in(b.session(), admit.clone(), None)
        .await
        .unwrap()
    else {
        panic!("admission")
    };
    handle.done().await.unwrap();
    let before = f.snapshot(id).await;
    denied(
        f.worker
            .admit_queue_in(b.session(), admit.clone(), None)
            .await,
    );
    denied(f.worker.recover_queue_in(b.session(), &queue, None).await);
    f.unchanged(id, before).await;
    host_replays(&f, &b, &queue, control, admit.id).await;
    super::positive::queue(&f).await;
    f.cleanup().await;
}

async fn enqueue(f: &Fixture, b: &ExecutionContext) -> QueueId {
    let id = b.session().session_id();
    let queue = QueueId::new();
    let input =
        InputEnvelope::text("queued", f.native.session_revision(id).await.unwrap()).unwrap();
    f.host
        .enqueue_session_input_in(
            b.session(),
            input,
            queue.clone(),
            ControlId::new(),
            options(),
        )
        .await
        .unwrap();
    queue
}

async fn host_replays(
    f: &Fixture,
    b: &ExecutionContext,
    queue: &QueueId,
    control: QueueControl,
    admission_id: ControlId,
) {
    assert!(matches!(
        f.host
            .recover_queue_in(b.session(), queue, None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
    assert!(matches!(
        f.host
            .dispatch_queue_in(b.session(), control, admission_id, None)
            .await
            .unwrap(),
        Admission::Replayed(_)
    ));
}
