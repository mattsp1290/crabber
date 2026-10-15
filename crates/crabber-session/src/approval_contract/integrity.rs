use super::{Error, Store, fixture};
use crate::approval::{ApprovalRecord, verify_receipt};
use crabber_core::{InteractionState, ManualClock};

pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let f = fixture::staged(store, clock).await;
    let receipt = f
        .execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let events = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap();
    let source = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap();
    verify_receipt(&receipt, source).unwrap();
    for change in 0..8 {
        let mut event = source.clone();
        match change {
            0 => event.session_id = crabber_core::SessionId::new(),
            1 => event.run_id = crabber_core::RunId::new(),
            2 => event.cursor = None,
            3 => event.correlation = None,
            4 => event.live_only = true,
            5 => event.payload["revision"] = serde_json::json!(0),
            6 => event.payload["version"] = serde_json::json!(2),
            7 => event.payload["extra"] = serde_json::json!(true),
            _ => unreachable!(),
        }
        assert_eq!(
            verify_receipt(&receipt, &event),
            Err(Error::UnknownStoreOutcome)
        );
    }
    let record = ApprovalRecord::new(
        f.batch.clone(),
        f.run.session_id.clone(),
        f.run.id.clone(),
        "workspace".into(),
        time::OffsetDateTime::UNIX_EPOCH,
    )
    .unwrap();
    record.validate(&f.run.session_id, &f.run.id).unwrap();
    for change in 0..9 {
        let mut bad = record.clone();
        match change {
            0 => bad.version = 2,
            1 => bad.run_id = crabber_core::RunId::new(),
            2 => bad.questions.reverse(),
            3 => {
                bad.questions.pop();
            }
            4 => bad.questions[0].view.consumed = true,
            5 => bad.questions[0].view.expires_at = None,
            6 => bad.questions[0].view.state = InteractionState::Answered,
            7 => bad.state = crate::ApprovalContinuationState::Claimed,
            8 => bad.batch.calls[0].target.call_id = bad.batch.calls[1].target.call_id.clone(),
            _ => unreachable!(),
        }
        assert_eq!(
            bad.validate(&f.run.session_id, &f.run.id),
            Err(Error::UnknownStoreOutcome)
        );
    }
}
